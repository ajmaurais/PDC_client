
import sys
import json
from hashlib import md5
import requests
import re

RAW_BASENAME_RE = re.compile(r'/([^/]+\.raw)')
RAW_DIRNAME_RE = re.compile(r'cloudfront\.net\/(.*)\/([^/]+\.raw)')


def writeFileMetadata(data, ofname, format='json'):
    '''
    Write file metadata.

    Parameters:
        data (list): List of dictionaries representing the metadata for each file.
        ofname (str): Output file name.
        format (str): Output file format. One of ["json", "tsv", "str"]

    Raises:
        ValueError: If unknown output file format.
    '''

    # make sure all the keys are the same
    keys = data[0].keys()
    for file in data:
        if keys != file.keys():
            raise KeyError('File dict keys must be identical!')

    if format in ('json', 'str'):
        if format == 'json':
            with open(ofname, 'w') as outF:
                json.dump(data, outF)
        else:
            print(json.dumps(data, indent = 2))

    elif format == 'tsv':
        with open(ofname, 'w') as outF:
            for i, h in enumerate(keys):
                if i != 0:
                    outF.write('\t')
                outF.write(h)
            outF.write('\n')
            for file in data:
                for i, v in enumerate(file.values()):
                    if i != 0:
                        outF.write('\t')
                    outF.write(v)
                outF.write('\n')

    else:
        raise ValueError(f'{format} is an unknown output format!')


def md5_sum(fname):
    ''' Get the md5 digest of a file. '''
    file_hash = md5()
    with open(fname, 'rb') as inF:
        while chunk := inF.read(8192):
            file_hash.update(chunk)
    return file_hash.hexdigest()


def fileBasename(url):
    '''
    Attempt to extract raw file basename from url.

    Parameters:
        url (str): The full url.

    Returns:
        basename (str): The file basename, None if no match was found.
    '''

    match = RAW_BASENAME_RE.search(url)
    return None if not match else match.group(1)


def splitRawPath(url):
    '''
    Attempt to extract raw file path, dirname, and basename from url.

    Parameters:
        url (str): The full url.

    Returns:
        dirname (str): The raw file directory name
        basename (str): The file basename
    '''

    match = RAW_DIRNAME_RE.search(url)
    if match:
        return match.groups()
    else:
        return None


def downloadFile(url, ofname, expected_md5=None, nRetrys=5):
    '''
    Download a single file.

    The expected md5 sum is checked against the downloaded file and the
    download is retried up to n times if it does not match.

    Parameters:
        url (str): The file url.
        ofname (str): The name of the file to write.
        expected_md5 (str): Expected md5 sum. None to skip checksum.
        nRetrys (int): defaults to 5.

    Returns:
        sucess (bool): True if sucessfull, False if not.
    '''

    tries = 0
    while tries < nRetrys:
        tries += 1
        try:
            with requests.get(url, stream=True) as fstream:
                fstream.raise_for_status()
                with open(ofname, 'wb') as outF:
                    for chunk in fstream.iter_content(chunk_size=8192):
                        outF.write(chunk)
        except (requests.Timeout, requests.ConnectionError):
            continue
        except (requests.exceptions.RequestException) as error:
            sys.stderr.write('Failed to download file "{}" because "{}"'.format(ofname, error))

        if expected_md5 is None:
            return True
        if md5_sum(ofname) == expected_md5:
            return True

    return False


