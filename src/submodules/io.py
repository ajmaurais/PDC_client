
import os
import json
from csv import DictReader
from hashlib import md5
import re
import warnings
from typing import TextIO

import httpx

from .api import FILE_DATA_KEYS, DATA_ID_KEYS
from .logger import LOGGER

RAW_BASENAME_RE = re.compile(r'/([^/]+\.raw)')
FILE_EXT_RE = re.compile(r'^([\w\-%& \\\/=\+]+)\.(.*)$')


def normalize_fname(s: str) -> str:
    ''' Convert non-alphanumeric characters to underscores.'''
    ret = s
    ret = re.sub('[^a-zA-Z_]+', ' ', ret)
    ret = re.sub(r'\s+', '_', ret)
    return ret


def splitext(path):
    m = FILE_EXT_RE.search(path)
    if m:
        return m.groups()
    return path, ''


def _write_row(itterable, ostream, sep='\t', quote=None):
    for i, it in enumerate(itterable):
        if i > 0:
            ostream.write(sep)
        value_s = str(it) if it is not None else ''
        ostream.write(value_s if quote is None else f'{quote}{value_s}{quote}')
    ostream.write('\n')


def flatten_metadata(study_metadata, files, aliquots, cases):
    '''
    Flatten aliquot and case metadata into a single dict for each file.

    Parameters:
        study_metadata (dict): The study metadata.
        files (list): List of file metadata.
        aliquots (list): List of aliquot metadata.
        cases (list): List of case metadata.

    Returns:
        data (list): List of dictionaries with the flattened metadata for each file.
    '''

    if any(len(a['file_ids']) != 1 for a in aliquots):
        raise ValueError('Cannot flatten aliquots with more than 1 file_id.')

    ret = []
    for file in files:
        ret.append(file)
        ret[-1]['experiment_type'] = study_metadata['experiment_type']
        ret[-1]['analytical_fraction'] = study_metadata['analytical_fraction']

        # add aliquot metadata
        file_id = file['file_id']
        aliquot_data = next((a for a in aliquots if a['file_ids'][0] == file_id), None)
        if aliquot_data is None:
            raise ValueError(f'No aliquot data found for file_id: {file_id}')
        for k, v in aliquot_data.items():
            if k != 'file_ids':
                ret[-1][k] = v

        # add case metadata
        case_data = next((c for c in cases if c['case_id'] == aliquot_data['case_id']), None)
        if case_data is None:
            raise ValueError(f'No case data found for file_id: {file_id}')
        for k, v in case_data.items():
            ret[-1][k] = v

    return ret


def is_dia(study_metadata):
    ''' Check if the study is a DIA study. '''
    if not isinstance(study_metadata, dict):
        raise ValueError('study_metadata must be a dictionary!')

    experiment_type = study_metadata.get('experiment_type')
    if experiment_type is None:
        warnings.warn("'experiment_type' not found in study metadata.")
        return False
    return experiment_type.lower() == 'label free'


def write_metadata_file(data: dict|list, ofname: str, format: str='json'):
    '''
    Write metadata file.

    Parameters:
        data (dict|list): The metadata to write.
            If a dict, the metadata is written as a single json object.
        ofname (str): Output file name.
        format (str): Output file format. One of ["json", "tsv", "str"]

    Raises:
        ValueError: If unknown output file format.
    '''

    if isinstance(data, dict):
        with open(ofname, 'w', encoding='utf-8') as outF:
            json.dump(data, outF, indent=2)
        return

    # make sure all the keys are the same
    keys = data[0].keys()
    for file in data:
        if keys != file.keys():
            raise KeyError('File dict keys must be identical!')

    if format in ('json', 'str'):
        if format == 'json':
            with open(ofname, 'w', encoding='utf-8') as outF:
                json.dump(data, outF, indent=2)
        else:
            print(json.dumps(data, indent=2))

    elif format == 'tsv':
        with open(ofname, 'w', encoding='utf-8') as outF:
            _write_row(keys, outF, sep='\t')
            for file in data:
                _write_row([file[key] for key in keys], outF, sep='\t')
    else:
        raise ValueError(f'{format} is an unknown output format!')


def read_file_metadata(fp: TextIO, format: str) -> list:
    '''
    Read study file metadata.

    Parameters:
        fp (file): File pointer.
        format (str): The metadata format. One of ["tsv", "json"].

    Returns:
        data (list): List of file metadata dictionaries.

    Raises:
        RuntimeError: If unknown metadata format.
    '''
    if format == 'tsv':
        return list(DictReader(fp, delimiter='\t'))
    if format == 'json':
        return json.load(fp)
    raise RuntimeError(f"Unknown metadata format!: '{format}'")


def write_skyline_annotations(data: list, ofname: str):
    '''
    Write Skyline annotations file.

    Parameters:
        data (list): List of file metadata dictionaries for each file.
        ofname (str): Output file name.
    '''

    exclude_keys = set(DATA_ID_KEYS + FILE_DATA_KEYS)
    file_annotations = [key for key in data[0].keys() if key not in exclude_keys]

    # make csv headers
    headers = ['ElementLocator']
    headers += ['annotation_' + key for key in data[0].keys() if key in file_annotations]

    with open(ofname, 'w') as outF:
        _write_row(headers, outF, sep=',', quote='"')
        for file in data:
            annotation_values = ['Replicate:/' + splitext(file['file_name'])[0]]
            annotation_values += [file[a] if file[a] else '' for a in file_annotations]
            _write_row(annotation_values, outF, sep=',', quote='"')


def md5_sum(fname: str) -> str:
    ''' Get the md5 digest of a file. '''
    file_hash = md5()
    with open(fname, 'rb') as inF:
        while chunk := inF.read(8192):
            file_hash.update(chunk)
    return file_hash.hexdigest()


def file_basename(url):
    '''
    Attempt to extract raw file basename from url.

    Parameters:
        url (str): The full url.

    Returns:
        basename (str): The file basename, None if no match was found.
    '''

    match = RAW_BASENAME_RE.search(url)
    return None if not match else match.group(1)


def download_file(url: str, ofname: str,
                  expected_md5: str=None, expected_size: int=None,
                  n_retries:int=2) -> bool:
    '''
    Download a single file.

    The expected md5 sum is checked against the downloaded file and the
    download is retried up to n times if it does not match.

    Parameters:
        url (str): The file url.
        ofname (str): The name of the file to write.
        expected_md5 (str): Expected md5 sum. None to skip checksum.
        expected_size (int): Expected file size. None to skip size check.
        n_retrys (int): defaults to 5.

    Returns:
        sucess (bool): True if sucessfull, False if not.
    '''

    tries = 0
    while tries < n_retries:
        tries += 1
        try:
            with httpx.stream("GET", url) as response:
                response.raise_for_status()
                with open(ofname, 'wb') as outF:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        outF.write(chunk)
        except (httpx.TimeoutException, httpx.RequestError) as e:
            LOGGER.warning('Failed to download file "%s" because "%s"', ofname, e)
            LOGGER.warning('Retry %i of %i', tries + 1, n_retries)
            continue
        except (httpx.HTTPStatusError) as e:
            LOGGER.warning('Failed to download file "%s" because "%s"', ofname, e)
            LOGGER.warning('Retry %i of %i', tries + 1, n_retries)
            continue

        if expected_md5 is None:
            LOGGER.warning('Skipping md5 check for file "%s"', ofname)
        elif md5_sum(ofname) != expected_md5:
            LOGGER.error('Expected MD5 checksum does not match for file "%s"', ofname)
            return False

        if expected_size is None:
            LOGGER.warning('Skipping size check for file "%s"', ofname)
        elif os.path.getsize(ofname) != expected_size:
            LOGGER.error('Expected file size does not match for file "%s"', ofname)
            return False
        return True

    LOGGER.error('Failed to download file "%s" after %d attempt(s)', ofname, n_retries)
    return False