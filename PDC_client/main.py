
import argparse
import sys
import os
from datetime import datetime

from . import submodules

SUBCOMMANDS = {'studyID', 'PDCStudyID', 'studyName',
               'metadata', 'metadataToSky',
               'file', 'files'}


def _firstSubcommand(argv):
    for i in range(1, len(argv)):
        if argv[i] in SUBCOMMANDS:
            return i
    return len(argv)


class Main(object):
    '''
    A class to parse subcommands.
    Inspired by this blog post: https://chase-seibert.github.io/blog/2014/03/21/python-multilevel-argparse.html
    '''

    STUDY_ID_DESCRIPTION = 'Get the study_id from the pdc_study_id.'
    PDC_STUDY_ID_DESCRIPTION = 'Get the pdc_study_id from the study_id.'
    STUDY_NAME_DESCRIPTION = 'Get the study name.'
    METADATA_DESCRIPTION = 'Get the metadata for files in a study.'
    METADATA_TO_SKY_DESCRIPTION = 'Convert a metadata tsv or json to Skyline annotation csv.'
    FILE_DESCRIPTION = 'Download a single file.'
    FILES_DESCRIPTION = 'Download all the files in a study.'

    def __init__(self):
        parser = argparse.ArgumentParser(description='Command line client for NCI Proteomics Data Commons',
                                         usage = f'''PDC_client <command> [<args>]

Available commands:
   studyID         {Main.STUDY_ID_DESCRIPTION}
   PDCStudyID      {Main.PDC_STUDY_ID_DESCRIPTION}
   studyName       {Main.STUDY_NAME_DESCRIPTION}
   metadata        {Main.METADATA_DESCRIPTION}
   metadataToSky   {Main.METADATA_TO_SKY_DESCRIPTION}
   file            {Main.FILE_DESCRIPTION}
   files           {Main.FILES_DESCRIPTION}''')
        parser.add_argument('--debug', choices = ['pdb', 'pudb'], default=None,
                            help='Start the main method in selected debugger')
        parser.add_argument('command', help = 'Subcommand to run.')
        subcommand_start = _firstSubcommand(sys.argv)
        args = parser.parse_args(sys.argv[1:(subcommand_start + 1)])

        if args.debug:
            if args.debug == 'pdb':
                import pdb as debugger
            elif args.debug == 'pudb':
                import pudb as debugger
            debugger.set_trace()

        if not args.command in SUBCOMMANDS:
            sys.stderr.write(f'ERROR: {args.command} is an unknown command!\n')
            parser.print_help()
            sys.exit(1)
        getattr(self, args.command)(subcommand_start + 1)


    def studyID(self, start=2):
        parser = argparse.ArgumentParser(description=Main.STUDY_ID_DESCRIPTION)
        parser.add_argument('-u', '--baseUrl', default=submodules.api.BASE_URL,
                            help=f'The base URL for the PDC API. {submodules.api.BASE_URL} is the default.')
        parser.add_argument('--skipVerify', default=False, action='store_true',
                            help='Skip ssl verification?')
        parser.add_argument('pdc_study_id')
        args = parser.parse_args(sys.argv[start:])
        study_id = submodules.api.study_id(args.pdc_study_id, args.baseUrl,
                                           verify=not args.skipVerify)
        if study_id is None:
            sys.stderr.write('ERROR: No study found matching study_id!\n')
            sys.exit(1)
        sys.stdout.write(f'{study_id}\n')


    def PDCStudyID(self, start=2):
        parser = argparse.ArgumentParser(description=Main.PDC_STUDY_ID_DESCRIPTION)
        parser.add_argument('-u', '--baseUrl', default=submodules.api.BASE_URL,
                            help=f'The base URL for the PDC API. {submodules.api.BASE_URL} is the default.')
        parser.add_argument('--skipVerify', default=False, action='store_true',
                            help='Skip ssl verification?')
        parser.add_argument('study_id')
        args = parser.parse_args(sys.argv[start:])
        pdc_study_id = submodules.api.pdc_study_id(args.study_id, args.baseUrl,
                                                   verify=not args.skipVerify)
        sys.stdout.write(f'{pdc_study_id}\n')


    def studyName(self, start=2):
        parser = argparse.ArgumentParser(description=Main.STUDY_NAME_DESCRIPTION)
        parser.add_argument('-u', '--baseUrl', default=submodules.api.BASE_URL,
                            help=f'The base URL for the PDC API. {submodules.api.BASE_URL} is the default.')
        parser.add_argument('--skipVerify', default=False, action='store_true',
                            help='Skip ssl verification?')
        parser.add_argument('--normalize', default=False, action='store_true',
                            help='Remove special characters from study name so it a valid file name.')
        parser.add_argument('study_id')
        args = parser.parse_args(sys.argv[start:])
        study_name = submodules.api.study_name(args.study_id, args.baseUrl,
                                               verify=not args.skipVerify)
        if args.normalize:
            study_name = submodules.io.normalize_fname(study_name)

        sys.stdout.write(f'{study_name}\n')


    def metadata(self, start=2):
        parser = argparse.ArgumentParser(description=Main.METADATA_DESCRIPTION)
        parser.add_argument('-f', '--format', choices=['json', 'tsv', 'str'], default = 'json',
                            help='The output file format. Default is "json".')
        parser.add_argument('-n', '--nFiles', type=int, default=None,
                            help='The number of files to get metadata for. Default is all files in study')
        parser.add_argument('-o', '--ofname', default='study_metadata',
                            help='Output base name.')
        parser.add_argument('-u', '--baseUrl', default=submodules.api.BASE_URL,
                            help=f'The base URL for the PDC API. {submodules.api.BASE_URL} is the default.')
        parser.add_argument('--skipVerify', default=False, action='store_true',
                            help='Skip ssl verification?')
        parser.add_argument('-a', '--skylineAnnotations', default=False, action='store_true',
                            help='Also save Skyline annotations csv file')
        parser.add_argument('study_id', help='The study id.')
        args = parser.parse_args(sys.argv[start:])

        ofname = f'{args.ofname}.{args.format}'
        data = submodules.api.metadata(args.study_id, url=args.baseUrl, n_files=args.nFiles, verify=not args.skipVerify)
        if data is None:
            sys.exit(1)
        if len(data) == 0:
            sys.stderr.write('ERROR: Could not find any data associated with study!\n')
            sys.exit(1)
        submodules.io.writeFileMetadata(data, ofname, format=args.format)
        if args.skylineAnnotations:
            submodules.io.writeSkylineAnnotations(data, f'{args.ofname}_annotations.csv')


    def metadataToSky(self, start=2):
        parser = argparse.ArgumentParser(description=Main.METADATA_TO_SKY_DESCRIPTION)
        parser.add_argument('-i', '--in', default=None,
                            choices=('tsv', 'json'), dest='input_format',
                            help='Specify metadata file format. '
                                 'By default the format is inferred from the file extension.')
        parser.add_argument('metadata_file', help='The metadata file to convert.')
        args = parser.parse_args(sys.argv[start:])

        if args.input_format:
            input_format = args.input_format
        else:
            input_format = os.path.splitext(args.metadata_file)[1][1:]

        with open(args.metadata_file, 'r') as outF:
            data = submodules.io.readFileMetadata(outF, input_format)

        submodules.io.writeSkylineAnnotations(data, 'skyline_annotations.csv')


    def file(self, start=2):
        parser = argparse.ArgumentParser(description=Main.FILE_DESCRIPTION)
        parser.add_argument('-o', '--ofname', default=None,
                            help='Output file name.')
        parser.add_argument('-m', '--md5sum', default=None,
                            help='The expected file md5 sum. If blank, the check sum step is skipped.')
        parser.add_argument('--noBackup', action='store_true', default=False,
                            help='Don\'t backup duplicate files. '
                                 'By default, if the file already exists the new file is written to a tempory file as it is '
                                 'being downloaded and overwritten once the download is completed.')
        parser.add_argument('-f', '--force', action='store_true', default=False,
                            help='Re-download even if the target file already exists.')
        parser.add_argument('url', help='The file url.')

        args = parser.parse_args(sys.argv[start:])

        if args.ofname is None:
            ofname = submodules.io.fileBasename(args.url)
            if ofname is None:
                sys.stderr.write('ERROR: Could not determine output file name!\n')
                sys.exit(1)
        else:
            ofname = args.ofname

        remove_old = False
        if os.path.isfile(ofname):
            if not args.force and args.md5sum is not None:
                if submodules.io.md5_sum(ofname) == args.md5sum:
                    sys.stdout.write(f'The file: "{ofname}" has already been downloaded. Use --force option to override.\n')
                    sys.exit(0)

            if args.noBackup:
                os.remove(ofname)
            else:
                remove_old = True
                old_ofname = ofname
                ofname += '_{}.tmp'.format(datetime.now().strftime("%y%m%d_%H%M%S"))

        if not submodules.io.downloadFile(args.url, ofname, expected_md5=args.md5sum):
            sys.stderr.write(f'ERROR: Failed to download file: {ofname}\n')
            sys.exit(1)

        if remove_old:
            os.rename(ofname, old_ofname)

def main():
    _ = Main()

if __name__ == '__main__':
    main()

