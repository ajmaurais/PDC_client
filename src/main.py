
import argparse
import sys
import os
from datetime import datetime

from .submodules.api import Client, BASE_URL
from .submodules import io
from .submodules.logger import LOGGER

SUBCOMMANDS = {'studyID', 'PDCStudyID', 'studyName',
               'metadata', 'metadataToSky',
               'file'}


def _firstSubcommand(argv):
    for i in range(1, len(argv)):
        if argv[i] in SUBCOMMANDS:
            return i
    return len(argv)


class Main:
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

    def __init__(self, argv=sys.argv):
        self.argv = argv

        parser = argparse.ArgumentParser(description='Command line client for NCI Proteomics Data Commons',
                                         usage = f'''PDC_client <command> [<args>]

Available commands:
   studyID         {Main.STUDY_ID_DESCRIPTION}
   PDCStudyID      {Main.PDC_STUDY_ID_DESCRIPTION}
   studyName       {Main.STUDY_NAME_DESCRIPTION}
   metadata        {Main.METADATA_DESCRIPTION}
   metadataToSky   {Main.METADATA_TO_SKY_DESCRIPTION}
   file            {Main.FILE_DESCRIPTION}''')
        parser.add_argument('command', help = 'Subcommand to run.')
        subcommand_start = _firstSubcommand(self.argv)
        args = parser.parse_args(self.argv[1:(subcommand_start + 1)])

        if not args.command in SUBCOMMANDS:
            LOGGER.error('%s is an unknown command!\n', args.command)
            parser.print_help()
            sys.exit(1)
        getattr(self, args.command)(subcommand_start + 1)


    def studyID(self, start=2):
        parser = argparse.ArgumentParser(description=Main.STUDY_ID_DESCRIPTION)
        parser.add_argument('-u', '--baseUrl', default=BASE_URL,
                            help=f'The base URL for the PDC API. {BASE_URL} is the default.')
        parser.add_argument('--skipVerify', default=False, action='store_true',
                            help='Skip ssl verification?')
        parser.add_argument('pdc_study_id')
        args = parser.parse_args(self.argv[start:])

        with Client(url=args.baseUrl, verify=not args.skipVerify) as client:
            study_id = client.get_study_id(args.pdc_study_id)

        if study_id is None:
            LOGGER.error('No study found matching pdc_study_id!\n')
            sys.exit(1)
        sys.stdout.write(f'{study_id}\n')


    def PDCStudyID(self, start=2):
        parser = argparse.ArgumentParser(description=Main.PDC_STUDY_ID_DESCRIPTION)
        parser.add_argument('-u', '--baseUrl', default=BASE_URL,
                            help=f'The base URL for the PDC API. {BASE_URL} is the default.')
        parser.add_argument('--skipVerify', default=False, action='store_true',
                            help='Skip ssl verification?')
        parser.add_argument('study_id')
        args = parser.parse_args(self.argv[start:])

        with Client(url=args.baseUrl, verify=not args.skipVerify) as client:
            pdc_study_id = client.get_pdc_study_id(args.study_id)

        if pdc_study_id is None:
            LOGGER.error('No study found matching study_id!\n')
            sys.exit(1)
        sys.stdout.write(f'{pdc_study_id}\n')


    def studyName(self, start=2):
        parser = argparse.ArgumentParser(description=Main.STUDY_NAME_DESCRIPTION)
        parser.add_argument('-u', '--baseUrl', default=BASE_URL,
                            help=f'The base URL for the PDC API. {BASE_URL} is the default.')
        parser.add_argument('--skipVerify', default=False, action='store_true',
                            help='Skip ssl verification?')
        parser.add_argument('--normalize', default=False, action='store_true',
                            help='Remove special characters from study name so it a valid file name.')
        parser.add_argument('study_id')
        args = parser.parse_args(self.argv[start:])

        with Client(url=args.baseUrl, verify=not args.skipVerify) as client:
            study_name = client.get_study_name(args.study_id)

        if study_name is None:
            LOGGER.error('No study found matching study_id!\n')
            sys.exit(1)

        if args.normalize:
            study_name = io.normalize_fname(study_name)
        sys.stdout.write(f'{study_name}\n')


    def metadata(self, start=2):
        parser = argparse.ArgumentParser(description=Main.METADATA_DESCRIPTION)
        parser.add_argument('-n', '--nFiles', type=int, default=None, dest='n_files',
                            help='The number of files to get metadata for. Default is all files in study')
        parser.add_argument('-u', '--baseUrl', default=BASE_URL,
                            help=f'The base URL for the PDC API. {BASE_URL} is the default.')
        parser.add_argument('--skipVerify', default=False, action='store_true',
                            help='Skip ssl verification?')

        f_args = parser.add_argument_group('Output format options')
        f_args.add_argument('-p', '--prefix', default=None,
                            help='The prefix to add to the output file names. '
                                 'Default is the PDC study id.')
        f_args.add_argument('-f', '--format', choices=('json', 'tsv', 'str'), default = 'json',
                            help="The output file format. Default is 'json'. "
                                 "'tsv' is only compatable with DIA data.")
        f_args.add_argument('-a', '--skylineAnnotations', default=False, action='store_true',
                            help='Also save Skyline annotations csv file. Only compatable with DIA data.')
        f_args.add_argument('--flatten', default=False, action='store_true',
                            help='Combine metadata into a single flat file. '
                                 'Only compatable with DIA data.')

        parser.add_argument('study_id', help='The study id.')
        args = parser.parse_args(self.argv[start:])

        with Client(url=args.baseUrl, verify=not args.skipVerify) as client:
            # get study metadata and check that output options are compatable with experiment type
            study_metadata = client.get_study_metadata(study_id=args.study_id)
            if study_metadata is None:
                LOGGER.error('Could not retrieve metadata for study: %s', args.study_id)
                sys.exit(1)
            experiment_type = study_metadata['experiment_type']
            if not io.is_dia(study_metadata) and \
                (args.flatten or args.skylineAnnotations or args.format == 'tsv'):
                LOGGER.error('Output format not supported for %s experiments', experiment_type)
                sys.exit(1)

            # download remaining metadata
            files = client.get_study_raw_files(args.study_id, n_files=args.n_files)
            aliquots = client.get_study_aliquots(args.study_id,
                                                 file_ids=[f['file_id'] for f in files])
            cases = client.get_study_cases(args.study_id)

        # check that no metadata is missing
        metadata_files = {'study_metadata': study_metadata, 'files': files,
                          'aliquots': aliquots, 'cases': cases}
        all_good = True
        for name, data in metadata_files.items():
            if data is None:
                LOGGER.error("Could not retreive %s data for study: '%s'", name, args.study_id)
                all_good = False
        if not all_good:
            sys.exit(1)

        prefix = f'{study_metadata["pdc_study_id"]}_' if args.prefix is None else args.prefix
        if args.flatten:
            if any(len(a['file_ids']) != 1 for a in aliquots):
                LOGGER.error('Cannot flatten aliquots with more than 1 file_id.')
                sys.exit(1)

            flat_data = io.flatten_metadata(**metadata_files)
            io.write_metadata_file(flat_data, f'{prefix}flat.{args.format}',
                                   format=args.format)

            if args.skylineAnnotations:
                io.writeSkylineAnnotations(flat_data, 'skyline_annotations.csv')

        else:
            for name, data in metadata_files.items():
                io.write_metadata_file(data, f'{prefix}{name}.{args.format}',
                                       format=args.format)


    def metadataToSky(self, start=2):
        parser = argparse.ArgumentParser(description=Main.METADATA_TO_SKY_DESCRIPTION)
        parser.add_argument('-i', '--in', default=None,
                            choices=('tsv', 'json'), dest='input_format',
                            help='Specify metadata file format. '
                                 'By default the format is inferred from the file extension.')
        parser.add_argument('metadata_file', help='The metadata file to convert.')
        args = parser.parse_args(self.argv[start:])

        if args.input_format:
            input_format = args.input_format
        else:
            input_format = os.path.splitext(args.metadata_file)[1][1:]

        with open(args.metadata_file, 'r') as outF:
            data = io.readFileMetadata(outF, input_format)

        io.writeSkylineAnnotations(data, 'skyline_annotations.csv')


    def file(self, start=2):
        parser = argparse.ArgumentParser(description=Main.FILE_DESCRIPTION)
        parser.add_argument('-u', '--baseUrl', default=BASE_URL,
                            help=f'The base URL for the PDC API. {BASE_URL} is the default. '
                                  'Only used with --fileID option.')
        parser.add_argument('-o', '--ofname', default=None,
                            help='Output file name.')
        parser.add_argument('-m', '--md5sum', default=None,
                            help='The expected file md5 sum. If blank, the check sum step is skipped.')
        parser.add_argument('-s', '--size', default=None, type=int,
                            help='The expected file size. If blank, the file size check is skipped.')
        parser.add_argument('--noBackup', action='store_true', default=False,
                            help='Don\'t backup duplicate files. '
                                 'By default, if the file already exists the new file is written to a tempory file as it is '
                                 'being downloaded and overwritten once the download is completed.')
        parser.add_argument('-f', '--force', action='store_true', default=False,
                            help='Re-download even if the target file already exists.')

        source_args = parser.add_mutually_exclusive_group(required=True)
        source_args.add_argument('--url', help='The file url.')
        source_args.add_argument('--fileID', help='The PDC file_id.')

        args = parser.parse_args(self.argv[start:])

        if args.ofname is None:
            ofname = io.file_basename(args.url)
            if ofname is None:
                LOGGER.error('Could not determine output file name!\n')
                sys.exit(1)
        else:
            ofname = args.ofname

        remove_old = False
        if os.path.isfile(ofname):
            if not args.force and args.md5sum is not None:
                if io.md5_sum(ofname) == args.md5sum:
                    sys.stdout.write(f'The file: "{ofname}" has already been downloaded. Use --force option to override.\n')
                    sys.exit(0)

            if args.noBackup:
                os.remove(ofname)
            else:
                remove_old = True
                old_ofname = ofname
                ofname += f'_{datetime.now().strftime("%y%m%d_%H%M%S")}.tmp'

        if not io.download_file(args.url, ofname, expected_md5=args.md5sum):
            LOGGER.error("Failed to download file: '%s'", ofname)
            sys.exit(1)

        if remove_old:
            os.rename(ofname, old_ofname)

def main():
    _ = Main()

if __name__ == '__main__':
    main()

