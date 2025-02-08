
import argparse
import json
import difflib

from PDC_client.submodules import api

STUDIES = ['PDC000504', 'PDC000341', 'PDC000414', 'PDC000464',
           'PDC000110', ]


def main():
    parser = argparse.ArgumentParser(description='This script checks if there are any changes to '
                                                 'the test api data in tests/data/api and prints '
                                                 'a summary of the differences.')
    parser.add_argument('--write', action='store_true', default=False,
                        help='Overwrite test files in data/api with new api data?')
    parser.add_argument('pdc_study_ids', nargs='?', default=None,
                        help='PDC study ID(s) to download new data for. '
                             'If not specified, data is downloaded for all ids in TEST_STUDIES')

    args = parser.parse_args()

    study_ids = STUDIES if args.pdc_study_ids is None else args.pdc_study_ids

    for study in study_ids:
        api.



if __name__ == '__main__':
    main()
