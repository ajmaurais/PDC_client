
import sys
import os
import argparse
import json
import difflib
import subprocess
import asyncio
import random
from typing import Any
from uuid import UUID

from resources.data import STUDY_METADATA, STUDY_CATALOG, EXPERIMENT_METADATA
from resources.data import FILE_METADATA, SAMPLE_METADATA, CASE_METADATA

from PDC_client.submodules.api import Client, BASE_URL

DUPLICATE_FILE_TEST_STUDIES = ['PDC000251']
STUDIES = ['PDC000504', 'PDC000451', 'PDC000592'] + DUPLICATE_FILE_TEST_STUDIES

ENDPOINTS = {'study': 'Study metadata',
             'studyCatalog': 'Study catalog',
             'experiment': 'Experiment metadata',
             'file': 'File metadata',
             'samples': 'Sample metadata',
             'case': 'Case metadata'}

TEST_DATA = {'study': STUDY_METADATA,
             'studyCatalog': STUDY_CATALOG,
             'experiment': EXPERIMENT_METADATA,
             'file': FILE_METADATA,
             'samples': SAMPLE_METADATA,
             'case': CASE_METADATA}

# ANSI color codes
RED = '\033[31m'    # Red for removals (-)
GREEN = '\033[32m'  # Green for additions (+)
RESET = '\033[0m'   # Reset color


def colorize_diff(diff):
    '''Adds ANSI colors to diff output.'''
    colored_lines = []
    for line in diff:
        if line.startswith("-"):
            colored_lines.append(f"{RED}{line}{RESET}")
        elif line.startswith("+"):
            colored_lines.append(f"{GREEN}{line}{RESET}")
        else:
            colored_lines.append(line)
    return colored_lines


def get_diff(lhs, rhs, name):
    '''
    Get the diff text for 2 JSON objects.

    Parameters
    ----------
    lhs: dict
    rhs: dict
    name: str
        The name of the file that is being compared.

    Returns
    -------
    diff: list
        A line with each line of the diff text between lhs and rhs.
    '''
    lhs_s, rhs_s = [json.dumps(x, indent=2, sort_keys=False).splitlines() for x in [lhs, rhs]]

    diff = difflib.unified_diff(lhs_s, rhs_s,
                                fromfile='' if name is None else f'Old {name}',
                                tofile='' if name is None else f'New {name}',
                                lineterm='')

    return diff


def print_diff(diff, color=True):
    '''
    Pretty print the difference between 2 json dictionaries to stdout.

    Parameters
    ----------
    diff: list
        A list of the diff text.
    color: bool
        Should the output be colored?
    '''

    interactive_terminal = sys.stdout.isatty()

    if color and interactive_terminal:
        diff = colorize_diff(diff)
    diff_s = '\n'.join(diff)

    if interactive_terminal and os.get_terminal_size().lines < len(diff):
        with subprocess.Popen(['less', '-R'], stdin=subprocess.PIPE, text=True) as pager:
            pager.communicate(diff_s)
    else:
        sys.stdout.write(diff_s + '\n')


def n_diff(lhs, rhs):
    lhs_s, rhs_s = [json.dumps(x, indent=2, sort_keys=False).splitlines() for x in [lhs, rhs]]

    added = 0
    removed = 0
    for l in difflib.ndiff(lhs_s, rhs_s):
        if l.startswith('+ '):
            added += 1
        elif l.startswith('- '):
            removed += 1

    return added, removed


def _element_key(elem: Any) -> Any:
    '''
    Turn elem into something Python can compare across types:
      - dict -> tuple of (key, value_key) pairs in key order
      - list -> tuple of element_keys
      - string -> itself
    '''
    if isinstance(elem, dict):
        return tuple((k, _element_key(elem[k])) for k in sorted(elem))
    if isinstance(elem, list):
        return tuple(_element_key(item) for item in elem)
    return elem


def sort_nested_objs(obj: Any) -> Any:
    '''
    Recursively sort nested objects by their keys and values.

    This function processes dictionaries, lists, and other data types recursively:
    - If the input is a dictionary, it returns a new dictionary with keys sorted
        in ascending order and values processed recursively.
    - If the input is a list, it returns a new list with each element processed
        recursively.
    - For all other data types, the input is returned unchanged.

    Args:
        obj (Any): The input object to be processed. Can be a dictionary, list,
                   or any other data type.

    Returns:
        Any: A new object with dictionaries sorted by keys and nested structures
             processed recursively.
    '''
    if isinstance(obj, dict):
        return {k: sort_nested_objs(obj[k]) for k in sorted(obj)}
    elif isinstance(obj, list):
        canon = [sort_nested_objs(elem) for elem in obj]
        return sorted(canon, key=_element_key)
    else:
        return obj


def load_json_to_dict(name):
    ''' Load json file and handle cases where the file is empty or does not exist. '''

    if name not in TEST_DATA:
        raise ValueError(f'Unknown endpoint {name}.')

    fname = TEST_DATA[name]
    if not os.path.isfile(fname):
        return ''

    with open(fname, 'r', encoding='utf-8') as inF:
        text = inF.read().strip()

    if text == '':
        return ''

    data = json.loads(text)
    return sort_nested_objs(data)


def update_test_data(files, color=True):
    '''
    Update metadata file(s) with new data and print formated summary of the data that has been
    written.

    Parameters
    ----------
    files: list
        A dict where each test data file is the key and the values is a tuple of the new api data
        as the first element and the second element is the path to the existing test data file.
    color: bool
        Write colored output to stdout?
    '''

    diff_counts = {}
    for name, (new_data, test_file) in files.items():
        old_data = load_json_to_dict(test_file)
        diff_counts[name] = n_diff(old_data, new_data)

    if sum(sum(x) for x in diff_counts.values()) == 0:
        sys.stdout.write('All test data is already up to date.\n')
        return

    term_width = os.get_terminal_size().columns
    max_diff = max(sum(x) for x in diff_counts.values())
    max_name = max(len(x) for x in diff_counts)
    plot_width = term_width - sum((max_name, len(str(max_diff)), 3))

    plot_divisor = 1 if max_diff < plot_width else max_diff / plot_width

    n_insertions = 0
    n_deletions = 0
    n_files = 0
    sys.stdout.write('Updating test data...\n')
    for name, (added, removed) in diff_counts.items():
        if added + removed > 0:
            n_insertions += added
            n_deletions += removed
            n_files += 1

            sys.stdout.write(f' {name.rjust(max_name)} | ')
            added_s = '+' * round(added / plot_divisor)
            removed_s = '-' * round(removed / plot_divisor)

            with open(TEST_DATA[files[name][1]], 'w', encoding='utf-8') as outF:
                json.dump(files[name][0], outF, indent=2, sort_keys=False)

            if color and sys.stdout.isatty():
                plot_line = f'{GREEN}{added_s}{RESET}{RED}{removed_s}{RESET}'
            else:
                plot_line = f'{added_s}{removed_s}'
            sys.stdout.write(f'{plot_line}\n')

    sys.stdout.write(f'{n_files} File{"s" if n_files > 1 else ""} changed, ')
    sys.stdout.write(f'{n_insertions} insertion{"s" if n_insertions > 1 else ""}(+), ')
    sys.stdout.write(f'{n_deletions} deletion{"s" if n_deletions > 1 else ""}(-)\n')


def add_duplicate_files(files, studies):
    random.seed(12)
    for study in studies:
        for i in range(2):
            files[study].append({
                'data_category': 'duplicate_file_test',
                'file_format': 'txt',
                'file_id': str(UUID(int=random.getrandbits(128))),
                'file_name': 'empty_file.txt',
                'file_size': '0',
                'file_submitter_id': f'empty_file_{i+1}.txt',
                'file_type': 'text',
                'md5sum': 'd41d8cd98f00b204e9800998ecf8427e'
            })

        files[study].append({
            'data_category': 'duplicate_name_test',
            'file_format': 'txt',
            'file_id': str(UUID(int=random.getrandbits(128))),
            'file_name': 'not_empty_file.txt',
            'file_size': '4',
            'file_submitter_id': f'empty_file_{1}.txt',
            'file_type': 'text',
            'md5sum': 'b026324c6904b2a9cb4b88d6d61c81d1'
        })
        files[study].append({
            'data_category': 'duplicate_name_test',
            'file_format': 'txt',
            'file_id': str(UUID(int=random.getrandbits(128))),
            'file_name': 'not_empty_file.txt',
            'file_size': '4',
            'file_submitter_id': f'empty_file_{2}.txt',
            'file_type': 'text',
            'md5sum': '26ab0db90d72e28ad0ba1e22ee510510'
        })

    return sort_nested_objs(files)


async def download_metadata(pdc_study_ids, endpoints, url=BASE_URL):
    ''' download all study_ids for pdc_study_ids '''

    async with Client(timeout=30, url=url) as client:
        study_metadata_tasks = list()
        async with asyncio.TaskGroup() as tg:
            for study in pdc_study_ids:
                study_metadata_tasks.append(
                    tg.create_task(client.async_get_study_metadata(pdc_study_id=study))
                )
        study_metadata = {pdc_id: task.result() for pdc_id, task in zip(pdc_study_ids, study_metadata_tasks)}
        study_ids = {study['study_id']: study['pdc_study_id'] for study in study_metadata.values()}

        study_metadata_tasks = list()
        study_catalog_tasks = list()
        experiment_tasks = list()
        file_tasks = list()
        sample_tasks = list()
        case_tasks = list()
        async with asyncio.TaskGroup() as tg:
            for study in study_ids:
                if 'study' in endpoints:
                    study_metadata_tasks.append(
                        tg.create_task(client.async_get_study_metadata(study_id=study))
                    )
                if 'studyCatalog' in endpoints:
                    study_catalog_tasks.append(
                        tg.create_task(client.async_get_study_catalog(study_ids[study]))
                    )
                if 'file' in endpoints:
                    file_tasks.append(
                        tg.create_task(client.async_get_study_raw_files(study))
                    )
                if 'experiment' in endpoints:
                    study_submitter_id = study_metadata[study_ids[study]]['study_submitter_id']
                    experiment_tasks.append(
                        tg.create_task(client.async_get_experimental_metadata(study_submitter_id))
                    )
                if 'samples' in endpoints:
                    sample_tasks.append(
                        tg.create_task(client.async_get_study_samples(study))
                    )
                if 'case' in endpoints:
                    case_tasks.append(
                        tg.create_task(client.async_get_study_cases(study))
                    )

    study_metadata = None
    if 'study' in endpoints:
        study_metadata = sort_nested_objs([task.result() for task in study_metadata_tasks])

    study_catalog = None
    if 'studyCatalog' in endpoints:
        study_catalog = sort_nested_objs({study_ids[study]: task.result()
                                          for study, task in zip(study_ids.keys(), study_catalog_tasks)})

    experiment_metadata = None
    if 'experiment' in endpoints:
        experiment_metadata = sort_nested_objs({study_ids[study]: task.result()
                                                for study, task in zip(study_ids.keys(), experiment_tasks)})

    raw_files = None
    if 'file' in endpoints:
        raw_files = {study_ids[study]: task.result()
                     for study, task in zip(study_ids.keys(), file_tasks)}

        # remove url slot from file metadata because the urls are temporary
        for study in raw_files:
            for i in range(len(raw_files[study])):
                raw_files[study][i].pop('url')
        raw_files = sort_nested_objs(raw_files)

    samples = None
    if 'samples' in endpoints:
        samples = sort_nested_objs({study_ids[study]: task.result()
                                    for study, task in zip(study_ids.keys(), sample_tasks)})

    cases = None
    if 'case' in endpoints:
        cases = sort_nested_objs({study_ids[study]: task.result()
                                  for study, task in zip(study_ids.keys(), case_tasks)})

    return {'study': study_metadata,
            'studyCatalog': study_catalog,
            'experiment': experiment_metadata,
            'file': raw_files,
            'samples': samples,
            'case': cases}


def filter_old_data(test_data):
    ''' Remove studies from old data which are not in new data. '''
    data = test_data.copy()

    # filter Study metadata
    if 'study' in data and isinstance(data['study'][1], list):
        study_ids = {study['study_id'] for study in data['study'][0]}
        data['study'][1] = [study for study in data['study'][1] if study['study_id'] in study_ids]

    # filter File metadata
    for endpoint in ['file', 'sample', 'case']:
        if endpoint in data and isinstance(data[endpoint][1], dict):
            data[endpoint][1] = {k: v for k, v in data[endpoint][1].items()
                                 if k in data[endpoint][0]}

    return data


def main():
    parser = argparse.ArgumentParser(description='This script checks if there are any changes to '
                                                 'the test api data in tests/data/api and prints '
                                                 'a summary of the differences.')
    parser.add_argument('-u', '--baseUrl', default=BASE_URL, dest='url',
                        help=f'The base URL for the PDC API. {BASE_URL} is the default.')
    parser.add_argument('--write', action='store_true', default=False,
                        help='Overwrite test files in data/api with new api data?')
    parser.add_argument('--plain', action='store_true', default=False,
                        help="Don't use colors in diff output.")
    parser.add_argument('-e', '--endpoint', action='append',
                        choices=list(ENDPOINTS.keys()), dest='endpoints',
                        help='Specify which endpoints to check. If not specified, all endpoints are checked.')
    parser.add_argument('pdc_study_ids', nargs='*', default=None,
                        help='PDC study ID(s) to download new data for. '
                             'If not specified, data is downloaded for all ids in TEST_STUDIES')

    args = parser.parse_args()

    if args.endpoints is None:
        args.endpoints = list(ENDPOINTS.keys())

    all_studies = len(args.pdc_study_ids) == 0
    pdc_study_ids = STUDIES if all_studies else args.pdc_study_ids

    api_data = asyncio.run(
        download_metadata(pdc_study_ids, args.endpoints, url=args.url)
        )

    # add duplicate files to test data
    if 'file' in args.endpoints:
        api_data['file'] = add_duplicate_files(api_data['file'],
                                               [study for study in DUPLICATE_FILE_TEST_STUDIES
                                                if study in pdc_study_ids])

    test_data = {key: [api_data[key], key] for key, data in api_data.items()
                 if data is not None}

    # remove endpoints that are not specified
    for endpoint in list(test_data.keys()):
        if test_data[endpoint][0] is None:
            test_data.pop(endpoint)

    if args.write:
        update_test_data(test_data, color=not args.plain)
        sys.exit(0)

    # load json files from paths
    for name in test_data:
        test_data[name][1] = load_json_to_dict(test_data[name][1])

    # filter old data of applicable
    if not all_studies:
        test_data = filter_old_data(test_data)

    diff_lines = list()
    for name, (data, old_data) in test_data.items():
        diff = list(get_diff(old_data, data, name=name))
        diff_lines += diff

    if len(diff_lines) == 0:
        for name in test_data:
            sys.stdout.write(f'{ENDPOINTS[name]} up to date\n')
        sys.exit(0)

    print_diff(diff_lines, color=not args.plain)


if __name__ == '__main__':
    main()