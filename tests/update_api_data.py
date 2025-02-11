
import sys
import os
import argparse
import json
import difflib
import subprocess
import asyncio
import aiohttp

from PDC_client.submodules import api

from setup_tests import STUDY_METADATA, FILE_METADATA

STUDIES = ['PDC000504', 'PDC000341', 'PDC000414', 'PDC000464',
           'PDC000110']

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


def print_diff(lhs, rhs, name=None, color=True):
    '''
    Pretty print the difference between 2 json dictionaries.

    Parameters
    ----------
    lhs: dict
    rhs: dict
    name: str
        The name of the file that is being compared.
    '''
    lhs_s, rhs_s = [json.dumps(x, indent=2, sort_keys=True).splitlines() for x in [lhs, rhs]]

    diff = difflib.unified_diff(lhs_s, rhs_s,
                                fromfile='' if name is None else f'Old {name}',
                                tofile='' if name is None else f'New {name}',
                                lineterm='')

    interactive_terminal = sys.stdout.isatty()

    if color and interactive_terminal:
        diff = colorize_diff(diff)
    diff_s = '\n'.join(diff)

    if len(diff_s) == 0:
        sys.stdout.write(f'{name} up to date.\n')
        return

    if interactive_terminal and os.get_terminal_size().lines < len(diff):
        with subprocess.Popen(['less', '-R'], stdin=subprocess.PIPE, text=True) as pager:
            pager.communicate(diff_s)
    else:
        sys.stdout.write(diff_s + '\n')


def n_diff(lhs, rhs):
    lhs_s, rhs_s = [json.dumps(x, indent=2, sort_keys=True).splitlines() for x in [lhs, rhs]]

    added = 0
    removed = 0
    for l in difflib.ndiff(lhs_s, rhs_s):
        if l.startswith('+ '):
            added += 1
        elif l.startswith('- '):
            removed += 1

    return added, removed


def load_json_to_dict(fname):
    ''' Load json file and handle cases where the file is empty or does not exist. '''
    if not os.path.isfile(fname):
        return ''

    with open(fname, 'r', encoding='utf-8') as inF:
        text = inF.read().strip()

    if text == '':
        return ''

    return json.loads(text)


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

            with open(files[name][1], 'w', encoding='utf-8') as outF:
                json.dump(files[name][0], outF, indent=2, sort_keys=True)

            if color and sys.stdout.isatty():
                plot_line = f'{GREEN}{added_s}{RESET}{RED}{removed_s}{RESET}'
            else:
                plot_line = f'{added_s}{removed_s}'
            sys.stdout.write(f'{plot_line}\n')

    sys.stdout.write(f'{n_files} File{"s" if n_files > 1 else ""} changed, ')
    sys.stdout.write(f'{n_insertions} insertion{"s" if n_insertions > 1 else ""}(+), ')
    sys.stdout.write(f'{n_deletions} deletion{"s" if n_deletions > 1 else ""}(-)\n')


def check_file(new_data, old_data_path, name='', color=True, write=False):
    old_data = load_json_to_dict(old_data_path)
    if write:
        if old_data != new_data:
            sys.stdout.write(f'Updating {name}...\n')
            with open(old_data_path, 'w', encoding='utf-8') as outF:
                json.dump(new_data, outF, indent=2, sort_keys=True)
    else:
        print_diff(old_data, new_data, name=name, color=color)


async def download_metadata(pdc_study_ids):
    # download all study_ids for pdc_study_ids
    async with aiohttp.ClientSession() as session:
        study_id_tasks = list()
        async with asyncio.TaskGroup() as tg:
            for study in pdc_study_ids:
                study_id_tasks.append(
                    tg.create_task(api._async_get_study_id(session, pdc_study_id=study))
                )
        study_ids = [task.result() for task in study_id_tasks]

        study_metadata_tasks = list()
        file_tasks = list()
        async with asyncio.TaskGroup() as tg:
            for study in study_ids:
                study_metadata_tasks.append(
                    tg.create_task(api._async_get_study_metadata(session, study_id=study))
                )
                file_tasks.append(
                    tg.create_task(api._async_get_raw_files(session, study))
                )

    study_metadata = [task.result() for task in study_metadata_tasks]
    raw_files = {study: task.result() for study, task in zip(study_ids, file_tasks)}

    # remove url slot from file metadata because the urls are temporary
    for study in raw_files:
        for i in range(len(raw_files[study])):
            raw_files[study][i].pop('url')

    return study_metadata, raw_files


def main():
    parser = argparse.ArgumentParser(description='This script checks if there are any changes to '
                                                 'the test api data in tests/data/api and prints '
                                                 'a summary of the differences.')
    parser.add_argument('--write', action='store_true', default=False,
                        help='Overwrite test files in data/api with new api data?')
    parser.add_argument('--plain', action='store_true', default=False,
                        help="Don't use colors in diff output.")
    parser.add_argument('pdc_study_ids', nargs='?', default=None,
                        help='PDC study ID(s) to download new data for. '
                             'If not specified, data is downloaded for all ids in TEST_STUDIES')

    args = parser.parse_args()

    pdc_study_ids = STUDIES if args.pdc_study_ids is None else args.pdc_study_ids

    test_studies, test_files = asyncio.run(download_metadata(pdc_study_ids))

    test_data = {'Study metadata': (test_studies, STUDY_METADATA),
                 'File metadata': (test_files, FILE_METADATA)}

    if args.write:
        update_test_data(test_data, color=not args.plain)
    else:
        for name, (data, path) in test_data.items():
            check_file(data, path, name=name, color=not args.plain)


if __name__ == '__main__':
    main()
