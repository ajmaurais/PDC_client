
import os
from shlex import join as join_shell
import subprocess
from inspect import stack

def make_work_dir(work_dir, clear_dir=False):
    '''
    Setup work directory for test.

    Parameters
    ----------
    clear_dir: bool
        If the directory already exists, should the files already in directory be deleted?
        Will not work recursively or delete directories.
    '''
    if not os.path.isdir(work_dir):
        if os.path.isfile(work_dir):
            raise RuntimeError('Cannot create work directory!')
        os.makedirs(work_dir)
    else:
        if clear_dir:
            for file in os.listdir(work_dir):
                os.remove(f'{work_dir}/{file}')


def run_command(command, wd, prefix=None):
    '''
    Run command in subprocess and write stdout, stderr, return code and command to
    textfiles in specified directory.

    Parameters
    ----------
    command: list
        The command to run. Each argument should be a separate list element.
    wd: str
        The directory to run the command from.
    prefix: str
        A prefix to add to stdout, stderr, rc and command files.
        If None, the name of the calling function is used as the prefix.
    '''
    encoding = 'utf-8'
    result = subprocess.run(command, cwd=wd,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=False, check=False)

    prefix_path = f'{wd}/{prefix if prefix else stack()[1][3]}'

    result.stderr = result.stderr.decode(encoding)
    result.stdout = result.stdout.decode(encoding)

    with open(f'{prefix_path}.command.txt', 'w', encoding=encoding) as outF:
        outF.write(f"{join_shell(command)}\n")
    with open(f'{prefix_path}.stdout.txt', 'w', encoding=encoding) as outF:
        outF.write(f"{result.stdout}\n")
    with open(f'{prefix_path}.stderr.txt', 'w', encoding=encoding) as outF:
        outF.write(f"{result.stderr}\n")
    with open(f'{prefix_path}.rc.txt', 'w', encoding=encoding) as outF:
        outF.write(f'{str(result.returncode)}\n')

    return result