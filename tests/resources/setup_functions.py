
import os

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
