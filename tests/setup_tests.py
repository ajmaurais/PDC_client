
import os

TEST_DIR = os.path.dirname(os.path.abspath(__file__))

STUDY_METADATA = f'{TEST_DIR}/data/api/parsed/studies.json'
STUDY_CATALOG = f'{TEST_DIR}/data/api/raw/study_catalog.json'
FILE_METADATA = f'{TEST_DIR}/data/api/parsed/files.json'
ALIQUOT_METADATA = f'{TEST_DIR}/data/api/parsed/aliquots.json'
CASE_METADATA = f'{TEST_DIR}/data/api/parsed/cases.json'


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
