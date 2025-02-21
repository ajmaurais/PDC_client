
from os.path import getsize
from PDC_client.submodules.io import md5_sum

from .. import TEST_DIR

STUDY_METADATA = f'{TEST_DIR}/resources/data/api/studies.json'
STUDY_CATALOG = f'{TEST_DIR}/resources/data/api/study_catalog.json'
FILE_METADATA = f'{TEST_DIR}/resources/data/api/files.json'
ALIQUOT_METADATA = f'{TEST_DIR}/resources/data/api/aliquots.json'
CASE_METADATA = f'{TEST_DIR}/resources/data/api/cases.json'

PDC_TEST_URLS = f'{TEST_DIR}/resources/data/test_urls.json'

PDC_TEST_FILE_IDS = [{'file_id': '4d6c2dec-ca0a-4bfe-aa01-b67c45b8c4e4',
                      'file_name': 'CPTAC3_non-ccRCC_JHU_Phosphoproteome.label.txt',
                      'file_size': '272',
                      'md5sum': 'b9498a8e0a62588ab482c21d7bf3cf1f'},
                     {'file_id': '22c6de9a-ef6d-4c8c-9a03-1e3e4f8dc4aa',
                      'file_name': 'CPTAC3_non-ccRCC_JHU_Phosphoproteome.sample.txt',
                      'file_size': '2497',
                      'md5sum': '88f64d4343079f242f8a8561e8d89854'}]

TEST_URLS = [{'url': 'https://raw.githubusercontent.com/ajmaurais/PDC_client/refs/heads/dev/README.md',
              'file_name': 'README.md',
              'md5sum': md5_sum(f'{TEST_DIR}/../README.md'),
              'file_size': getsize(f'{TEST_DIR}/../README.md')}]