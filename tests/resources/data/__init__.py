
from os.path import getsize
from PDC_client.submodules.io import md5_sum

from .. import TEST_DIR

STUDY_METADATA = f'{TEST_DIR}/resources/data/api/studies.json'
STUDY_CATALOG = f'{TEST_DIR}/resources/data/api/study_catalog.json'
FILE_METADATA = f'{TEST_DIR}/resources/data/api/files.json'
SAMPLE_METADATA = f'{TEST_DIR}/resources/data/api/aliquots.json'
CASE_METADATA = f'{TEST_DIR}/resources/data/api/cases.json'

PDC_TEST_URLS = f'{TEST_DIR}/resources/data/test_urls.json'

PDC_TEST_FILE_IDS = [{"file_id": "127602b9-a2b4-4683-816d-741ebb8bec82",
                      "file_size": "395",
                      "md5sum": "2b30abf33e9931aa1c96061d164fb302",
                      "file_name": "NCI7_Proteomic_Coverage_JHU_Phosphoproteome.sample.txt"},
                     {"file_id": "b2e6a890-1d98-4df1-90a2-d3e1ddb5f72d",
                      "file_size": "503",
                      "md5sum": "5dbbc14c6abb2fc2b402b45ff21b5fbe",
                      "file_name": "NCI7_Experimental_JHU_Proteome.sample.txt"},
                     {"file_id": "e83aa0ba-8047-402b-a52c-4ea8d5253248",
                      "file_size": "548",
                      "md5sum": "cfea698450943c780657daa4d2fc5cc7",
                      "file_name": "102CPTAC_COprospective_W_VU_20160806_09CO018_f06.raw.cap.psm"},
                      {"file_id": "c7d8a4f2-ba60-45ee-a6cb-2046c5f05713",
                       "file_size": "2067",
                       "md5sum": "289464c687fda336abc099fa5d926fef",
                       "file_name": "Phospho_FN12_N221T222_240min_C2_081814.psm"}]

TEST_URLS = [{'url': 'https://raw.githubusercontent.com/ajmaurais/PDC_client/refs/heads/dev/README.md',
              'file_name': 'README.md',
              'md5sum': md5_sum(f'{TEST_DIR}/../README.md'),
              'file_size': getsize(f'{TEST_DIR}/../README.md')}]