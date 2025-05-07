
from os.path import getsize
from PDC_client.submodules.io import md5_sum

from .. import TEST_DIR

STUDY_METADATA = f'{TEST_DIR}/resources/data/api/studies.json'
STUDY_CATALOG = f'{TEST_DIR}/resources/data/api/study_catalog.json'
EXPERIMENT_METADATA = f'{TEST_DIR}/resources/data/api/experiments.json'
FILE_METADATA = f'{TEST_DIR}/resources/data/api/files.json'
SAMPLE_METADATA = f'{TEST_DIR}/resources/data/api/samples.json'
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


MISSING_SRM_IDS = {
    "01CPTAC_OVprospective_G_JHUZ_20160317_QE_r01.raw": {
        "file_id": "b1c62d32-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "c94fd04b-fa93-426a-a3f8-8ddc349d4cd7",
        "study_run_metadata_submitter_id": "01CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "01CPTAC_OVprospective_G_JHUZ_20160317_QE_r03.raw": {
        "file_id": "b1c744be-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "c94fd04b-fa93-426a-a3f8-8ddc349d4cd7",
        "study_run_metadata_submitter_id": "01CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "03CPTAC_OVprospective_G_JHUZ_20160317_QE_r01.raw": {
        "file_id": "b1c8a412-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "3efaab99-7a3c-4dff-9729-4440d552f25f",
        "study_run_metadata_submitter_id": "03CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "02CPTAC_OVprospective_G_JHUZ_20160317_QE_r01.raw": {
        "file_id": "b1c97847-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "0ca02ba4-b702-4bf8-8d1a-b1a51a23b85e",
        "study_run_metadata_submitter_id": "02CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "01CPTAC_OVprospective_G_JHUZ_20160317_QE_r02.raw": {
        "file_id": "b1cb5217-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "c94fd04b-fa93-426a-a3f8-8ddc349d4cd7",
        "study_run_metadata_submitter_id": "01CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "02CPTAC_OVprospective_G_JHUZ_20160317_QE_r03.raw": {
        "file_id": "b1cd4c91-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "0ca02ba4-b702-4bf8-8d1a-b1a51a23b85e",
        "study_run_metadata_submitter_id": "02CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "04CPTAC_OVprospective_G_JHUZ_20160317_QE_r01.raw": {
        "file_id": "b1cee1e2-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "1bc503e8-fb4a-4ec8-b387-ea6d4ebb937b",
        "study_run_metadata_submitter_id": "04CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "03CPTAC_OVprospective_G_JHUZ_20160317_QE_r03.raw": {
        "file_id": "b1d0c57c-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "3efaab99-7a3c-4dff-9729-4440d552f25f",
        "study_run_metadata_submitter_id": "03CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "03CPTAC_OVprospective_G_JHUZ_20160317_QE_r02.raw": {
        "file_id": "b1d2217f-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "3efaab99-7a3c-4dff-9729-4440d552f25f",
        "study_run_metadata_submitter_id": "03CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "02CPTAC_OVprospective_G_JHUZ_20160317_QE_r02.raw": {
        "file_id": "b1d37b19-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "0ca02ba4-b702-4bf8-8d1a-b1a51a23b85e",
        "study_run_metadata_submitter_id": "02CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "04CPTAC_OVprospective_G_JHUZ_20160317_QE_r02.raw": {
        "file_id": "b34a56ca-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "1bc503e8-fb4a-4ec8-b387-ea6d4ebb937b",
        "study_run_metadata_submitter_id": "04CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "04CPTAC_OVprospective_G_JHUZ_20160317_QE_r03.raw": {
        "file_id": "b395df11-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "1bc503e8-fb4a-4ec8-b387-ea6d4ebb937b",
        "study_run_metadata_submitter_id": "04CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "05CPTAC_OVprospective_G_JHUZ_20160317_QE_r01.raw": {
        "file_id": "b3a85e57-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "e6b79f2d-6f33-42c5-8361-78e56d7f70ff",
        "study_run_metadata_submitter_id": "05CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "05CPTAC_OVprospective_G_JHUZ_20160317_QE_r02.raw": {
        "file_id": "b3b462e0-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "e6b79f2d-6f33-42c5-8361-78e56d7f70ff",
        "study_run_metadata_submitter_id": "05CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "05CPTAC_OVprospective_G_JHUZ_20160317_QE_r03.raw": {
        "file_id": "b3e47ee3-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "e6b79f2d-6f33-42c5-8361-78e56d7f70ff",
        "study_run_metadata_submitter_id": "05CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "06CPTAC_OVprospective_G_JHUZ_20160317_QE_r01.raw": {
        "file_id": "b4150862-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "3886817e-1c14-4917-ab26-2f8ef704ffeb",
        "study_run_metadata_submitter_id": "06CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "06CPTAC_OVprospective_G_JHUZ_20160317_QE_r02.raw": {
        "file_id": "b49785c5-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "3886817e-1c14-4917-ab26-2f8ef704ffeb",
        "study_run_metadata_submitter_id": "06CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "06CPTAC_OVprospective_G_JHUZ_20160317_QE_r03.raw": {
        "file_id": "b504669d-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "3886817e-1c14-4917-ab26-2f8ef704ffeb",
        "study_run_metadata_submitter_id": "06CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "07CPTAC_OVprospective_G_JHUZ_20160317_QE_r01.raw": {
        "file_id": "b507e610-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "44157373-8a97-4a53-b915-33e66a937534",
        "study_run_metadata_submitter_id": "07CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "07CPTAC_OVprospective_G_JHUZ_20160317_QE_r02.raw": {
        "file_id": "b51859f7-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "44157373-8a97-4a53-b915-33e66a937534",
        "study_run_metadata_submitter_id": "07CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "07CPTAC_OVprospective_G_JHUZ_20160317_QE_r03.raw": {
        "file_id": "b52a3a4a-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "44157373-8a97-4a53-b915-33e66a937534",
        "study_run_metadata_submitter_id": "07CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "08CPTAC_OVprospective_G_JHUZ_20160317_QE_r01.raw": {
        "file_id": "b60df3cc-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "9d541319-7725-4ae3-93b7-a7a61f966317",
        "study_run_metadata_submitter_id": "08CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "08CPTAC_OVprospective_G_JHUZ_20160317_QE_r02.raw": {
        "file_id": "b63d4e36-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "9d541319-7725-4ae3-93b7-a7a61f966317",
        "study_run_metadata_submitter_id": "08CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "08CPTAC_OVprospective_G_JHUZ_20160317_QE_r03.raw": {
        "file_id": "b65063e0-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "9d541319-7725-4ae3-93b7-a7a61f966317",
        "study_run_metadata_submitter_id": "08CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "09CPTAC_OVprospective_G_JHUZ_20160317_QE_r01.raw": {
        "file_id": "b6661fb7-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "1e10224b-77d2-489e-930b-83e425362bdb",
        "study_run_metadata_submitter_id": "09CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "09CPTAC_OVprospective_G_JHUZ_20160317_QE_r02.raw": {
        "file_id": "b68842c6-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "1e10224b-77d2-489e-930b-83e425362bdb",
        "study_run_metadata_submitter_id": "09CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "09CPTAC_OVprospective_G_JHUZ_20160317_QE_r03.raw": {
        "file_id": "b74bf326-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "1e10224b-77d2-489e-930b-83e425362bdb",
        "study_run_metadata_submitter_id": "09CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "10CPTAC_OVprospective_G_JHUZ_20160317_QE_r01.raw": {
        "file_id": "b7962152-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "a0db8a42-792b-4384-a49d-0f1ed8b6a578",
        "study_run_metadata_submitter_id": "10CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "10CPTAC_OVprospective_G_JHUZ_20160317_QE_r02.raw": {
        "file_id": "b81f3c14-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "a0db8a42-792b-4384-a49d-0f1ed8b6a578",
        "study_run_metadata_submitter_id": "10CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "10CPTAC_OVprospective_G_JHUZ_20160317_QE_r03.raw": {
        "file_id": "b85aa18d-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "a0db8a42-792b-4384-a49d-0f1ed8b6a578",
        "study_run_metadata_submitter_id": "10CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "11CPTAC_OVprospective_G_JHUZ_20160317_QE_r01.raw": {
        "file_id": "b9164b40-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "3f3d76c4-e24e-4de4-88f7-ca927c337898",
        "study_run_metadata_submitter_id": "11CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "11CPTAC_OVprospective_G_JHUZ_20160317_QE_r02.raw": {
        "file_id": "b94ff47a-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "3f3d76c4-e24e-4de4-88f7-ca927c337898",
        "study_run_metadata_submitter_id": "11CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "11CPTAC_OVprospective_G_JHUZ_20160317_QE_r03.raw": {
        "file_id": "ba1c18d8-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "3f3d76c4-e24e-4de4-88f7-ca927c337898",
        "study_run_metadata_submitter_id": "11CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "12CPTAC_OVprospective_G_JHUZ_20160317_QE_r01.raw": {
        "file_id": "baa3afb8-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "11664b50-30ec-44c3-88fd-7af0e08cc284",
        "study_run_metadata_submitter_id": "12CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "12CPTAC_OVprospective_G_JHUZ_20160317_QE_r02.raw": {
        "file_id": "baf0b780-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "11664b50-30ec-44c3-88fd-7af0e08cc284",
        "study_run_metadata_submitter_id": "12CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "12CPTAC_OVprospective_G_JHUZ_20160317_QE_r03.raw": {
        "file_id": "bb231629-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "11664b50-30ec-44c3-88fd-7af0e08cc284",
        "study_run_metadata_submitter_id": "12CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "13CPTAC_OVprospective_G_JHUZ_20160317_QE_r01.raw": {
        "file_id": "bb796c6c-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "422fd532-f2f3-4bd3-8203-261de55fada9",
        "study_run_metadata_submitter_id": "13CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "13CPTAC_OVprospective_G_JHUZ_20160317_QE_r02.raw": {
        "file_id": "bbb8b3e5-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "422fd532-f2f3-4bd3-8203-261de55fada9",
        "study_run_metadata_submitter_id": "13CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "13CPTAC_OVprospective_G_JHUZ_20160317_QE_r03.raw": {
        "file_id": "bc82509a-0277-11eb-bc0e-0aad30af8a83",
        "study_run_metadata_id": "422fd532-f2f3-4bd3-8203-261de55fada9",
        "study_run_metadata_submitter_id": "13CPTAC_OVprospective_G_JHUZ_20160317"
    },
    "02CPTAC_CompRef_C_GBM_A_PNNL_20211027_B2S6_f03.raw": {
        "file_id": "0345eeae-3c1d-4101-a0f9-f5b3bcb81720",
        "study_run_metadata_id": "97849085-c2aa-419e-9714-1a8d926baf92",
        "study_run_metadata_submitter_id": "02CPTAC_CompRef_C_GBM_Acetylome_PNNL_20211027"
    },
    "03CPTAC_CompRef_C_GBM_A_PNNL_20211110_B3S6_f01.raw": {
        "file_id": "0995824f-3f7c-472e-8ae7-ac82d0e67c0d",
        "study_run_metadata_id": "7c761f54-8385-4d84-86c1-7c8046f0860a",
        "study_run_metadata_submitter_id": "03CPTAC_CompRef_C_GBM_Acetylome_PNNL_20211110"
    },
    "03CPTAC_CompRef_C_GBM_A_PNNL_20211110_B3S6_f04.raw": {
        "file_id": "1d94225b-a0d8-471b-8c06-c4ad06fb73ad",
        "study_run_metadata_id": "7c761f54-8385-4d84-86c1-7c8046f0860a",
        "study_run_metadata_submitter_id": "03CPTAC_CompRef_C_GBM_Acetylome_PNNL_20211110"
    },
    "02CPTAC_CompRef_C_GBM_A_PNNL_20211027_B2S6_f02.raw": {
        "file_id": "4cc78809-87a6-4572-98c8-59cabb1370b7",
        "study_run_metadata_id": "97849085-c2aa-419e-9714-1a8d926baf92",
        "study_run_metadata_submitter_id": "02CPTAC_CompRef_C_GBM_Acetylome_PNNL_20211027"
    },
    "01CPTAC_CompRef_C_GBM_A_PNNL_20211025_B1S6_f02.raw": {
        "file_id": "5297353f-4cf9-42e4-81dc-a01e18ff8898",
        "study_run_metadata_id": "b893426c-1fda-45db-9700-b9437d270a58",
        "study_run_metadata_submitter_id": "01CPTAC_CompRef_C_GBM_Acetylome_PNNL_20211025"
    },
    "03CPTAC_CompRef_C_GBM_A_PNNL_20211110_B3S6_f02.raw": {
        "file_id": "5f2a2c56-0ab9-4093-85d5-9e9a03cba500",
        "study_run_metadata_id": "7c761f54-8385-4d84-86c1-7c8046f0860a",
        "study_run_metadata_submitter_id": "03CPTAC_CompRef_C_GBM_Acetylome_PNNL_20211110"
    },
    "03CPTAC_CompRef_C_GBM_A_PNNL_20211110_B3S6_f03.raw": {
        "file_id": "777e7537-3adc-4555-ba7a-dac9f429373f",
        "study_run_metadata_id": "7c761f54-8385-4d84-86c1-7c8046f0860a",
        "study_run_metadata_submitter_id": "03CPTAC_CompRef_C_GBM_Acetylome_PNNL_20211110"
    },
    "02CPTAC_CompRef_C_GBM_A_PNNL_20211027_B2S6_f04.raw": {
        "file_id": "7fbf0c84-5ccf-4fcc-ba4b-e92e10e82c6c",
        "study_run_metadata_id": "97849085-c2aa-419e-9714-1a8d926baf92",
        "study_run_metadata_submitter_id": "02CPTAC_CompRef_C_GBM_Acetylome_PNNL_20211027"
    },
    "02CPTAC_CompRef_C_GBM_A_PNNL_20211027_B2S6_f01.raw": {
        "file_id": "aec9977f-ebda-4a85-805e-59cce58879a7",
        "study_run_metadata_id": "97849085-c2aa-419e-9714-1a8d926baf92",
        "study_run_metadata_submitter_id": "02CPTAC_CompRef_C_GBM_Acetylome_PNNL_20211027"
    },
    "01CPTAC_CompRef_C_GBM_A_PNNL_20211025_B1S6_f01.raw": {
        "file_id": "ba79915b-5674-4b89-8955-c2b7d93e0e71",
        "study_run_metadata_id": "b893426c-1fda-45db-9700-b9437d270a58",
        "study_run_metadata_submitter_id": "01CPTAC_CompRef_C_GBM_Acetylome_PNNL_20211025"
    },
    "01CPTAC_CompRef_C_GBM_A_PNNL_20211025_B1S6_f04.raw": {
        "file_id": "c7cbedec-fcea-474c-a9c2-d7f1f8e758b6",
        "study_run_metadata_id": "b893426c-1fda-45db-9700-b9437d270a58",
        "study_run_metadata_submitter_id": "01CPTAC_CompRef_C_GBM_Acetylome_PNNL_20211025"
    },
    "01CPTAC_CompRef_C_GBM_A_PNNL_20211025_B1S6_f03.raw": {
        "file_id": "de95bacc-9a64-43a7-839c-f67198dfeada",
        "study_run_metadata_id": "b893426c-1fda-45db-9700-b9437d270a58",
        "study_run_metadata_submitter_id": "01CPTAC_CompRef_C_GBM_Acetylome_PNNL_20211025"
    },
    "empty_file.txt": {
        "file_id": "87751d4c-a850-1e2c-44dc-da6a797d76de",
        "study_run_metadata_submitter_id": None,
        "study_run_metadata_id": None
    },
    "not_empty_file.txt": {
        "file_id": "7b87a9e2-5fef-e911-ff22-a27b02c7bff2",
        "study_run_metadata_submitter_id": None,
        "study_run_metadata_id": None
    }
}