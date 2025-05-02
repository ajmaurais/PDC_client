
import os
import unittest
import json
import re
import random

from resources.setup_functions import make_work_dir, run_command
from resources import TEST_DIR
from resources.data import FILE_METADATA, SAMPLE_METADATA, CASE_METADATA, STUDY_METADATA
from resources.data import PDC_TEST_URLS, TEST_URLS

from PDC_client.submodules import io


class TestMd5(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.work_dir = TEST_DIR + '/work/test_md5'
        make_work_dir(cls.work_dir, clear_dir=True)


    def test_md5(self):
        args = ['md5sum', __file__]
        md5_result = run_command(args, self.work_dir, prefix='md5sum')
        self.assertEqual(md5_result.returncode, 0)
        target = re.split(r'\s+', md5_result.stdout.strip())[0]

        io_md5 = io.md5_sum(__file__)
        self.assertEqual(io_md5, target)


class TestHelperFunctions(unittest.TestCase):

    def test_file_basename(self):
        with open(PDC_TEST_URLS, 'r', encoding='utf-8') as inF:
            test_urls = json.load(inF)

        for file in test_urls:
            result = io.file_basename(file['url'])
            self.assertEqual(result, file['file_name'])


class TestMetadataFunctions(unittest.TestCase):
    def setUp(self):
        with open(STUDY_METADATA, 'r', encoding='utf-8') as inF:
            self.studies = json.load(inF)
            self.studies = {study['pdc_study_id']: study for study in self.studies}
        with open(FILE_METADATA, 'r', encoding='utf-8') as inF:
            self.files = json.load(inF)
        with open(ALIQUOT_METADATA, 'r', encoding='utf-8') as inF:
            self.aliquots = json.load(inF)
        with open(CASE_METADATA, 'r', encoding='utf-8') as inF:
            self.cases = json.load(inF)

        self.study_types = {'dia': [k for k, v in self.studies.items() if io.is_dia(v)],
                            'dda': [k for k, v in self.studies.items() if not io.is_dia(v)]}


    def test_flatten_dia_study(self):
        allowed_null_keys = {'year_of_death', 'year_of_birth', 'cause_of_death', 'vital_status'}
        for pdc_study_id in self.study_types['dia']:
            data = {'study_metadata': self.studies[pdc_study_id],
                    'files': self.files[pdc_study_id],
                    'aliquots': self.aliquots[pdc_study_id],
                    'cases': self.cases[pdc_study_id]}

            target_keys = ['experiment_type', 'analytical_fraction']
            target_keys += data['files'][0].keys()
            target_keys += [k for k in data['aliquots'][0] if k != 'file_ids']
            target_keys += data['cases'][0].keys()
            target_keys = set(target_keys)

            flat_data = io.flatten_metadata(**data)
            for file in flat_data:
                self.assertEqual(len(file), len(target_keys))
                for key, value in file.items():
                    self.assertIn(key, target_keys)
                    if key in allowed_null_keys and value is None:
                            continue
                    if value is None:
                        print(key)
                    self.assertIsInstance(value, str, f'Value for key "{key}" is not a string!')


    def test_flatten_dda_study(self):
        for pdc_study_id in self.study_types['dda']:
            data = {'study_metadata': self.studies[pdc_study_id],
                    'files': self.files[pdc_study_id],
                    'aliquots': self.aliquots[pdc_study_id],
                    'cases': self.cases[pdc_study_id]}

            with self.assertRaises(ValueError) as e:
                io.flatten_metadata(**data)

            self.assertIn('Cannot flatten aliquots with more than 1 file_id.', str(e.exception))


    def test_missing_aliquot(self):
        for pdc_study_id in self.study_types['dia']:
            data = {'study_metadata': self.studies[pdc_study_id],
                    'files': self.files[pdc_study_id],
                    'aliquots': self.aliquots[pdc_study_id],
                    'cases': self.cases[pdc_study_id]}

            # remove random aliquot from aliquots
            random.seed(42)
            file_id = random.choice(data['files'])['file_id']
            for i, aliquot in enumerate(data['aliquots']):
                if file_id in aliquot['file_ids']:
                    data['aliquots'].pop(i)
                    break

            with self.assertRaises(ValueError) as e:
                io.flatten_metadata(**data)

            self.assertIn(f'No aliquot data found for file_id: {file_id}', str(e.exception))


    def test_read_metadata_tsv(self):
        test_study = 'PDC000504'
        test_file = f'{TEST_DIR}/resources/data/output/PDC000504_flat.tsv'
        with open(test_file, 'r', encoding='utf-8') as inF:
            data = io.read_file_metadata(inF, format='tsv')
        self.assertEqual(len(data), len(self.files[test_study]))


    def test_missing_case(self):
        for pdc_study_id in self.study_types['dia']:
            data = {'study_metadata': self.studies[pdc_study_id],
                    'files': self.files[pdc_study_id],
                    'aliquots': self.aliquots[pdc_study_id],
                    'cases': self.cases[pdc_study_id]}

            # remove random case from cases
            random.seed(3)
            aliquot_id_i = random.randint(0, len(data['aliquots']))
            case_id = data['aliquots'][aliquot_id_i]['case_id']
            file_ids = [a['file_ids'][0] for a in data['aliquots'] if a['case_id'] == case_id]
            for i, case in enumerate(data['cases']):
                if case['case_id'] == case_id:
                    data['cases'].pop(i)
                    break

            with self.assertRaises(ValueError) as e:
                io.flatten_metadata(**data)

            self.assertTrue(any(f'No case data found for file_id: {file_id}' in str(e.exception)
                                for file_id in file_ids))


class TestDownloadFile(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.work_dir = f'{TEST_DIR}/work/download_file'
        make_work_dir(cls.work_dir, clear_dir=True)


    def test_download_file(self):
        for file in TEST_URLS:
            self.assertTrue(io.download_file(file['url'], f"{self.work_dir}/{file['file_name']}",
                                             expected_md5=file['md5sum'], expected_size=file['file_size']))
            self.assertTrue(os.path.isfile(f'{self.work_dir}/{file["file_name"]}'))
            self.assertEqual(io.md5_sum(f'{self.work_dir}/{file["file_name"]}'), file['md5sum'])
            self.assertEqual(os.path.getsize(f'{self.work_dir}/{file["file_name"]}'), file['file_size'])


    def test_bad_md5(self):
        file = TEST_URLS[0]
        target_md5 = io.md5_sum(f'{TEST_DIR}/../Dockerfile')
        target_file_name = f'{self.work_dir}/{file["file_name"]}'
        with self.assertLogs(level='ERROR') as cm:
            self.assertFalse(io.download_file(file['url'], target_file_name,
                                              expected_md5=target_md5,
                                              expected_size=file['file_size'],
                                              n_retries=1))

        self.assertTrue(any(f'Expected MD5 checksum does not match for file "{target_file_name}"' in msg
                            for msg in cm.output), cm.output)


    def test_bad_size(self):
        file = TEST_URLS[0]
        target_size = 4
        target_file_name = f'{self.work_dir}/{file["file_name"]}'
        with self.assertLogs(level='ERROR') as cm:
            self.assertFalse(io.download_file(file['url'], target_file_name,
                                              expected_md5=file['md5sum'],
                                              expected_size=target_size,
                                              n_retries=1))

        self.assertTrue(any(f'Expected file size does not match for file "{target_file_name}"' in msg
                            for msg in cm.output), cm.output)


    def test_bad_url(self):
        file = TEST_URLS[0]
        url = 'https://www.nowwhere.com/this/is/a/bad/url'
        target_file_name = f'{self.work_dir}/{file["file_name"]}'
        with self.assertLogs(level='ERROR') as cm:
            self.assertFalse(io.download_file(url, target_file_name,
                                              expected_md5=file['md5sum'],
                                              expected_size=file['file_size'],
                                              n_retries=1))

        self.assertTrue(any(f'Failed to download file "{target_file_name}" after 1 attempt(s)' in msg
                            for msg in cm.output), cm.output)


    def test_skip_md5(self):
        file = TEST_URLS[0]
        target_file_name = f'{self.work_dir}/{file["file_name"]}'
        with self.assertLogs(level='WARNING') as cm:
            self.assertTrue(io.download_file(file['url'], target_file_name,
                                             expected_size=file['file_size'],
                                             n_retries=1))

        self.assertTrue(any(f'Skipping md5 check for file "{target_file_name}"' in msg
                            for msg in cm.output), cm.output)


    def test_skip_size(self):
        file = TEST_URLS[0]
        target_file_name = f'{self.work_dir}/{file["file_name"]}'
        with self.assertLogs(level='WARNING') as cm:
            self.assertTrue(io.download_file(file['url'], target_file_name,
                                             expected_md5=file['md5sum'],
                                             n_retries=1))

        self.assertTrue(any(f'Skipping size check for file "{target_file_name}"' in msg
                            for msg in cm.output), cm.output)