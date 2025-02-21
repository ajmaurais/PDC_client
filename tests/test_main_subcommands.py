
import os
import unittest

from resources import TEST_DIR, setup_functions
from resources.mock_graphql_server.data import api_data

from PDC_client import main


TEST_PDC_STUDY_ID = 'PDC000504'
# TEST_URL = 'https://pdc.cancer.gov/graphql'
TEST_URL = 'http://localhost:5000/graphql'


class TestStudySubcommands(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.work_dir = f'{TEST_DIR}/work/study_subcommands'
        setup_functions.make_work_dir(cls.work_dir, clear_dir=True)


    def test_study_id(self):
        target = api_data.get_study_id(TEST_PDC_STUDY_ID)

        args = ['PDC_client', 'studyID', TEST_PDC_STUDY_ID, '-u', TEST_URL]
        result = setup_functions.run_command(args, self.work_dir, prefix='study_id')

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), target)


    def test_study_name(self):
        study_id = api_data.get_study_id(TEST_PDC_STUDY_ID)
        target = api_data.studies[study_id]['study_name']

        args = ['PDC_client', 'studyName', study_id, '-u', TEST_URL]
        result = setup_functions.run_command(args, self.work_dir, prefix='study_name')

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), target)


    def test_pdc_study_id(self):
        study_id = api_data.get_study_id(TEST_PDC_STUDY_ID)
        target = TEST_PDC_STUDY_ID

        args = ['PDC_client', 'PDCStudyID', study_id, '-u', TEST_URL]
        result = setup_functions.run_command(args, self.work_dir, prefix='pdc_study_id')

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), target)


class TestMetadataSubcommand(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.work_dir = f'{TEST_DIR}/work/metadata_subcommand'
        setup_functions.make_work_dir(cls.work_dir, clear_dir=True)


    def test_default(self):
        study_id = api_data.get_study_id(TEST_PDC_STUDY_ID)

        args = ['PDC_client', 'metadata', '--prefix=default_', '-u', TEST_URL, study_id]
        result = setup_functions.run_command(args, self.work_dir, prefix='default')

        target_files = [f'default_{TEST_PDC_STUDY_ID}_{file}'
                        for file in ('study_metadata', 'files', 'aliquots', 'cases')]

        self.assertEqual(result.returncode, 0)
        self.assertTrue(all(os.path.exists(f'{self.work_dir}/{f}') for f in target_files))
        self.assertTrue(all(os.path.getsize(f'{self.work_dir}/{f}') > 0 for f in target_files))


    def test_flatten(self):
        pass


    def test_invalid_study_id(self):
        args = ['PDC_client', 'metadata', 'invalid_study_id', '-u', TEST_URL]
        result = setup_functions.run_command(command=args,
                                             wd=self.work_dir, prefix='invalid_study_id')
        self.assertEqual(result.returncode, 1)
        self.assertIn('Invalid study ID', result.stderr)


class TestFileSubcommands(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.work_dir = f'{TEST_DIR}/work/file_subcommands'
        setup_functions.make_work_dir(cls.work_dir, clear_dir=True)

        cls.test_file_url_index = 0
        cls.test_url_files = [
            {'url': 'https://raw.githubusercontent.com/ajmaurais/PDC_client/refs/heads/dev/README.md',
             'file_name': 'README.md',
             'md5': '',
             'size': 0}]

        cls .test_file_id_index = 0
        cls.test_file_id_files = [
            {'file_id': '4d6c2dec-ca0a-4bfe-aa01-b67c45b8c4e4',
             'file_name': 'CPTAC3_non-ccRCC_JHU_Phosphoproteome.label.txt',
             'file_size': '272',
             'md5sum': 'b9498a8e0a62588ab482c21d7bf3cf1f'},
            {'file_id': '22c6de9a-ef6d-4c8c-9a03-1e3e4f8dc4aa',
             'file_name': 'CPTAC3_non-ccRCC_JHU_Phosphoproteome.sample.txt',
             'file_size': '2497',
             'md5sum': '88f64d4343079f242f8a8561e8d89854'}]


    def test_file_id(self):
        if TEST_URL.startswith('http://localhost'):
            self.skipTest('Not implemented for mock server')


    def test_url(self):
        pass