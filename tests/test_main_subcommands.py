
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
        result = setup_functions.run_command(command=args, wd=self.work_dir, prefix='study_id')

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), target)


    def test_study_name(self):
        study_id = api_data.get_study_id(TEST_PDC_STUDY_ID)
        target = api_data.studies[study_id]['study_name']

        args = ['PDC_client', 'studyName', study_id, '-u', TEST_URL]
        result = setup_functions.run_command(command=args, wd=self.work_dir, prefix='study_name')

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), target)


    def test_pdc_study_id(self):
        study_id = api_data.get_study_id(TEST_PDC_STUDY_ID)
        target = TEST_PDC_STUDY_ID

        args = ['PDC_client', 'PDCStudyID', study_id, '-u', TEST_URL]
        result = setup_functions.run_command(command=args, wd=self.work_dir, prefix='pdc_study_id')

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), target)

