
import unittest
import json
import random

from PDC_client.submodules import api

import setup_tests


class TestStudyLevel(unittest.TestCase):
    '''
    Unit tests for the StudyLevel API queries.

    Attributes:
        studies (list): A list of study metadata loaded from a JSON file.

    Methods:
        setUpClass(): Loads the study metadata from a JSON file before any tests are run.
        setUp(): Initializes the API client before each test.
        tearDown(): Cleans up the API client after each test.
        do_sucessful_test(f, key, value): Helper method to test successful retrieval of study information.
        test_study_id(): Tests the retrieval of study IDs.
        test_pdc_study_id(): Tests the retrieval of PDC study IDs.
        test_study_name(): Tests the retrieval of study names.
        test_invalid_study_id(): Tests the handling of an invalid study ID.
        test_invalid_pdc_study_id(): Tests the handling of an invalid PDC study ID.
        test_invalid_study_name(): Tests the handling of an invalid study name.
    '''
    @classmethod
    def setUpClass(cls):
        with open(setup_tests.STUDY_METADATA, 'r', encoding='utf-8') as inF:
            cls.studies = json.load(inF)


    def setUp(self):
        self.client = api.Client()


    def tearDown(self):
        del self.client


    def do_sucessful_test(self, f, key, value):
        for study in self.studies:
            self.assertEqual(f(study[key]), study[value])


    def test_study_id(self):
        self.do_sucessful_test(self.client.get_study_id, 'pdc_study_id', 'study_id')


    def test_pdc_study_id(self):
        self.do_sucessful_test(self.client.get_pdc_study_id, 'study_id', 'pdc_study_id')


    def test_study_name(self):
        self.do_sucessful_test(self.client.get_study_name, 'study_id', 'study_name')


    def test_invalid_study_id(self):
        self.assertIsNone(self.client.get_study_id('DUMMY'))


    def test_invalid_pdc_study_id(self):
        self.assertIsNone(self.client.get_pdc_study_id('DUMMY'))


    def test_invalid_study_name(self):
        self.assertIsNone(self.client.get_study_name('DUMMY'))


    def test_only_latest_option(self):
        for study in self.studies:
            data = self.client.get_study_metadata(pdc_study_id='PDC000504',
                                                  only_latest=False)
            self.assertIsInstance(data, list)
            self.assertTrue(all('is_latest_version' in s for s in data))
            self.assertTrue(len([s for s in data if s['is_latest_version']]))


    def test_invalid_study_catalog(self):
        self.assertIsNone(self.client.get_study_catalog('DUMMY'))


class TestFileLevel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(setup_tests.FILE_METADATA, 'r', encoding='utf-8') as inF:
            cls.files = json.load(inF)

        with open(setup_tests.STUDY_METADATA, 'r', encoding='utf-8') as inF:
            study_list = json.load(inF)
        cls.studies = {study.pop('pdc_study_id'): study for study in study_list}
    

    @staticmethod
    def file_list_to_dict(file_list):
        file_dict = dict()
        for file in file_list.copy():
            file_dict[file.pop('file_id')] = file

        return file_dict


    def test_data(self):
        with api.Client() as client:
            for study, study_files in self.files.items():
                test_study_files = client.get_study_raw_files(self.studies[study]['study_id'])

                test_study_files = self.file_list_to_dict(test_study_files)
                study_files = self.file_list_to_dict(study_files)

                self.assertEqual(len(test_study_files), len(study_files))
                for file_id, data in study_files.items():
                    self.assertIn(file_id, test_study_files)

                    # make sure test data has url key then remove it
                    self.assertIn('url', test_study_files[file_id])
                    test_study_files[file_id].pop('url')

                    # check file level data
                    self.assertDictEqual(data, test_study_files[file_id])
                    self.assertEqual(test_study_files[file_id]['data_category'], 'Raw Mass Spectra')
                    self.assertRegex(test_study_files[file_id]['md5sum'], r'^[a-f0-9]{32}$')


    def test_invalid_study(self):
        with api.Client() as client:
            with self.assertLogs(level='ERROR') as cm:
                ret = client.get_study_raw_files('DUMMY')

        self.assertIsNone(ret)
        self.assertTrue('API query failed with response' in cm.output[0])


    def test_n_files_arg(self):
        random.seed(1)
        test_studies = random.sample(list(self.files.keys()), min(len(self.files), 3))

        with api.Client() as client:
            # test behavior of n_files=0
            data = client.get_study_raw_files(self.studies[test_studies[0]]['study_id'], n_files=0)
            self.assertIsInstance(data, list)
            self.assertEqual(len(data), 0)

            # test behavior of n_files > n_files_study
            index = min(len(test_studies) - 1, 1)
            n_files_study = len(self.files[test_studies[index]])
            data = client.get_study_raw_files(self.studies[test_studies[index]]['study_id'],
                                    n_files=n_files_study + 10)
            self.assertEqual(len(data), n_files_study)

            for study in test_studies:
                n_files = len(self.files[study])
                subset_n_files = random.randint(1, n_files - 1)
                data = client.get_study_raw_files(self.studies[study]['study_id'], n_files=subset_n_files)
                self.assertEqual(len(data), subset_n_files)


class TestAliquotLevel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(setup_tests.ALIQUOT_METADATA, 'r', encoding='utf-8') as inF:
            cls.aliquots = json.load(inF)

        with open(setup_tests.STUDY_METADATA, 'r', encoding='utf-8') as inF:
            study_list = json.load(inF)
        cls.studies = {study.pop('pdc_study_id'): study for study in study_list}


    @staticmethod
    def aliquot_list_to_dict(aliquot_list):
        aliquot_dict = dict()
        for aliquot in aliquot_list.copy():
            aliquot_dict[aliquot.pop('aliquot_id')] = aliquot

        return aliquot_dict


    def test_invalid_study(self):
        with api.Client() as client:
            with self.assertLogs(level='ERROR') as cm:
                ret = client.get_study_aliquots('DUMMY')

        self.assertIsNone(ret)
        self.assertTrue('API query failed with response' in cm.output[0])


    def test_data(self):
        page_len = 100
        # for study, study_aliquots in self.aliquots.items():
        study = 'PDC000504'
        study_aliquots = self.aliquots[study]

        study_aliquots = self.aliquot_list_to_dict(study_aliquots)
        
        with api.Client() as client:
            test_study_aliquots = client.get_study_aliquots(self.studies[study]['study_id'],
                                                                page_limit=page_len)

        self.assertEqual(len(test_study_aliquots), self.studies[study]['aliquots_count'])

        test_study_aliquots = self.aliquot_list_to_dict(test_study_aliquots)

        self.assertDictEqual(test_study_aliquots, study_aliquots)


    # def test_file_ids_arg(self):
    #     pass


class TestCaseLevel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(setup_tests.CASE_METADATA, 'r', encoding='utf-8') as inF:
            cls.cases = json.load(inF)

        with open(setup_tests.STUDY_METADATA, 'r', encoding='utf-8') as inF:
            study_list = json.load(inF)
        cls.studies = {study.pop('pdc_study_id'): study for study in study_list}
    

    @staticmethod
    def case_list_to_dict(case_list):
        case_dict = dict()
        for case in case_list.copy():
            case_dict[case.pop('case_id')] = case

        return case_dict


    def test_invalid_study(self):
        with api.Client() as client:
            with self.assertLogs(level='ERROR') as cm:
                ret = client.get_study_cases('DUMMY')

        self.assertIsNone(ret)
        self.assertTrue("Invalid query for study_id: '" in cm.output[0])


    def test_data(self):
        page_limit = 100

        with api.Client() as client:
            for study, study_cases in self.cases.items():
                study_cases = self.case_list_to_dict(study_cases)
                
                test_study_cases = client.get_study_cases(self.studies[study]['study_id'],
                                                          page_limit=page_limit)
                self.assertEqual(len(test_study_cases), self.studies[study]['cases_count'])
                test_study_cases = self.case_list_to_dict(test_study_cases)
                self.assertDictEqual(test_study_cases, study_cases)
