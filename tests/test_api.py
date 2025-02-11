
import unittest
import json
import random

from PDC_client.submodules import api

import setup_tests


class TestStudyLevel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(setup_tests.STUDY_METADATA, 'r', encoding='utf-8') as inF:
            cls.studies = json.load(inF)


    def do_sucessful_test(self, f, key, value):
        for study in self.studies:
            self.assertEqual(f(study[key]), study[value])


    def test_study_id(self):
        self.do_sucessful_test(api.get_study_id, 'pdc_study_id', 'study_id')


    def test_pdc_study_id(self):
        self.do_sucessful_test(api.get_pdc_study_id, 'study_id', 'pdc_study_id')


    def test_study_name(self):
        self.do_sucessful_test(api.get_study_name, 'study_id', 'study_name')


    def test_invalid_study_id(self):
        self.assertIsNone(api.get_study_id('DUMMY', api.BASE_URL))


    def test_invalid_pdc_study_id(self):
        self.assertIsNone(api.get_pdc_study_id('DUMMY', api.BASE_URL))


    def test_invalid_study_name(self):
        self.assertIsNone(api.get_study_name('DUMMY', api.BASE_URL))


class TestFileLevel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(setup_tests.FILE_METADATA, 'r', encoding='utf-8') as inF:
            cls.files = json.load(inF)


    @staticmethod
    def file_list_to_dict(file_list):
        file_dict = dict()
        for file in file_list.copy():
            file_dict[file.pop('file_id')] = file

        return file_dict


    def test_data(self):
        for study, study_files in self.files.items():
            test_study_files = api.get_raw_files(study)

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
        with self.assertLogs(level='ERROR') as cm:
            ret = api.get_raw_files('DUMMY')

        self.assertIsNone(ret)
        self.assertTrue('API query failed with response' in cm.output[0])


    def test_n_files_arg(self):
        random.seed(1)
        test_studies = random.sample(list(self.files.keys()), 3)

        # test behavior of n_files=0
        data = api.get_raw_files(test_studies[0], n_files=0)
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 0)

        # test behavior of n_files > n_files_study
        n_files_study = len(self.files[test_studies[1]])
        data = api.get_raw_files(test_studies[1], n_files=n_files_study + 10)
        self.assertEqual(len(data), n_files_study)

        for study in test_studies:
            n_files = len(self.files[study])
            subset_n_files = random.randint(1, n_files - 1)
            data = api.get_raw_files(study, n_files=subset_n_files)
            self.assertEqual(len(data), subset_n_files)
