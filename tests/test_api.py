
import unittest
import json

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
