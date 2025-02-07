
import unittest

from PDC_client.submodules import api

import setup_tests


class TestStudyLevel(unittest.TestCase):
    STUDIES = setup_tests.STUDIES

    def do_sucessful_test(self, f, key, value):
        for study in self.STUDIES:
            self.assertEqual(f(study[key], api.BASE_URL), study[value])


    def test_study_id(self):
        self.do_sucessful_test(api.study_id, 'pdc_study_id', 'study_id')


    def test_pdc_study_id(self):
        self.do_sucessful_test(api.pdc_study_id, 'study_id', 'pdc_study_id')


    def test_study_name(self):
        self.do_sucessful_test(api.study_name, 'study_id', 'study_name')


    def test_invalid_study_id(self):
        self.assertIsNone(api.study_id('DUMMY', api.BASE_URL))


    def test_invalid_pdc_study_id(self):
        self.assertIsNone(api.pdc_study_id('DUMMY', api.BASE_URL))


    def test_invalid_study_name(self):
        self.assertIsNone(api.study_name('DUMMY', api.BASE_URL))


    def test_



