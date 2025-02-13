
import unittest
from unittest import mock
import json

import setup_tests
import dummy_api

from PDC_client.submodules import api as real_api

TEST_PDC_STUDY_ID = 'PDC000504'


class TestAPICalls(unittest.TestCase):
    def get_fxns(self, fxn_name):
        self.assertTrue(hasattr(dummy_api, fxn_name))
        self.assertTrue(hasattr(real_api, fxn_name))

        real_fxn = getattr(real_api, fxn_name)
        dummy_fxn = getattr(dummy_api, fxn_name)

        return real_fxn, dummy_fxn


    def do_valid_value_test(self, fxn_name, *args, **kwargs):
        real_fxn, dummy_fxn = self.get_fxns(fxn_name)

        dummy_value = dummy_fxn(*args, **kwargs)
        real_value = real_fxn(*args, **kwargs)

        self.assertEqual(real_value, dummy_value)


    def do_valid_dict_test(self, fxn_name, *args, **kwargs):
        real_fxn, dummy_fxn = self.get_fxns(fxn_name)

        dummy_value = dummy_fxn(*args, **kwargs)
        real_value = real_fxn(*args, **kwargs)

        self.assertDictEqual(real_value, dummy_value)


    def do_valid_dict_list_test(self, fxn_name, element_id_name,
                                *args, exclude_keys=None, **kwargs):
        '''
        Test whether real and dummy API functions returning lists of dicts return the same output.

        Parameters
        ----------
        fxn_name: str
            The name of the function in both modules to test.
        element_id_name: str
            The name of the key in each dict which uniquely identifies the element.
            For example 'case_id', 'file_id'
        exclude_keys: list
            A list of keys to remove from every dict (if they exist) and exclude from comparisons.
        args: list
            Additional args to pass to test functions
        kwargs: dict
            Additional kwargs to pass to test functions
        '''

        def filter_dict_keys(d):
            if exclude_keys is None:
                return d

            ret = d.copy()
            for key in exclude_keys:
                if key in ret:
                    ret.pop(key)
            return ret

        real_fxn, dummy_fxn = self.get_fxns(fxn_name)

        dummy_values = dummy_fxn(*args, **kwargs)
        real_values = real_fxn(*args, **kwargs)


        self.assertIsNotNone(dummy_values)
        self.assertIsNotNone(real_values)
        real_values = {e.pop(element_id_name): filter_dict_keys(e) for e in real_values}
        dummy_values = {e.pop(element_id_name): filter_dict_keys(e) for e in dummy_values}

        self.assertEqual(len(dummy_values), len(real_values))
        for elem_id, real_elem in real_values.items():
            self.assertIn(elem_id, dummy_values)
            self.assertDictEqual(real_elem, dummy_values[elem_id])


    def do_invalid_value_test(self, fxn_name, *args, **kwargs):
        real_fxn, dummy_fxn = self.get_fxns(fxn_name)

        dummy_value = dummy_fxn(*args, **kwargs)
        real_value = real_fxn(*args, **kwargs)

        if real_value is None:
            self.assertIsNone(dummy_value)
        else:
            self.assertEqual(real_value, dummy_value)


    @mock.patch('PDC_client.submodules.api.LOGGER', mock.Mock())
    def do_invalid_dict_test(self, fxn_name, *args, **kwargs):
        real_fxn, dummy_fxn = self.get_fxns(fxn_name)

        dummy_value = dummy_fxn(*args, **kwargs)
        real_value = real_fxn(*args, **kwargs)

        self.assertIsNone(dummy_value)
        self.assertIsNone(real_value)


    def test_get_study_id(self):
        fxn_name = 'get_study_id'
        self.do_valid_value_test(fxn_name, TEST_PDC_STUDY_ID)
        self.do_invalid_value_test(fxn_name, 'DUMMY_ID')


    def test_get_pdc_study_id(self):
        fxn_name = 'get_pdc_study_id'

        study_id = dummy_api.get_study_id(TEST_PDC_STUDY_ID)
        self.do_valid_value_test(fxn_name, study_id)
        self.do_invalid_value_test(fxn_name, 'DUMMY_ID')


    def test_get_study_name(self):
        fxn_name = 'get_study_name'

        study_id = dummy_api.get_study_id(TEST_PDC_STUDY_ID)
        self.do_valid_value_test(fxn_name, study_id)
        self.do_invalid_value_test(fxn_name, 'DUMMY_ID')


    def test_get_study_metadata(self):
        fxn_name = 'get_study_metadata'

        study_id = dummy_api.get_study_id(TEST_PDC_STUDY_ID)
        self.do_valid_dict_test(fxn_name, study_id=study_id)
        self.do_valid_dict_test(fxn_name, pdc_study_id=TEST_PDC_STUDY_ID)
        self.do_invalid_dict_test(fxn_name, study_id='DUMMY_ID')

        with self.assertRaises(ValueError) as real_cm:
            real_api.get_study_metadata()

        with self.assertRaises(ValueError) as dummy_cm:
            real_api.get_study_metadata()

        self.assertEqual(str(real_cm.exception), str(dummy_cm.exception))


    def test_get_study_raw_files(self):
        fxn_name = 'get_study_raw_files'

        study_id = dummy_api.get_study_id(TEST_PDC_STUDY_ID)
        self.do_valid_dict_list_test(fxn_name, 'file_id', study_id, exclude_keys=['url'])
        self.do_invalid_dict_test(fxn_name, 'DUMMY_ID')


    def test_get_study_aliquots(self):
        fxn_name = 'get_study_aliquots'

        study_id = dummy_api.get_study_id(TEST_PDC_STUDY_ID)
        self.do_valid_dict_list_test(fxn_name, 'aliquot_id', study_id)
        self.do_invalid_dict_test(fxn_name, 'DUMMY_ID')


    def test_get_study_cases(self):
        fxn_name = 'get_study_cases'

        study_id = dummy_api.get_study_id(TEST_PDC_STUDY_ID)
        self.do_valid_dict_list_test(fxn_name, 'case_id', study_id)
        self.do_invalid_dict_test(fxn_name, 'DUMMY_ID')
