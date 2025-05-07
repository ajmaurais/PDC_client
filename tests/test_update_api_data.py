
import unittest
import json
import random
import copy

import update_api_data
from resources import data as test_data


# class TestSortEndpointData(unittest.TestCase):
#     def setUp(self):
#         with open(test_data.SAMPLE_METADATA, 'r', encoding='utf-8') as f:
#             self.aliquot_data = json.load(f)
#
#     def test_sort_aliquot_data(self):
#         # shuffle the aliquot data
#         random.seed(40)
#         shuffled_data = copy.deepcopy(self.aliquot_data)
#         for study in self.aliquot_data:
#             random.shuffle(shuffled_data[study])
#             shuffled_aliquot_ids = [aliquot['aliquot_id'] for aliquot in shuffled_data[study]]
#             aliquot_ids = [aliquot['aliquot_id'] for aliquot in self.aliquot_data[study]]
#             if len(set(shuffled_aliquot_ids)) == 1:
#                 continue
#             self.assertNotEqual(shuffled_aliquot_ids, aliquot_ids)
#
#         sorted_data = update_api_data.sort_nested_objs(shuffled_data)
#
#         for study in sorted_data:
#             file_ids = [aliquot['file_id_to_aliquot_run_metadata_id'] for aliquot in sorted_data[study]]
#             for aliquot_file_ids in file_ids:
#                 self.assertDictEqual(aliquot_file_ids, aliquot_file_ids))
#
#             first_file_ids = [aliquot['file_id_to_aliquot_run_metadata_id'][0] for aliquot in sorted_data[study]]
#             self.assertEqual(first_file_ids, sorted(first_file_ids))


class TestSortCaseData(unittest.TestCase):
    def setUp(self):
        with open(test_data.CASE_METADATA, 'r', encoding='utf-8') as f:
            self.case_data = json.load(f)

    def test_sort_case_data(self):
        # shuffle the case data
        random.seed(40)
        shuffled_data = copy.deepcopy(self.case_data)
        for study in self.case_data:
            random.shuffle(shuffled_data[study])
            shuffled_case_ids = [case['case_id'] for case in shuffled_data[study]]
            case_ids = [case['case_id'] for case in self.case_data[study]]
            if len(set(shuffled_case_ids)) == 1:
                continue
            self.assertNotEqual(shuffled_case_ids, case_ids)

        sorted_data = update_api_data.sort_nested_objs(shuffled_data)

        for study in sorted_data:
            case_ids = [case['case_id'] for case in sorted_data[study]]
            self.assertEqual(case_ids, sorted(case_ids))