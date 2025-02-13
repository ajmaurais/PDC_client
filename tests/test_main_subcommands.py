
import os
from io import StringIO
import contextlib
import unittest
from unittest.mock import patch

import setup_tests
import dummy_api

from PDC_client import main

TEST_PDC_STUDY_ID = 'PDC000504'


class TestStudySubcommands(unittest.TestCase):
    def test_study_id(self):
        target = dummy_api.get_study_id(TEST_PDC_STUDY_ID)

        ss = StringIO()
        with contextlib.redirect_stdout(ss):
            main.Main(argv=[os.path.abspath(f'{setup_tests.TEST_DIR}/../PDC_client/main.py'),
                            'studyID', TEST_PDC_STUDY_ID])

        ss.seek(0)
        self.assertEqual(ss.read().strip(), target)


    def test_study_name(self):
        study_id = dummy_api.get_study_id(TEST_PDC_STUDY_ID)
        target = dummy_api.get_study_name(study_id)

        ss = StringIO()
        with contextlib.redirect_stdout(ss):
            main.Main(argv=[os.path.abspath(f'{setup_tests.TEST_DIR}/../PDC_client/main.py'),
                            'studyName', study_id])

        ss.seek(0)
        self.assertEqual(ss.read().strip(), target)


    def test_pdc_study_id(self):
        study_id = dummy_api.get_study_id(TEST_PDC_STUDY_ID)
        target = TEST_PDC_STUDY_ID

        ss = StringIO()
        with contextlib.redirect_stdout(ss):
            main.Main(argv=[os.path.abspath(f'{setup_tests.TEST_DIR}/../PDC_client/main.py'),
                            'PDCStudyID', study_id])

        ss.seek(0)
        self.assertEqual(ss.read().strip(), target)
