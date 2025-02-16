
import unittest
import subprocess
import os
from signal import SIGTERM
import re
import json
import time
import httpx

# from requests.exceptions import ConnectionError

import setup_tests

from PDC_client.submodules import api

TEST_URL = 'http://localhost:5000/graphql'
PDC_URL = api.BASE_URL

def server_is_running():
    ''' Check if mock graphql server is running.'''
    query = 'query={ __schema { queryType { name }}}'
    try:
        response = httpx.get(f'{TEST_URL}?{query}')
        return response.status_code == 200
    except httpx.ConnectError:
        return False


class TestGraphQLServerBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        '''Set up the test server before running tests.'''

        if server_is_running():
            cls.server_process = None
            return

        cls.work_dir = setup_tests.TEST_DIR + '/work/mock_graphql_server'
        setup_tests.make_work_dir(cls.work_dir, clear_dir=True)
        cls.server_log = open(setup_tests.TEST_DIR + '/server.log', 'w', encoding='utf-8')

        args = ['python', f'{setup_tests.TEST_DIR}/mock_graphql_server.py']
        cls.server_process = subprocess.Popen(args, stderr=cls.server_log, stdout=cls.server_log)

        timeout = 5
        start_time = time.time()
        while time.time() - start_time < timeout:
            if server_is_running():
                cls.started_server = False
                return
            time.sleep(0.5)

        raise RuntimeError("Server did not start within the timeout period")

    @classmethod
    def tearDownClass(cls):
        '''Tear down the test client after running tests.'''
        if cls.server_process is not None:
            print('Killing server process')
            os.kill(cls.server_process.pid, SIGTERM)
            cls.server_process.wait()
            cls.server_log.close()


class TestRawRequests(TestGraphQLServerBase):
    # TEST_PDC_STUDY_ID = 'PDC000504'
    TEST_PDC_STUDY_ID = 'PDC000110'

    def get(self, url, query):
        with httpx.Client() as client:
            query = re.sub(r'\s+', ' ', query.strip())
            response = client.get(f'{url}?{query}')
            return response


    def do_comparison_test(self, query):
        test_response = self.get(TEST_URL, query)
        pdc_response = self.get(PDC_URL, query)

        # Ensure the response is 200 OK
        self.assertEqual(test_response.status_code, 200)
        self.assertEqual(pdc_response.status_code, 200)

        # Parse the response JSON
        test_data = test_response.json()
        pdc_data = pdc_response.json()

        # Assert the response data matches the expected data
        self.assertDictEqual(test_data, pdc_data)


    def test_study_catalog_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._study_catalog_query(self.TEST_PDC_STUDY_ID)
        self.do_comparison_test(query)


    def test_study_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._study_metadata_query('pdc_study_id', self.TEST_PDC_STUDY_ID)
        self.do_comparison_test(query)


class TestClient(TestGraphQLServerBase):
    # TEST_PDC_STUDY_ID = 'PDC000504'
    TEST_PDC_STUDY_ID = 'PDC000110'

    def do_comparison_test(self, function_name, *args, comparison_f, **kwargs):
        with api.Client(url=TEST_URL) as client:
            test_data = getattr(api.Client(), function_name)(*args, **kwargs)
        
        with api.Client(url=PDC_URL) as client:
            pdc_data = getattr(client, function_name)(*args, **kwargs)

        comparison_f(test_data, pdc_data)


    def test_study_id(self):
        self.do_comparison_test('get_study_id', self.TEST_PDC_STUDY_ID,
                                comparison_f=self.assertEqual)
    

    def test_study_id(self):
        self.do_comparison_test('get_study_id', self.TEST_PDC_STUDY_ID,
                                comparison_f=self.assertEqual)