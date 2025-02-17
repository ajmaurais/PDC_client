
import unittest
import subprocess
import os
from signal import SIGTERM
import re
import json
import time
import httpx

from resources import TEST_DIR
from resources.setup_functions import make_work_dir
from resources.data import STUDY_METADATA

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

        # setup dict to look up study ids
        with open(STUDY_METADATA, 'r', encoding='utf-8') as inF:
            studies = json.load(inF)
        cls.study_ids = {study['pdc_study_id']: study['study_id'] for study in studies}

        # return if server is already running
        if server_is_running():
            cls.server_process = None
            return

        cls.work_dir = TEST_DIR + '/work/mock_graphql_server'
        make_work_dir(cls.work_dir, clear_dir=True)
        cls.server_log = open(cls.work_dir + '/server.log', 'w', encoding='utf-8')

        args = ['python', f'{TEST_DIR}/mock_graphql_server.py']
        args = ['python', '-m', 'resources.mock_graphql_server.server']
        cls.server_process = subprocess.Popen(args, cwd=TEST_DIR,
                                              stderr=cls.server_log, stdout=cls.server_log)

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
        with httpx.Client(timeout=300) as client:
            query = re.sub(r'\s+', ' ', query.strip())
            response = client.get(f'{url}?{query}')
            return response


    def post(self, url, query):
        with httpx.Client(timeout=300) as client:
            query = re.sub(r'\s+', ' ', query.strip())
            response = client.post(url, json={'query': query})
            return response


    def do_comparison_test(self, query):
        pdc_response = self.get(PDC_URL, query)
        test_response = self.get(TEST_URL, query)

        # Ensure the response is 200 OK
        self.assertEqual(pdc_response.status_code, 200)
        self.assertEqual(test_response.status_code, 200)

        # Parse the response JSON
        pdc_data = pdc_response.json()
        test_data = test_response.json()

        # Assert the response data matches the expected data
        self.assertDictEqual(test_data, pdc_data)


    def test_study_catalog_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._study_catalog_query(self.TEST_PDC_STUDY_ID)
        self.do_comparison_test(query)


    def test_invalid_study_catalog_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._study_catalog_query('INVALID_STUDY_ID')
        self.do_comparison_test(query)


    def test_study_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._study_metadata_query('pdc_study_id', self.TEST_PDC_STUDY_ID)
        self.do_comparison_test(query)


    def test_invalid_study_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._study_metadata_query('pdc_study_id', 'INVALID_STUDY_ID')
        self.do_comparison_test(query)


    def test_files_per_study_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._study_raw_file_query(self.study_ids[self.TEST_PDC_STUDY_ID])
        pdc_response = self.post(PDC_URL, query)
        test_response = self.post(TEST_URL, query)

        # Ensure the response is 200 OK
        self.assertEqual(pdc_response.status_code, 200)
        self.assertEqual(test_response.status_code, 200)

        # Parse the response JSON
        pdc_data = pdc_response.json()
        test_data = test_response.json()

        def clear_url(data):
            self.assertIn('data', data)
            self.assertIn('filesPerStudy', data['data'])
            ret = dict()
            for file in data['data']['filesPerStudy']:
                self.assertIn('file_id', file)
                self.assertIn('signedUrl', file)
                self.assertIn('url', file['signedUrl'])
                file['signedUrl']['url'] = ''
                ret[file['file_id']] = file
            return ret

        pdc_data = clear_url(pdc_data)
        test_data = clear_url(test_data)
        self.assertEqual(len(pdc_data), len(test_data))

        # Assert the response data matches the expected data
        for file_id, file in pdc_data.items():
            self.assertIn(file_id, test_data)
            self.assertDictEqual(test_data[file_id], file)


class TestClient(TestGraphQLServerBase):
    # TEST_PDC_STUDY_ID = 'PDC000504'
    TEST_PDC_STUDY_ID = 'PDC000110'

    def do_comparison_test(self, function_name, *args, comparison_f, **kwargs):
        with api.Client(url=PDC_URL) as client:
            pdc_data = getattr(client, function_name)(*args, **kwargs)
        with api.Client(url=TEST_URL) as client:
            test_data = getattr(api.Client(), function_name)(*args, **kwargs)

        comparison_f(test_data, pdc_data)


    def do_invalid_test(self, function_name, *args, **kwargs):
        with api.Client(url=PDC_URL) as client:
            pdc_data = getattr(client, function_name)(*args, **kwargs)
        with api.Client(url=TEST_URL) as client:
            test_data = getattr(client, function_name)(*args, **kwargs)

        self.assertIsNone(test_data)
        self.assertIsNone(pdc_data)


    def test_study_id(self):
        self.do_comparison_test('get_study_id', self.TEST_PDC_STUDY_ID,
                                comparison_f=self.assertEqual)
        self.do_invalid_test('get_study_id', 'INVALID_STUDY_ID')

    def test_study_name(self):
        self.do_comparison_test('get_study_name', self.study_ids[self.TEST_PDC_STUDY_ID],
                                comparison_f=self.assertEqual)
        self.do_invalid_test('get_study_name', 'INVALID_STUDY_ID')


    def test_pdc_study_id(self):
        self.do_comparison_test('get_pdc_study_id', self.study_ids[self.TEST_PDC_STUDY_ID],
                                comparison_f=self.assertEqual)
        self.do_invalid_test('get_pdc_study_id', 'INVALID_STUDY_ID')
