
import unittest
import random
import subprocess
import os
from signal import SIGTERM
import re
import time
import httpx

from resources import TEST_DIR
from resources.setup_functions import make_work_dir
from resources.mock_graphql_server.data import api_data

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


def data_list_to_dict(data_list, key):
    ret = dict()
    for data in data_list.copy():
        ret[data[key]] = data

    return ret


class TestGraphQLServerBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        '''Set up the test server before running tests.'''

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
    TEST_PDC_STUDY_ID = 'PDC000504'
    # TEST_PDC_STUDY_ID = 'PDC000110'

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


    def get_paired_data(self, query):
        pdc_response = self.get(PDC_URL, query)
        test_response = self.get(TEST_URL, query)

        # Ensure the response is 200 OK
        self.assertEqual(pdc_response.status_code, 200)
        self.assertEqual(test_response.status_code, 200)

        # Parse the response JSON
        pdc_data = pdc_response.json()
        test_data = test_response.json()

        return pdc_data, test_data


    def do_comparison_test(self, query):
        pdc_data, test_data = self.get_paired_data(query)

        # Assert the response data matches the expected data
        self.assertDictEqual(test_data, pdc_data)


    def test_all_queries_covered(self):
        client_quries = [a for a in dir(api.Client)
                         if callable(getattr(api.Client, a)) and re.search(r'^_[a-z_]+_query$', a)]

        client_test_names = [f'test{a}' for a in client_quries]
        client_invalid_test_names = [f'test_invalid{a}' for a in client_quries]

        test_queries = {a for a in dir(self)
                        if callable(getattr(self, a)) and re.search(r'^test_[a-z_]+_query$', a)}

        for test_name in client_test_names:
            self.assertIn(test_name, test_queries)
        for test_name in client_invalid_test_names:
            self.assertIn(test_name, test_queries)


    def test_study_catalog_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._study_catalog_query(self.TEST_PDC_STUDY_ID)
        self.do_comparison_test(query)


    def test_invalid_study_catalog_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._study_catalog_query('INVALID_STUDY_ID')
        self.do_comparison_test(query)


    def test_study_metadata_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._study_metadata_query('pdc_study_id', self.TEST_PDC_STUDY_ID)
        self.do_comparison_test(query)


    def test_invalid_study_metadata_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._study_metadata_query('pdc_study_id', 'INVALID_STUDY_ID')
        self.do_comparison_test(query)


    def test_case_aliquot_query(self):
        self.assertTrue(server_is_running())

        study_id = api_data.get_study_id(self.TEST_PDC_STUDY_ID)
        limit = 10
        offset = 0

        query = api.Client._case_aliquot_query(study_id, offset, limit)

        pdc_data, test_data = self.get_paired_data(query)
        pdc_data = pdc_data['data']['paginatedCasesSamplesAliquots']
        test_data = test_data['data']['paginatedCasesSamplesAliquots']

        self.assertEqual(pdc_data['total'], test_data['total'])
        self.assertDictEqual(pdc_data['pagination'], test_data['pagination'])

        pdc_cases = {case['case_id']: case['samples'] for case in pdc_data['casesSamplesAliquots']}
        test_cases = {case['case_id']: case['samples'] for case in test_data['casesSamplesAliquots']}
        self.assertEqual(len(pdc_cases), len(test_cases))


    def test_invalid_case_aliquot_query(self):
        self.assertTrue(server_is_running())

        limit = 10
        offset = 0
        query = api.Client._case_aliquot_query('INVALID_STUDY', offset, limit)
        pdc_data, test_data = self.get_paired_data(query)

        self.assertIsNone(pdc_data['data']['paginatedCasesSamplesAliquots'])
        self.assertIsNone(test_data['data']['paginatedCasesSamplesAliquots'])


    def test_file_aliquot_query(self):
        self.assertTrue(server_is_running())

        study_id = api_data.get_study_id(self.TEST_PDC_STUDY_ID)
        random.seed(0)
        for file_id in random.sample(list(api_data.index_study_file_ids[study_id]), 1):
            query = api.Client._file_aliquot_query(file_id)

            pdc_data, test_data = self.get_paired_data(query)
            pdc_data = pdc_data['data']['fileMetadata']
            test_data = test_data['data']['fileMetadata']

            self.assertEqual(len(pdc_data), 1)
            self.assertEqual(len(test_data), 1)
            pdc_ids = {aliquot['aliquot_id'] for aliquot in pdc_data[0]['aliquots']}
            test_ids = {aliquot['aliquot_id'] for aliquot in test_data[0]['aliquots']}
            self.assertEqual(pdc_ids, test_ids)


    def test_invalid_file_aliquot_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._file_aliquot_query('INVALID_FILE_ID')

        pdc_data, test_data = self.get_paired_data(query)
        pdc_data = pdc_data['data']['fileMetadata']
        test_data = test_data['data']['fileMetadata']
        self.assertIsNone(pdc_data)
        self.assertIsNone(test_data)


    def test_study_case_query(self):
        self.assertTrue(False)


    def test_invalid_study_case_query(self):
        self.assertTrue(False)


    def test_study_file_id_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._study_file_id_query(api_data.get_study_id(self.TEST_PDC_STUDY_ID))
        pdc_response = self.post(PDC_URL, query)
        test_response = self.post(TEST_URL, query)

        # Ensure the response is 200 OK
        self.assertEqual(pdc_response.status_code, 200)
        self.assertEqual(test_response.status_code, 200)

        # Parse the response JSON
        pdc_data = pdc_response.json()
        test_data = test_response.json()

        self.assertEqual(len(pdc_data), len(test_data))

        # Assert the response data matches the expected data
        for file_id, file in pdc_data.items():
            self.assertIn(file_id, test_data)
            self.assertDictEqual(test_data[file_id], file)


    def test_invalid_study_file_id_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._study_raw_file_query('INVALID_STUDY_ID')

        pdc_response = self.post(PDC_URL, query)
        test_response = self.post(TEST_URL, query)

        # Ensure the response is 200 OK
        self.assertEqual(pdc_response.status_code, 200)
        self.assertEqual(test_response.status_code, 200)

        # Parse the response JSON
        pdc_data = pdc_response.json()
        test_data = test_response.json()
        self.assertIn('errors', pdc_data)
        self.assertIn('errors', test_data)
        self.assertIn('data', pdc_data)
        self.assertIn('data', test_data)
        self.assertDictEqual(pdc_data['data'], test_data['data'])


    def test_study_raw_file_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._study_raw_file_query(api_data.get_study_id(self.TEST_PDC_STUDY_ID))
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


    def test_invalid_study_raw_file_query(self):
        self.assertTrue(server_is_running())

        query = api.Client._study_raw_file_query('INVALID_STUDY_ID')

        pdc_response = self.post(PDC_URL, query)
        test_response = self.post(TEST_URL, query)

        # Ensure the response is 200 OK
        self.assertEqual(pdc_response.status_code, 200)
        self.assertEqual(test_response.status_code, 200)

        # Parse the response JSON
        pdc_data = pdc_response.json()
        test_data = test_response.json()
        self.assertIn('data', pdc_data)
        self.assertIn('data', test_data)
        self.assertIn('errors', pdc_data)
        self.assertIn('errors', test_data)
        self.assertDictEqual(pdc_data['data'], test_data['data'])


class TestClient(TestGraphQLServerBase):
    TEST_PDC_STUDY_ID = 'PDC000504'
    # TEST_PDC_STUDY_ID = 'PDC000110'

    def get_data_pair(self, function_name, *args, **kwargs):
        with api.Client(url=PDC_URL) as client:
            pdc_data = getattr(client, function_name)(*args, **kwargs)
        with api.Client(url=TEST_URL) as client:
            test_data = getattr(client, function_name)(*args, **kwargs)

        return pdc_data, test_data


    def do_single_comparison_test(self, function_name, *args, comparison_f, **kwargs):
        pdc_data, test_data = self.get_data_pair(function_name, *args, **kwargs)
        comparison_f(test_data, pdc_data)


    def do_invalid_test(self, function_name, *args, **kwargs):
        pdc_data, test_data = self.get_data_pair(function_name, *args, **kwargs)
        self.assertIsNone(test_data)
        self.assertIsNone(pdc_data)


    def test_coverage(self):
        client_methods = [a for a in dir(api.Client)
                          if callable(getattr(api.Client, a)) and re.search(r'^get_[a-z_]+$', a)]

        test_methods = [a for a in dir(self)
                        if callable(getattr(self, a)) and re.search(r'^test_[a-z_]+$', a)]

        for method in client_methods:
            self.assertIn(f'test_{method}', test_methods)


    def test_get_study_id(self):
        self.do_single_comparison_test('get_study_id', self.TEST_PDC_STUDY_ID,
                                       comparison_f=self.assertEqual)
        self.do_invalid_test('get_study_id', 'INVALID_STUDY_ID')

    def test_get_study_name(self):
        self.do_single_comparison_test('get_study_name', api_data.get_study_id(self.TEST_PDC_STUDY_ID),
                                       comparison_f=self.assertEqual)
        self.do_invalid_test('get_study_name', 'INVALID_STUDY_ID')


    def test_get_pdc_study_id(self):
        self.do_single_comparison_test('get_pdc_study_id', study_id=api_data.get_study_id(self.TEST_PDC_STUDY_ID),
                                       comparison_f=self.assertEqual)
        self.do_invalid_test('get_pdc_study_id', 'INVALID_STUDY_ID')


    def test_get_study_metadata(self):
        self.do_single_comparison_test('get_study_metadata', study_id=api_data.get_study_id(self.TEST_PDC_STUDY_ID),
                                       comparison_f=self.assertDictEqual)
        self.do_invalid_test('get_study_metadata', 'INVALID_STUDY_ID')


    def test_get_study_raw_files(self):
        pdc_data, test_data = self.get_data_pair('get_study_raw_files',
                                                 study_id=api_data.get_study_id(self.TEST_PDC_STUDY_ID))

        self.assertEqual(len(pdc_data), len(test_data))
        pdc_data = data_list_to_dict(pdc_data, 'file_id')
        test_data = data_list_to_dict(test_data, 'file_id')

        for file_id, file in pdc_data.items():
            self.assertIn('url', file)
            self.assertIn('url', test_data[file_id])
            file['url'] = ''
            test_data[file_id]['url'] = ''
            self.assertIn(file_id, test_data)
            self.assertDictEqual(test_data[file_id], file)


    def test_get_study_catalog(self):
        self.assertTrue(False)


    def test_get_study_aliquots(self):
        pdc_data, test_data = self.get_data_pair('get_study_aliquots',
                                                 api_data.get_study_id(self.TEST_PDC_STUDY_ID),
                                                 page_limit=100)

        self.assertEqual(len(pdc_data), len(test_data))
        pdc_data = data_list_to_dict(pdc_data, 'aliquot_id')
        test_data = data_list_to_dict(test_data, 'aliquot_id')

        for aliquot_id, aliquot in pdc_data.items():
            self.assertIn(aliquot_id, test_data)
            self.assertDictEqual(test_data[aliquot_id], aliquot)


    def test_get_study_cases(self):
        self.assertTrue(False)
