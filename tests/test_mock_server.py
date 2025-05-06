
import unittest
import random
import subprocess
import os
from signal import SIGTERM
import re
import time
import httpx
from copy import deepcopy

from resources import TEST_DIR
from resources.setup_functions import make_work_dir
from resources.mock_graphql_server.data import api_data
from resources.mock_graphql_server.server import server_is_running

from PDC_client.submodules import api

TEST_URL = 'http://127.0.0.1:5000/graphql'
PDC_URL = api.BASE_URL


def data_list_to_dict(data_list, key):
    ret = dict()
    for data in data_list.copy():
        ret[data[key]] = data

    return ret


class TestGraphQLServerBase(unittest.TestCase):
    # TEST_PDC_STUDY_ID = 'PDC000504'
    # TEST_PDC_STUDY_ID = 'PDC000251'
    TEST_PDC_STUDY_ID = 'PDC000451'

    @classmethod
    def setUpClass(cls):
        '''Set up the test server before running tests.'''

        # return if server is already running
        if server_is_running(url=TEST_URL):
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
            if server_is_running(url=TEST_URL):
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
    '''
    Test the raw graphQL request used by Client match the result of the test server.
    '''

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
        '''
        Check that all query functions in Client are covered by a test.

        Test that all query functions in Client matching the pattern _[a-z_]+_query
        have a corresponding test function in this class.
        '''
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
        query = api.Client._study_catalog_query(self.TEST_PDC_STUDY_ID)
        self.do_comparison_test(query)


    def test_invalid_study_catalog_query(self):
        query = api.Client._study_catalog_query('INVALID_STUDY_ID')
        self.do_comparison_test(query)


    def test_study_metadata_query(self):
        query = api.Client._study_metadata_query('pdc_study_id', self.TEST_PDC_STUDY_ID)
        self.do_comparison_test(query)


    def test_invalid_study_metadata_query(self):
        query = api.Client._study_metadata_query('pdc_study_id', 'INVALID_STUDY_ID')
        self.do_comparison_test(query)


    def test_case_aliquot_query(self):
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
        limit = 10
        offset = 0
        query = api.Client._case_aliquot_query('INVALID_STUDY', offset, limit)
        pdc_data, test_data = self.get_paired_data(query)

        self.assertIsNone(pdc_data['data']['paginatedCasesSamplesAliquots'])
        self.assertIsNone(test_data['data']['paginatedCasesSamplesAliquots'])


    def test_file_aliquot_query(self):
        study_id = api_data.get_study_id(self.TEST_PDC_STUDY_ID)
        random.seed(-1)
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
        query = api.Client._file_aliquot_query('INVALID_FILE_ID')

        pdc_data, test_data = self.get_paired_data(query)
        pdc_data = pdc_data['data']['fileMetadata']
        test_data = test_data['data']['fileMetadata']
        self.assertIsNone(pdc_data)
        self.assertIsNone(test_data)


    def test_study_case_query(self):
        study_id = api_data.get_study_id(self.TEST_PDC_STUDY_ID)
        limit = 10
        offset = 0

        query = api.Client._study_case_query(study_id, offset, limit)

        pdc_data, test_data = self.get_paired_data(query)
        pdc_data = pdc_data['data']['paginatedCaseDemographicsPerStudy']
        test_data = test_data['data']['paginatedCaseDemographicsPerStudy']

        self.assertEqual(pdc_data['total'], test_data['total'])
        self.assertDictEqual(pdc_data['pagination'], test_data['pagination'])

        pdc_cases = {case['case_id']: case['demographics'] for case in pdc_data['caseDemographicsPerStudy']}
        test_cases = {case['case_id']: case['demographics'] for case in test_data['caseDemographicsPerStudy']}
        self.assertEqual(len(pdc_cases), len(test_cases))


    def test_invalid_study_case_query(self):
        limit = 10
        offset = 0
        query = api.Client._study_case_query('INVALID_STUDY', offset, limit)
        pdc_data, test_data = self.get_paired_data(query)

        self.assertIsNone(pdc_data['data']['paginatedCaseDemographicsPerStudy'])
        self.assertIsNone(test_data['data']['paginatedCaseDemographicsPerStudy'])


    def test_study_file_id_query(self):
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


    def test_experimental_metadata_query(self):
        def parse_data(data):
            _data = deepcopy(data)
            self.assertIn('data', _data)
            self.assertIn('experimentalMetadata', _data['data'])
            self.assertEqual(len(_data['data']['experimentalMetadata']), 1)
            _data = _data['data']['experimentalMetadata'][0]['study_run_metadata']
            ret = dict()
            for run in _data:
                if 'aliquot_run_metadata' not in run:
                    print(run)
                study_run_metadata_id = run.pop('study_run_metadata_id')
                ret[study_run_metadata_id] = run

            return ret

        study_id = api_data.get_study_id(self.TEST_PDC_STUDY_ID)
        study_submitter_id = api_data.studies[study_id]['study_submitter_id']
        query = api.Client._experimental_metadata_query(study_submitter_id)

        pdc_data, test_data = self.get_paired_data(query)
        self.assertNotIn('errors', test_data, msg=test_data.get('errors'))

        pdc_data = parse_data(pdc_data)
        test_data = parse_data(test_data)

        self.assertEqual(len(pdc_data), len(test_data))

        for srm_id, run in pdc_data.items():
            self.assertIn(srm_id, test_data)
            test_run = test_data[srm_id]

            self.assertEqual(run.get('study_run_metadata_submitter_id'),
                             test_run.get('study_run_metadata_submitter_id'))
            self.assertEqual(len(run['aliquot_run_metadata']),
                             len(test_run['aliquot_run_metadata']))
            pdc_aliquots = {a['aliquot_run_metadata_id']: a['aliquot_id'] for a in run['aliquot_run_metadata']}
            test_aliquots = {a['aliquot_run_metadata_id']: a['aliquot_id'] for a in test_run['aliquot_run_metadata']}
            self.assertDictEqual(pdc_aliquots, test_aliquots)


    def test_invalid_experimental_metadata_query(self):
        query = api.Client._experimental_metadata_query('INVALID_STUDY_ID')

        pdc_response = self.get(PDC_URL, query)
        test_response = self.get(TEST_URL, query)
        self.assertEqual(pdc_response.status_code, 200)
        self.assertEqual(test_response.status_code, 200)
        self.assertEqual(len(pdc_response.json()['data']['experimentalMetadata']), 0)
        self.assertEqual(len(test_response.json()['data']['experimentalMetadata']), 0)


    def test_file_metadata_query(self):
        random.seed(12)
        test_files = {i: api_data.file_metadata[i] for i in
                      random.sample(list(api_data.file_metadata.keys()), 3)}

        query_keys = ['file_name', 'file_type', 'data_category',
                      'file_format', 'md5sum', 'file_size']

        for file_id, file_data in test_files.items():
            query = api.Client._file_metadata_query(file_id)
            pdc_response = self.get(PDC_URL, query)
            test_response = self.get(TEST_URL, query)
            self.assertEqual(pdc_response.status_code, 200)
            self.assertEqual(test_response.status_code, 200)

            # Parse the response
            pdc_data = pdc_response.json()['data']['fileMetadata']
            test_data = test_response.json()['data']['fileMetadata']
            self.assertEqual(len(pdc_data), 1)
            self.assertEqual(len(test_data), 1)
            pdc_data = pdc_data[0]
            test_data = test_data[0]

            for key in query_keys:
                self.assertIn(key, pdc_data)
                self.assertIn(key, test_data)
                self.assertEqual(pdc_data[key], test_data[key])


    def test_invalid_file_metadata_query(self):
        query = api.Client._file_metadata_query('INVALID_FILE_ID')

        pdc_response = self.get(PDC_URL, query)
        test_response = self.get(TEST_URL, query)
        self.assertEqual(pdc_response.status_code, 200)
        self.assertEqual(test_response.status_code, 200)
        self.assertIsNone(pdc_response.json()['data']['fileMetadata'])
        self.assertIsNone(test_response.json()['data']['fileMetadata'])


    def test_file_url_query(self):
        random.seed(12)
        test_files = {i: api_data.file_metadata[i] for i in
                      random.sample(list(api_data.file_metadata.keys()), 3)}

        query_keys = ['file_name', 'file_type', 'data_category', 'file_format']

        for file_id, file_data in test_files.items():
            query = api.Client._file_url_query(*[file_data[key] for key in query_keys])
            pdc_response = self.post(PDC_URL, query)
            test_response = self.post(TEST_URL, query)
            self.assertEqual(pdc_response.status_code, 200)
            self.assertEqual(test_response.status_code, 200)
            pdc_data = pdc_response.json()['data']['filesPerStudy']
            test_data = test_response.json()['data']['filesPerStudy']
            self.assertIsNotNone(pdc_data)
            self.assertIsNotNone(test_data)
            self.assertEqual(len(pdc_data), 1)
            self.assertEqual(len(test_data), 1)
            pdc_data = pdc_data[0]
            test_data = test_data[0]

            self.assertEqual(file_id, pdc_data['file_id'])
            for key in ('file_id', 'md5sum', 'file_size'):
                self.assertIn(key, pdc_data)
                self.assertIn(key, test_data)
                self.assertEqual(pdc_data[key], test_data[key])


    def test_invalid_file_url_query(self):
        query = api.Client._file_url_query('NA', 'NA', 'NA', 'NA')

        pdc_response = self.post(PDC_URL, query)
        test_response = self.post(TEST_URL, query)
        self.assertEqual(pdc_response.status_code, 200)
        self.assertEqual(test_response.status_code, 200)
        self.assertEqual(len(pdc_response.json()['data']['filesPerStudy']), 0)
        self.assertEqual(len(pdc_response.json()['data']['filesPerStudy']), 0)


class TestClient(TestGraphQLServerBase):
    '''
    Test the client functions that call the API.
    '''

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
        '''
        Check that all functions in Client that call the API are covered by a test.

        Test that all functions in Client matching the pattern 'get_[a-z_]+'
        have a corresponding test function in this class.
        '''
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
        pdc_data, test_data = self.get_data_pair('get_study_catalog', self.TEST_PDC_STUDY_ID)

        self.assertIn('versions', pdc_data)
        self.assertIn('versions', test_data)
        pdc_data = pdc_data['versions']
        test_data = test_data['versions']
        self.assertEqual(len(pdc_data), len(test_data))

        pdc_data = data_list_to_dict(pdc_data, 'study_id')
        test_data = data_list_to_dict(test_data, 'study_id')

        for study_id, version in pdc_data.items():
            self.assertIn(study_id, test_data)
            self.assertDictEqual(test_data[study_id], version)


    def test_get_study_samples(self):
        study_id = api_data.get_study_id(self.TEST_PDC_STUDY_ID)
        pdc_data, test_data = self.get_data_pair('get_study_samples', study_id, page_limit=50)

        self.assertEqual(len(pdc_data), len(test_data))
        pdc_data = data_list_to_dict(pdc_data, 'aliquot_id')
        test_data = data_list_to_dict(test_data, 'aliquot_id')

        for aliquot_id, aliquot in pdc_data.items():
            self.assertIn(aliquot_id, test_data)
            self.assertDictEqual(test_data[aliquot_id], aliquot)


    def test_get_study_cases(self):
        study_id = api_data.get_study_id(self.TEST_PDC_STUDY_ID)
        pdc_data, test_data = self.get_data_pair('get_study_cases', study_id, page_limit=50)

        self.assertEqual(len(pdc_data), len(test_data))
        pdc_data = data_list_to_dict(pdc_data, 'case_id')
        test_data = data_list_to_dict(test_data, 'case_id')

        for case_id, case in pdc_data.items():
            self.assertIn(case_id, test_data)
            self.assertDictEqual(test_data[case_id], case)


    def test_get_file_url(self):
        random.seed(7)
        test_files = {i: api_data.file_metadata[i] for i in
                      random.sample(list(api_data.file_metadata.keys()), 3)}

        for file_id, file_data in test_files.items():
            pdc_data, test_data = self.get_data_pair('get_file_url', file_id)
            self.assertIn('url', pdc_data)
            self.assertIn('url', test_data)
            pdc_data['url'] = ''
            test_data['url'] = ''
            self.assertDictEqual(pdc_data, test_data)