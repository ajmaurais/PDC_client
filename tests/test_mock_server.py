
import unittest
import subprocess
import os
from signal import SIGTERM
import json
import time
import httpx

# from requests.exceptions import ConnectionError

import setup_tests

URL = 'http://localhost:5000/graphql'

def server_is_running():
    ''' Check if mock graphql server is running.'''
    query = 'query={ __schema { queryType { name }}}'
    try:
        response = httpx.get(f'{URL}?{query}')
        return response.status_code == 200
    except httpx.ConnectError:
        return False


class TestGraphQLServer(unittest.TestCase):
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


    def test_query_user(self):
        self.assertTrue(server_is_running())

        '''Test querying the user endpoint in GraphQL.'''
        query = ''' query {
                user(id: "123") { id name email }
            } '''

        response = httpx.post(URL, json={'query': query})

        # Ensure the response is 200 OK
        self.assertEqual(response.status_code, 200)

        # Parse the response JSON
        data = response.json()

        # Expected result
        expected_data = {
            'data': {
                'user': {
                    'id': '123',
                    'name': 'John Doe',
                    'email': 'john@example.com'
                }
            }
        }

        # Assert the response data matches the expected data
        self.assertDictEqual(data, expected_data)

if __name__ == '__main__':
    unittest.main()
