
import os
import unittest
import random
from csv import DictReader
from abc import ABC, abstractmethod

from resources import TEST_DIR, setup_functions
from resources.mock_graphql_server.data import api_data
from resources.data import PDC_TEST_FILE_IDS, TEST_URLS

from PDC_client.submodules.io import is_dia, md5_sum
from PDC_client.submodules.api import Client


TEST_PDC_STUDY_ID = 'PDC000504'
TEST_URL = 'https://pdc.cancer.gov/graphql'
# TEST_URL = 'http://127.0.0.1:5000/graphql'


class TestStudySubcommands(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.work_dir = f'{TEST_DIR}/work/study_subcommands'
        setup_functions.make_work_dir(cls.work_dir, clear_dir=True)


    def test_study_id(self):
        target = api_data.get_study_id(TEST_PDC_STUDY_ID)

        args = ['PDC_client', 'studyID', '-u', TEST_URL, TEST_PDC_STUDY_ID]
        result = setup_functions.run_command(args, self.work_dir, prefix='study_id')

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), target)


    def test_study_name(self):
        study_id = api_data.get_study_id(TEST_PDC_STUDY_ID)
        target = api_data.studies[study_id]['study_name']

        args = ['PDC_client', 'studyName', '-u', TEST_URL, study_id]
        result = setup_functions.run_command(args, self.work_dir, prefix='study_name')

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), target)


    def test_pdc_study_id(self):
        study_id = api_data.get_study_id(TEST_PDC_STUDY_ID)
        target = TEST_PDC_STUDY_ID

        args = ['PDC_client', 'PDCStudyID', '-u', TEST_URL, study_id]
        result = setup_functions.run_command(args, self.work_dir, prefix='pdc_study_id')

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), target)


class SkylineAnnotationsTestBase(ABC):
    SKYLINE_ANNOTATIONS_PDC_STUDY_ID = 'PDC000504'
    TARGET_SKYLINE_ANNOTATIONS = f'{TEST_DIR}/resources/data/output/{SKYLINE_ANNOTATIONS_PDC_STUDY_ID}_skyline_annotations.csv'

    @abstractmethod
    def assertEqual(self, lhs, rhs):
        pass

    @abstractmethod
    def assertIn(self, lhs, rhs):
        pass

    def _format_dict(self, data):
        ret = {}
        for row in data:
            if 'ElementLocator' not in row:
                raise AssertionError("'ElementLocator' not found in: %s" % row)
            ret[row['ElementLocator']] = {k: v for k, v in row.items() if k != 'ElementLocator'}

        return ret


    def assertSkylineAnnotationsEqual(self, rhs_fname, lhs_fname):
        ''' Compare two Skyline annotations files '''

        with open(rhs_fname, 'r') as inF:
            rhs_data = list(DictReader(inF, delimiter=','))
        with open(lhs_fname, 'r') as inF:
            lhs_data = list(DictReader(inF, delimiter=','))

        self.assertEqual(len(rhs_data), len(lhs_data))
        rhs_data = self._format_dict(rhs_data)
        lhs_data = self._format_dict(lhs_data)

        for replicate, data in lhs_data.items():
            self.assertIn(replicate, rhs_data)
            self.assertEqual(set(rhs_data[replicate].keys()), set(data.keys()))
            for key in data:
                self.assertEqual(rhs_data[replicate][key], data[key])


class TestMetadataSubcommand(unittest.TestCase, SkylineAnnotationsTestBase):
    @classmethod
    def setUpClass(cls):
        cls.work_dir = f'{TEST_DIR}/work/metadata_subcommand'
        setup_functions.make_work_dir(cls.work_dir, clear_dir=True)


    def get_test_study(self, dda=False, seed=1):
        ''' Chose random study to test '''
        dia_studies = [study['pdc_study_id'] for study in api_data.studies.values()
                       if (not dda) and is_dia(study)]
        self.assertGreaterEqual(len(dia_studies), 1, 'No DIA studies found in mock data')
        random.seed(seed)
        return random.choice(dia_studies)


    def test_default(self):
        study_id = api_data.get_study_id(TEST_PDC_STUDY_ID)

        args = ['PDC_client', 'metadata', '-u', TEST_URL, study_id]
        result = setup_functions.run_command(args, self.work_dir, prefix='test_default')

        target_files = [f'{TEST_PDC_STUDY_ID}_{file}.json'
                        for file in ('study_metadata', 'files', 'aliquots', 'cases')]

        self.assertEqual(result.returncode, 0)
        self.assertTrue(all(os.path.exists(f'{self.work_dir}/{f}') for f in target_files))
        self.assertTrue(all(os.path.getsize(f'{self.work_dir}/{f}') > 0 for f in target_files))


    def test_flatten(self):
        pdc_study_id = self.get_test_study(dda=False, seed=40)
        study_id = api_data.get_study_id(pdc_study_id)

        prefix = f'{pdc_study_id}_flatten_test_'
        args = ['PDC_client', 'metadata', f'--prefix={prefix}', '--flatten',
                '-u', TEST_URL, study_id]
        result = setup_functions.run_command(args, self.work_dir, prefix='default')

        self.assertEqual(result.returncode, 0, result.stderr)
        target_file = f'{prefix}flat.json'
        self.assertTrue(os.path.exists(f'{self.work_dir}/{target_file}'),
                        f"target_file '{target_file}' not found in {self.work_dir}")
        self.assertTrue(os.path.getsize(f'{self.work_dir}/{target_file}'))


    def test_flatten_tsv(self):
        pdc_study_id = self.get_test_study(dda=False, seed=40)
        study_id = api_data.get_study_id(pdc_study_id)

        prefix = f'{pdc_study_id}_flatten_tsv_test_'
        args = ['PDC_client', 'metadata', f'--prefix={prefix}',
                '--flatten', '--format=tsv',
                '-u', TEST_URL, study_id]
        result = setup_functions.run_command(args, self.work_dir, prefix='default')

        self.assertEqual(result.returncode, 0, result.stderr)
        target_file = f'{prefix}flat.tsv'
        self.assertTrue(os.path.exists(f'{self.work_dir}/{target_file}'),
                        f"target_file '{target_file}' not found in {self.work_dir}")
        self.assertTrue(os.path.getsize(f'{self.work_dir}/{target_file}'))


    def test_invalid_study_id(self):
        study_id = 'INVALID_STUDY_ID'
        args = ['PDC_client', 'metadata', '-u', TEST_URL, study_id]
        result = setup_functions.run_command(command=args, wd=self.work_dir,
                                             prefix='invalid_study_id')
        self.assertEqual(result.returncode, 1)
        self.assertIn(f'Could not retrieve metadata for study: {study_id}', result.stderr)


    def test_skyline_annotations(self):
        test_pdc_study_id = self.SKYLINE_ANNOTATIONS_PDC_STUDY_ID
        study_id = api_data.get_study_id(test_pdc_study_id)
        args = ['PDC_client', 'metadata', '-u', TEST_URL,
                '--flatten', '--skylineAnnotations', study_id]
        result = setup_functions.run_command(command=args, wd=self.work_dir,
                                             prefix='test_skyline_annotations')

        self.assertEqual(result.returncode, 0, result.stderr)
        target_file = f'{test_pdc_study_id}_flat.json'
        self.assertTrue(os.path.exists(f'{self.work_dir}/{target_file}'),
                        f"target_file '{target_file}' not found in {self.work_dir}")
        self.assertTrue(os.path.getsize(f'{self.work_dir}/{target_file}'))

        test_annotations_file = f'{self.work_dir}/{test_pdc_study_id}_skyline_annotations.csv'
        self.assertTrue(os.path.exists(test_annotations_file),
                        f"target_file '{test_annotations_file}' not found in {self.work_dir}")
        self.assertEqual(os.path.getsize(test_annotations_file),
                         os.path.getsize(self.TARGET_SKYLINE_ANNOTATIONS))
        self.assertSkylineAnnotationsEqual(test_annotations_file, self.TARGET_SKYLINE_ANNOTATIONS)


class TestMetadataToSky(unittest.TestCase, SkylineAnnotationsTestBase):
    @classmethod
    def setUpClass(cls):
        cls.work_dir = f'{TEST_DIR}/work/metadataToSky_subcommand'
        cls.test_metadata_json = f'{TEST_DIR}/resources/data/output/{cls.SKYLINE_ANNOTATIONS_PDC_STUDY_ID}_flat.json'
        cls.test_metadata_tsv = f'{TEST_DIR}/resources/data/output/{cls.SKYLINE_ANNOTATIONS_PDC_STUDY_ID}_flat.tsv'
        setup_functions.make_work_dir(cls.work_dir, clear_dir=True)


    def test_metadataToSky(self):
        args = ['PDC_client', 'metadataToSky', self.test_metadata_json]
        result = setup_functions.run_command(command=args, wd=self.work_dir,
                                             prefix='test_metadataToSky')

        self.assertEqual(result.returncode, 0, result.stderr)
        test_annotations_file = f'{self.work_dir}/skyline_annotations.csv'
        self.assertTrue(os.path.exists(test_annotations_file),
                        f"target_file '{test_annotations_file}' not found in {self.work_dir}")
        self.assertEqual(os.path.getsize(test_annotations_file),
                         os.path.getsize(self.TARGET_SKYLINE_ANNOTATIONS))
        self.assertSkylineAnnotationsEqual(test_annotations_file, self.TARGET_SKYLINE_ANNOTATIONS)


    def test_tsv_metadataToSky(self):
        args = ['PDC_client', 'metadataToSky', self.test_metadata_tsv]
        result = setup_functions.run_command(command=args, wd=self.work_dir,
                                             prefix='test_metadataToSky')

        self.assertEqual(result.returncode, 0, result.stderr)
        test_annotations_file = f'{self.work_dir}/skyline_annotations.csv'
        self.assertTrue(os.path.exists(test_annotations_file),
                        f"target_file '{test_annotations_file}' not found in {self.work_dir}")
        self.assertEqual(os.path.getsize(test_annotations_file),
                         os.path.getsize(self.TARGET_SKYLINE_ANNOTATIONS))
        self.assertSkylineAnnotationsEqual(test_annotations_file, self.TARGET_SKYLINE_ANNOTATIONS)


class TestFileSubcommands(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.work_dir = f'{TEST_DIR}/work/file_subcommands'
        setup_functions.make_work_dir(cls.work_dir, clear_dir=True)
        cls.test_file_id_index = 0

        cls.mock_server_active = TEST_URL.startswith('http://127.0.0.1')

        cls.pdc_file_url = None
        if not cls.mock_server_active:
            with Client() as client:
                cls.pdc_file_url = client.get_file_url(PDC_TEST_FILE_IDS[cls.test_file_id_index]['file_id'])


    def test_mutually_exclusive_args(self):
        args = ['PDC_client', 'file', '--fileID', 'file_id', '--url', 'url']
        result = setup_functions.run_command(args, self.work_dir, prefix='test_invalid_args')
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn('argument --url: not allowed with argument --fileID', result.stderr)


    def test_file_id(self):
        if self.mock_server_active:
            self.skipTest('Not implemented for mock server')
        self.assertIsNotNone(self.pdc_file_url, 'Test file data lookup in setup failed!')

        ofname = f'{self.work_dir}/test_pdc_file_id_download.txt'
        args = ['PDC_client', 'file', f'--ofname={ofname}',
               '--baseUrl', TEST_URL, '--fileID',
               PDC_TEST_FILE_IDS[self.test_file_id_index]['file_id']]
        result = setup_functions.run_command(args, self.work_dir, prefix='file_id')

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(os.path.isfile(ofname), True, f'{ofname} does not exist')


    def test_pdc_url(self):
        if self.mock_server_active:
            self.skipTest('Not implemented for mock server')
        self.assertIsNotNone(self.pdc_file_url, 'Test file URL lookup in setup failed!')

        ofname = f'{self.work_dir}/test_pdc_url_download.txt'
        args = ['PDC_client', 'file', f'--ofname={ofname}',
                f'--md5sum={self.pdc_file_url["md5sum"]}', f'--size={self.pdc_file_url["file_size"]}',
                '--url', self.pdc_file_url['url']]
        result = setup_functions.run_command(args, self.work_dir, prefix='url')

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(os.path.isfile(ofname), True, f'{ofname} does not exist')
        self.assertEqual(os.path.getsize(ofname), int(self.pdc_file_url['file_size']))
        self.assertEqual(md5_sum(ofname), self.pdc_file_url['md5sum'])


    def test_url(self):
        for file in TEST_URLS:
            ofname = f'{self.work_dir}/test_url_download.txt'
            args = ['PDC_client', 'file', f'--ofname={ofname}',
                    '--md5sum', file['md5sum'], '--size', str(file['file_size']), '--url', file['url']]
            result = setup_functions.run_command(args, self.work_dir, prefix='test_url')

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(os.path.isfile(ofname), True, f'{ofname} does not exist')
            self.assertEqual(os.path.getsize(ofname), int(file['file_size']))
            self.assertEqual(md5_sum(ofname), file['md5sum'])
