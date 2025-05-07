
import json
import re
from typing import Generator

from .logger import LOGGER

from ..data import STUDY_METADATA, STUDY_CATALOG, FILE_METADATA
from ..data import EXPERIMENT_METADATA, SAMPLE_METADATA, CASE_METADATA
from ..data import MISSING_SRM_IDS


def split_ms_file_extension(file_path):
    match = re.search(r'^(.+?)(\.(?:raw|d\.zip|mzML))?$', file_path)
    if match:
        file_name = match.group(1)
        file_extension = match.group(2) if match.group(2) else ''
        return file_name, file_extension
    else:
        raise ValueError(f"Invalid file path format: {file_path}")


class Data:
    def __init__(self):
        # read study metadata
        with open(STUDY_METADATA, 'r', encoding='utf-8') as inF:
            study_data = json.load(inF)
        self.studies = {study['study_id']: study for study in study_data}

        # read study catalog
        with open(STUDY_CATALOG, 'r', encoding='utf-8') as inF:
            self.study_catalog = json.load(inF)
            for study in self.study_catalog.values():
                for version in study['versions']:
                    version['is_latest_version'] = 'yes' if version['is_latest_version'] else 'no'

        # read file metadata
        with open(FILE_METADATA, 'r', encoding='utf-8') as inF:
            file_per_study = json.load(inF)

        # rearange study_id and pdc_study_id keys
        self.files_per_study = dict()
        for pdc_study_id, files in file_per_study.items():
            study_id = self.get_study_id(pdc_study_id)
            self.files_per_study[study_id] = list()
            for file in files:
                file['pdc_study_id'] = pdc_study_id
                self.files_per_study[study_id].append(file)

        # read experimental metadata
        with open(EXPERIMENT_METADATA, 'r', encoding='utf-8') as inF:
            experiment_data = json.load(inF)
        self.experiments = dict()
        for pdc_study_id, experiments in experiment_data.items():
            study_id = self.get_study_id(pdc_study_id)
            self.experiments[study_id] = experiments

        self.file_metadata = dict()
        self.index_study_file_ids = dict()
        file_metadata_keys = ['file_name', 'file_type', 'file_format', 'data_category', 'md5sum', 'file_size']
        for study_id, files in self.files_per_study.items():
            self.index_study_file_ids[study_id] = list()
            for file in files:
                self.file_metadata[file['file_id']] = {key: file[key] for key in file_metadata_keys}
                self.file_metadata[file['file_id']]['aliquots'] = list()
                self.file_metadata[file['file_id']]['study_run_metadata_id'] = self.get_study_run_metadata_id(file['file_name'])
                self.index_study_file_ids[study_id].append(file['file_id'])

        # read aliquot data
        with open(SAMPLE_METADATA, 'r', encoding='utf-8') as inF:
            aliquot_data = json.load(inF)

        self.index_study_cases = dict()
        self.cases = dict()
        for pdc_study_id, aliquots in aliquot_data.items():
            study_id = self.get_study_id(pdc_study_id)
            self.index_study_cases[study_id] = set()

            for aliquot in aliquots:
                self.index_study_cases[study_id].add(aliquot['case_id'])

                for file_id in aliquot['file_id_to_aliquot_run_metadata_id']:
                    if file_id not in self.file_metadata:
                        raise RuntimeError(f"Missing file metadata for file_id: '{file_id}'")
                    self.file_metadata[file_id]['aliquots'].append({'aliquot_id': aliquot['aliquot_id']})

                case_id = aliquot['case_id']
                if case_id not in self.cases:
                    self.cases[case_id] = dict()
                    self.cases[case_id]['samples'] = dict()
                    self.cases[case_id]['case_id'] = case_id

                sample_id = aliquot['sample_id']
                if sample_id not in self.cases[case_id]['samples']:
                    self.cases[case_id]['samples'][sample_id] = dict()
                    self.cases[case_id]['samples'][sample_id]['aliquots'] = list()
                    self.cases[case_id]['samples'][sample_id]['sample_id'] = sample_id
                    self.cases[case_id]['samples'][sample_id]['sample_submitter_id'] = aliquot['sample_submitter_id']
                    self.cases[case_id]['samples'][sample_id]['sample_type'] = aliquot['sample_type']
                    self.cases[case_id]['samples'][sample_id]['tissue_type'] = aliquot['tissue_type']

                self.cases[case_id]['samples'][sample_id]['aliquots'].append({'aliquot_id': aliquot['aliquot_id'],
                                                                              'analyte_type': aliquot['analyte_type']})

        # convert index sets to lists
        self.index_study_cases = {k: list(v) for k, v in self.index_study_cases.items()}

        # read case demographics metadata
        with open(CASE_METADATA, 'r', encoding='utf-8') as inF:
            case_data = json.load(inF)

        # add case demographics to cases
        for pdc_study_id, cases in case_data.items():
            study_id = self.get_study_id(pdc_study_id)
            for case in cases:
                case_id = case['case_id']
                if case_id not in self.cases:
                    raise ValueError(f'Case {case_id} not found in aliquot data!')
                case.pop('case_id')
                self.cases[case_id]['demographics'] = case


    def get_study_id(self, pdc_study_id):
        '''
        Retrieve the study_id based on the provided pdc_study_id.

        Args:
            pdc_study_id (str): The PDC study ID.
        Returns:
            str: The study ID if found, otherwise None.
        '''
        for study in self.studies.values():
            if study['pdc_study_id'] == pdc_study_id:
                return study['study_id']
        return None


    def get_study_id_by_submitter_id(self, study_submitter_id):
        for study in self.studies.values():
            if study['study_submitter_id'] == study_submitter_id:
                return study['study_id']
        return None


    def get_studies(self, study_id=None, pdc_study_id=None) -> Generator[dict, None, None]:
        '''
        Retrieve study information based on study_id or pdc_study_id.

        Args:
            study_id (str, optional): The ID of the study to retrieve. Defaults to None.
            pdc_study_id (str, optional): The PDC study ID to retrieve. Defaults to None.

        Yields:
            dict: A dictionary containing the study information if found, otherwise None.
        '''
        if pdc_study_id is not None or study_id is not None:
            if study_id is None:
                study_id = self.get_study_id(pdc_study_id)
            if study_id in self.studies:
                yield self.studies[study_id]
        else:
            yield from self.studies.values()


    def get_files_per_study(self, study_id=None,
                            pdc_study_id=None,
                            data_category=None,
                            file_name=None,
                            file_type=None,
                            file_format=None) -> Generator[dict, None, None]:
        '''
        Retrieve file information based on study_id or pdc_study_id.

        Args:
            study_id (str, optional): The ID of the study to retrieve. Defaults to None.
            pdc_study_id (str, optional): The PDC study ID to retrieve. Defaults to None.
            data_category (str, optional): The data category to filter files. Defaults to None.
            file_name (str, optional): The file name to filter files. Defaults to None.
            file_type (str, optional): The file type to filter files. Defaults to None.
            file_format (str, optional): The file format to filter files. Defaults to None.

        Yields:
            dict: A dictionary containing the file information if found.
        '''
        _study_id = study_id if study_id is not None else self.get_study_id(pdc_study_id)

        for data_study_id, study in self.files_per_study.items():
            if _study_id is None or _study_id == data_study_id:
                for file in study:
                    match = data_category is None or file['data_category'] == data_category
                    match &= file_name is None or file['file_name'] == file_name
                    match &= file_type is None or file['file_type'] == file_type
                    match &= file_format is None or file['file_format'] == file_format
                    if match:
                        yield file


    def get_total_cases_per_study(self, study_id):
        return len(self.index_study_cases[study_id])


    def get_cases_per_study(self, study_id, offset):
        i = offset
        total = self.get_total_cases_per_study(study_id)
        while i < total:
            yield self.cases[self.index_study_cases[study_id][i]]
            i += 1


    def get_file_id(self, file_name):
        '''
        Get the file ID based on the file name.
        Args:
            file_name (str): The name of the file to retrieve the ID for.
        Returns:
            str: The file ID if found, otherwise None.
        '''
        file_base = split_ms_file_extension(file_name)[0]
        for file_id, file in self.file_metadata.items():
            it_file_base = split_ms_file_extension(file['file_name'])[0]
            if it_file_base == file_base:
                return file_id

        LOGGER.warning(f"File ID not found for file name: {file_name}")
        return None


    def _resolve_missing_srm_ids(self, file_name):
        if file_name in MISSING_SRM_IDS:
            return True, MISSING_SRM_IDS[file_name]['study_run_metadata_id']
        return False, None


    def get_study_run_metadata_id(self, file_name):
        '''
        Retrieve the study run metadata submitter ID based on the file ID.

        Args:
            file_name (str): The name of the file to retrieve the submitter ID for.

        Returns:
            str: The study run metadata submitter ID if found, otherwise None.
        '''
        query_file_base = split_ms_file_extension(file_name)[0]
        for study in self.experiments.values():
            for run in study:
                it_file_base = split_ms_file_extension(run['study_run_metadata_submitter_id'])[0]
                if it_file_base == query_file_base:
                    return run['study_run_metadata_id']

        found, srm_id = self._resolve_missing_srm_ids(file_name)
        if found:
            return srm_id

        LOGGER.warning(f"study_run_metadata_submitter_id not found for file name: {file_name}")
        return None


    def get_file_metadata(self, file_id=None, study_run_metadata_id=None):
        '''
        Retrieve file metadata based on file_id or study_run_metadata_id.
        Args:
            file_id (str, optional): The ID of the file to retrieve. Defaults to None.
            study_run_metadata_id (str, optional): The study run metadata ID to retrieve. Defaults to None.
        Returns:
            dict: A dictionary containing the file metadata if found, otherwise None.
        '''
        if file_id is not None:
            return self.file_metadata.get(file_id, None)
        elif study_run_metadata_id is not None:
            for file in self.file_metadata.values():
                if file['study_run_metadata_id'] == study_run_metadata_id:
                    return file
            return None
        else:
            raise ValueError('Both file_id or study_run_metadata_id cannot be None!')


api_data = Data()