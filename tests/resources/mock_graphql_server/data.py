
import json
from typing import Generator

from ..data import STUDY_METADATA, STUDY_CATALOG, FILE_METADATA, ALIQUOT_METADATA

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

        # read aliquot data
        with open(ALIQUOT_METADATA, 'r', encoding='utf-8') as inF:
            aliquot_data = json.load(inF)

        self.aliquots_per_file = dict()
        for pdc_study_id, aliquots in aliquot_data.items():
            for aliquot in aliquots:
                self.aliquots_per_file[aliquot['aliquot_id']] = aliquot


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
                            data_category=None) -> Generator[dict, None, None]:
        '''
        Retrieve file information based on study_id or pdc_study_id.

        Args:
            study_id (str, optional): The ID of the study to retrieve. Defaults to None.
            pdc_study_id (str, optional): The PDC study ID to retrieve. Defaults to None.
            data_category (str, optional): The data category to filter files. Defaults to None.

        Yields:
            dict: A dictionary containing the file information if found, otherwise None.
        '''
        parameter_count = sum(v is not None for v in (study_id, pdc_study_id))
        if parameter_count == 0:
            for study in self.files_per_study.values():
                for file in study:
                    if data_category is None or file['data_category'] == data_category:
                        yield file

        elif parameter_count == 1:
            if pdc_study_id is not None:
                study_id = self.get_study_id(pdc_study_id)
                if study_id is None:
                    return None

            study = self.files_per_study.get(study_id)
            if study is None:
                return None

            for file in study:
                if data_category is None or file['data_category'] == data_category:
                    yield file

        else:
            raise ValueError('Both study_id and pdc_study_id cannot be provided!')


    def get_cases_per_study(self, study_id, offset):
        pass


    def get_file_metadata(self, file_id):
        pass


api_data = Data()
# api_data = 'data'
