

import re
import asyncio
import sys
from typing import Callable

from httpx import Limits, AsyncClient

from .logger import LOGGER

CLIENT_TIMEOUT = 10
BASE_URL ='https://proteomic.datacommons.cancer.gov/graphql'

FILE_METADATA_KEYS = ['file_id', 'file_name', 'file_submitter_id', 'md5sum', 'file_size',
                    'data_category', 'file_type', 'file_format', 'url']

class Query():
    def __init__(self,
                 url: str = BASE_URL,
                 timeout: int = CLIENT_TIMEOUT,
                 verify: bool = True,
                 max_connections: int = 1,
                 max_keepalive_connections: int = 5,
                 keepalive_expiry: int = 5,
                 request_retries: int = 5):

        self.url = url
        self.request_retries = request_retries
        self.loop = asyncio.new_event_loop()

        self.client = AsyncClient(limits=Limits(max_connections=max_connections,
                                                max_keepalive_connections=max_keepalive_connections,
                                                keepalive_expiry=keepalive_expiry),
                                  timeout=timeout, verify=verify)


    def __del__(self):
        self.loop.run_until_complete(self.client.aclose())
        self.loop.close()


    async def _post(self, query: str) -> dict:
        query = re.sub(r'\s+', ' ', query.strip())
        for _ in range(self.request_retries):
            response = await self.client.post(self.url, json={'query': query})
            if response.status_code == 200:
                return response.json()
            # if response.status_code >= 400 and response.status_code < 500:
            #     break
        sys.stderr.write(f'url:\n"{self.url}?{query}"\n')
        raise RuntimeError(f'Failed with response code {response.status_code}!')


    async def _get(self, query) -> dict:
        query = re.sub(r'\s+', ' ', query.strip())
        for _ in range(self.request_retries):
            response = await self.client.get(f'{self.url}?{query}')
            if response.status_code == 200:
                return response.json()
            # if response.status_code >= 400 and response.status_code < 500:
            #     break
        sys.stderr.write(f'url:\n"{self.url}?{query}"\n')
        raise RuntimeError(f'Failed with response code {response.status_code}!')


    @staticmethod
    def _log_post_errors(errors: list):
        ''' Write _post errors to LOGGER '''

        LOGGER.error('API query failed with response(s):', stacklevel=2)
        for error in errors:
            LOGGER.error('\n\tCode: %s\n\tEndpoint: %s\n\tMessage: %s\n',
                        error['extensions']['code'],
                        ', '.join(error['path']),
                        error['message'],
                        stacklevel=2)


    async def async_get_study_id(self, pdc_study_id: str) -> str|None:
        '''
        Async version of get_study_id

        Parameters
        ----------
        pdc_study_id: str
            The PDC study ID.

        Returns
        -------
        study_id: str
            The study_id or None if no study_id could be found for pdc_study_id
        '''

        query = '''query={
            studyCatalog (pdc_study_id: "%s" acceptDUA: true){
                versions { study_id is_latest_version }
            }}''' % pdc_study_id

        data = await self._get(query)

        if len(data['data']['studyCatalog']) == 0:
            return None

        for version in data['data']['studyCatalog'][0]['versions']:
            if version['is_latest_version'] == 'yes':
                return version['study_id']
        return None


    def get_study_id(self, pdc_study_id: str) -> str|None:
        '''
        Get study_id for a pdc_study_id.

        Parameters
        ----------
        pdc_study_id: str
            The study ID.
        kwargs: dict
            Additional kwargs passed to async_get_study_id

        Returns
        -------
        study_id: str
            The study_id or None if no study_id could be found for pdc_study_id
        '''
        return self.loop.run_until_complete(self.async_get_study_id(pdc_study_id))


    async def async_get_study_metadata(self, pdc_study_id: str|None=None,
                                       study_id: str|None=None) -> list|None:
        '''
        Async version of get_study_metadata

        Parameters
        ----------
        pdc_study_id: str
            If None the study_id must be specified.
        study_id: str
            If None the pdc_study_id must be specified.

        Returns
        -------
        metadata: dict
            The metadata or None if no metadata could be found for study_id
        '''

        if study_id is not None:
            _id = study_id
            id_name = 'study_id'
            study_id_task = None
        elif pdc_study_id is not None:
            _id = pdc_study_id
            id_name = 'pdc_study_id'
            study_id_task = asyncio.create_task(
                    self.async_get_study_id(pdc_study_id)
                )
        else:
            raise ValueError('Both pdc_study_id and study_id cannot be None!')

        study_query = '''query={
                study (%s: "%s" acceptDUA: true) {
                    study_id
                    pdc_study_id
                    study_name
                    analytical_fraction
                    experiment_type
                    cases_count
                    aliquots_count
                }
            } ''' % (id_name, _id)

        data = await asyncio.create_task(self._get(study_query))

        if study_id_task is None:
            if data['data']['study'] is None:
                return None
            return data['data']['study'][0]

        study_id = await study_id_task

        if study_id is None:
            return None

        for study in data['data']['study']:
            if study['study_id'] == study_id:
                return study
        raise RuntimeError('Could not find latest study for pdc_study_id!')


    def get_study_metadata(self, pdc_study_id: str|None=None,
                           study_id: str|None=None, **kwargs) -> dict|None:
        '''
        Parameters
        ----------
        pdc_study_id: str
            If None the study_id must be specified.
        study_id: str
            If None the pdc_study_id must be specified.

        Returns
        -------
        metadata: dict
            The metadata or None if no metadata could be found for study_id
        '''
        return self.loop.run_until_complete(
            self.async_get_study_metadata(pdc_study_id=pdc_study_id, study_id=study_id)
            )


    def get_pdc_study_id(self, study_id: str) -> str:
        '''
        Get pdc_study_id for a study_id.

        Parameters
        ----------
        study_id: str
            The study ID.

        Returns
        -------
        pdc_study_id: str
        '''
        data = self.get_study_metadata(study_id=study_id)
        if data is not None:
            return data['pdc_study_id']
        return None


    def get_study_name(self, study_id: str) -> str:
        '''
        Get study_name for a study_id.

        Parameters
        ----------
        study_id: str
            The study ID.

        Returns
        -------
        study_name: str
        '''
        data = self.get_study_metadata(study_id=study_id)
        if data is not None:
            return data['study_name']
        return None


    async def _get_paginated_data(self,
                                  query_f: Callable[[str, str, int], str],
                                  data_name: str,
                                  study_id: str,
                                  page_limit: int=100) -> list:

        endpoint_name = f'paginated{data_name[0].upper()}{data_name[1:]}'

        data = list()
        query = query_f(study_id, 0, page_limit)
        first_page = await self._get(query)

        if first_page['data'][endpoint_name] is None:
            LOGGER.error("Invalid query for study_id: '%s'", study_id)
            return None

        total = first_page['data'][endpoint_name]['total']

        if total <= page_limit:
            return first_page['data'][endpoint_name][data_name]

        offset = page_limit
        page_tasks = list()
        async with asyncio.TaskGroup() as tg:
            while offset < total:
                page_tasks.append(
                    tg.create_task(self._get(query_f(study_id, offset, page_limit)))
                )
                offset += page_limit

        # Flatten pages into 1 list
        data = list()
        for page in [first_page] + [task.result() for task in page_tasks]:
            data += page['data'][endpoint_name][data_name]

        return data


    # async def async_get_file_metadata(file_id: str|None=None,
    #                                 study_id: str|None=None,
    #                                 client: AsyncClient|None=None,
    #                                 verify: bool=True,
    #                                 timeout: float=CLIENT_TIMEOUT,
    #                                 url: str=BASE_URL, **kwargs) -> dict:
    #     '''
    #     Async version of get_file_metadata.

    #     Parameters
    #     ----------
    #     file_id: str
    #         The file ID.
    #     client: httpx.AsyncClient
    #         The client to use for get requests.
    #         If None a client is instantiated in the function.
    #     verify: bool
    #         Should SSL verification be disabled for requests?
    #     timeout: float
    #         Request timeout.
    #     url: str
    #         The base URL to use for get requests.
    #     kwargs: dict
    #         Additional kwargs passed to _get

    #     Returns
    #     -------
    #     metadata: dict
    #         The metadata for the file or None if no metadata could be found for file_id.
    #     '''

    #     if study_id is not None:
    #         _id = study_id
    #         id_name = 'file_id'
    #     elif file_id is not None:
    #         _id = file_id
    #         id_name = 'file_id'
    #     else:
    #         raise ValueError('Both file_id and study_id cannot be None!')

    #     # query to get aliquot IDs associated with each file.
    #     query = '''query={
    #             fileMetadata (%s: "%s" acceptDUA: true) {
    #                 file_id aliquots { aliquot_id } }
    #                 }''' % (id_name, _id)


    # def get_file_metadata(file_id: str, **kwargs) -> dict:
    #     '''
    #     '''
    #     return asyncio.run(async_get_study_metadata(file_id, **kwargs))


    async def async_get_study_aliquots(self, study_id: str,
                                       file_ids: list|None=None,
                                       page_limit: int=100) -> list:
        '''
        Async version of get_study_aliquots.

        Parameters
        ----------
        study_id: str
            The study ID.
        file_ids: list
            A list of file IDs to retreive data for. If None, all the files in the study are used.
        page_limit: int
            Page limit passed to _get_paginated_data.

        Returns
        -------
        aliquots: list
            A list of aliquots in the study where each list element is metadata for an aliquot
            or None if no cases could be found for study_id.
        '''

        # query to get study, cases, samples, and aliquots
        def aliquot_query(study_id, offset, limit):
            return '''query={
                paginatedCasesSamplesAliquots (study_id: "%s" offset: %u limit: %u) {
                    total
                    casesSamplesAliquots {
                        case_id
                        samples { sample_id sample_submitter_id sample_type tissue_type
                            aliquots { aliquot_id analyte_type }
                        }
                    }
                    pagination { count from page total pages size }
                } }''' % (study_id, offset, limit)

        # query to get aliquot IDs associated with each file.
        def file_query(file_id):
            return '''query={
                fileMetadata (file_id: "%s" acceptDUA: true) {
                    file_id aliquots { aliquot_id } }
                    }''' % file_id

        # query to get all file_ids in study
        file_id_query = '''query {
            filesPerStudy (study_id: "%s", data_category: "Raw Mass Spectra" acceptDUA: true) {
                file_id }
            } ''' % study_id

        aliquot_task = asyncio.create_task(
                self._get_paginated_data(aliquot_query, 'casesSamplesAliquots', study_id,
                                         page_limit=page_limit)
            )

        if file_ids is None:
            file_id_data = await self._post(file_id_query)

            if 'errors' in file_id_data:
                self._log_post_errors(file_id_data['errors'])
                return None

            file_ids = [f['file_id'] for f in file_id_data['data']['filesPerStudy']]

        aliquot_id_tasks = list()
        async with asyncio.TaskGroup() as tg:
            for file_id in file_ids:
                aliquot_id_tasks.append(
                    tg.create_task(self._get(file_query(file_id)))
                )

        # construct dictionary of aliquot_ids maped to file_ids
        file_ids = dict()
        for task in aliquot_id_tasks:
            data = task.result()
            file_id = data['data']['fileMetadata'][0]['file_id']
            for aliquot in data['data']['fileMetadata'][0]['aliquots']:
                file_ids[aliquot['aliquot_id']] = file_id

        aliquot_data = await aliquot_task

        if aliquot_data is None:
            LOGGER.error("Invalid query for study_id: '%s'", study_id)

        # flatten aliquots into 1 list
        aliquots = list()
        for case in aliquot_data:
            for sample in case['samples']:
                for aliquot in sample['aliquots']:
                    new_a = {k: aliquot[k] for k in ('aliquot_id', 'analyte_type')}
                    new_a.update({k: sample[k] for k in ('sample_id', 'sample_submitter_id',
                                                        'sample_type', 'tissue_type')})
                    new_a['case_id'] = case['case_id']
                    new_a['file_id'] = file_ids.get(new_a['aliquot_id'], 'MISSING')

                    aliquots.append(new_a)

        return aliquots


    def get_study_aliquots(self, study_id: str, **kwargs) -> list|None:
        '''
        Get metadata for all aliquots in a study.

        Parameters
        ----------
        study_id: str
            The study ID.
        kwargs: dict
            Additional kwargs passed to async_get_study_aliquots

        Returns
        -------
        aliquots: list
            A list of aliquots in the study where each list element is metadata for each aliquot
            or None if no cases could be found for study_id.
        '''
        return self.loop.run_until_complete(self.async_get_study_aliquots(study_id, **kwargs))


    async def async_get_study_cases(self, study_id: str,
                                    page_limit: int=100) -> list|None:
        '''
        Async versio of get_study_cases.

        Parameters
        ----------
        study_id: str
            The study id.
        page_limit: int
            Page limit passed to _get_paginated_data.

        Returns
        -------
        cases: list
            A list of cases in the study where each list element is metadata for a case
            or None if no cases could be found for study_id.
        '''

        def query_f(study_id, page_offset, page_limit):
            return '''query={
                paginatedCaseDemographicsPerStudy (study_id: "%s" offset: %u limit: %u acceptDUA: true) {
                total
                caseDemographicsPerStudy {
                    case_id
                    demographics { ethnicity gender race cause_of_death
                        vital_status year_of_birth year_of_death }
                }
                pagination { count from page total pages size }
            } }''' % (study_id, page_offset, page_limit)

        data = await self._get_paginated_data(query_f, 'caseDemographicsPerStudy', study_id,
                                              page_limit=page_limit)

        if data is None:
            return None

        cases = list()
        for case in data:
            if len(case['demographics']) > 1:
                LOGGER.warning('Incorrect number of demographics in case %s', case["case_id"])
            new_case = case['demographics'][0]
            new_case['case_id'] = case['case_id']

            cases.append(new_case)

        return cases


    def get_study_cases(self, study_id: str, **kwargs) -> list|None:
        '''
        Get all metadata for all cases for a study.

        Parameters
        ----------
        study_id: str
            The study id.
        kwargs: dict
            Additional kwargs passed to async_get_study_cases.

        Returns
        -------
        cases: list
            A list of cases in the study where each list element is metadata for a case
            or None if no cases could be found for study_id.
        '''
        return self.loop.run_until_complete(self.async_get_study_cases(study_id))


    async def async_get_study_raw_files(self, study_id: str,
                                        n_files: int|None=None) -> list|None:
        '''
        Async versio of get_study_raw_files

        Parameters
        ----------
        study_id: str
            The study id.
        n_files: int
            Limit metadata to n files. If None metadata is returned for all files.

        Returns
        -------
        files: list
            A list of files in the study where each list element is metadata for a file
            or None if no files could be found for study_id.
        '''

        query = '''query {
        filesPerStudy (study_id: "%s" data_category: "Raw Mass Spectra" acceptDUA: true) {
                file_id file_name file_submitter_id md5sum file_size
                data_category file_type file_format signedUrl {url}}
            }''' % study_id

        # get a list of .raw files in study
        payload = await self._post(query)

        if 'errors' in payload:
            self._log_post_errors(payload['errors'])
            return None

        keys = ('file_id', 'file_name', 'file_submitter_id', 'md5sum', 'file_size',
                'data_category', 'file_type', 'file_format')
        data = list()
        for file in payload['data']['filesPerStudy']:
            if file['data_category'] == 'Raw Mass Spectra':
                new_file = {k: file[k] for k in keys}
                new_file['url'] = file['signedUrl']['url']
                data.append(new_file)

        if n_files is not None:
            return data[:n_files]

        return data


    def get_study_raw_files(self, study_id: str, **kwargs) -> list|None:
        '''
        Get metadata for raw files in a study

        Parameters
        ----------
        study_id: str
            The study id.
        kwargs: dict
            Additional kwargs passed to async_get_study_raw_files

        Returns
        -------
        files: list
            A list of files in the study where each list element is metadata for a file
            or None if no files could be found for study_id.
        '''
        return self.loop.run_until_complete(self.async_get_study_raw_files(study_id, **kwargs))
