
import re
import asyncio
from typing import Callable, Optional

from httpx import Limits, AsyncClient
from httpx import ConnectError

from .logger import LOGGER

CLIENT_TIMEOUT = 10
BASE_URL ='https://proteomic.datacommons.cancer.gov/graphql'

FILE_DATA_KEYS = ['file_id', 'file_name', 'file_submitter_id', 'md5sum', 'file_size',
                  'experiment_type', 'analytical_fraction', 'analyte_type',
                  'data_category', 'file_type', 'file_format', 'url']

DATA_ID_KEYS = ["file_id", "file_submitter_id",
                "sample_id", "case_id", "demographic_id"]

class Client():
    '''
    Client class for interacting with the PDC API.

    Attributes
    ----------
    url: str
        The base URL for the API.
    request_retries: int
        Number of times to retry a request in case of failure.
    client: httpx.AsyncClient
        The HTTP client for making requests.

    Methods
    -------
    async async_get_study_id(pdc_study_id: str) -> str|None:
        Asynchronously gets the study ID for a given PDC study ID.
    get_study_id(pdc_study_id: str) -> str|None:
        Gets the study ID for a given PDC study ID.
    async async_get_study_metadata(pdc_study_id: str|None=None, study_id: str|None=None) -> list|None:
        Asynchronously gets the metadata for a study.
    get_study_metadata(pdc_study_id: str|None=None, study_id: Optional[str]=None) -> list|None
        Gets the metadata for a study.
    get_pdc_study_id(study_id: str) -> str:
        Gets the PDC study ID for a given study ID.
    get_study_name(study_id: str) -> str:
        Gets the study name for a given study ID.
    async async_get_study_samples(study_id: str, file_ids: Optional[list]=None, page_limit: int=100) -> list:
        Asynchronously gets the samples and aliquots for a study.
    get_study_samples(study_id: str, **kwargs) -> list|None:
        Gets the samples and aliquots for a study.
    async async_get_study_cases(study_id: str, page_limit: int=100) -> list|None:
        Asynchronously gets the cases for a study.
    get_study_cases(study_id: str, **kwargs) -> list|None:
        Gets the cases for a study.
    async async_get_study_raw_files(study_id: str, n_files: Optional[int]=None) -> list|None:
        Asynchronously gets the raw files for a study.
    get_study_raw_files(study_id: str, **kwargs) -> list|None:
        Gets the raw files for a study.
    async async_get_file_url(file_id: str) -> dict|None:
        Asynchronously gets the URL for a file.
    get_file_url(file_id: str) -> dict|None:
        Gets the URL for a file.
    '''

    def __init__(self,
                 url: str = BASE_URL,
                 timeout: Optional[int]=CLIENT_TIMEOUT,
                 verify: Optional[bool]=True,
                 max_connections: Optional[int]=5,
                 max_keepalive_connections: Optional[int]=5,
                 keepalive_expiry: Optional[int]=5,
                 request_retries: Optional[int]=5):
        '''
        Parameters
        ----------
        url: str
            The base URL for the API.
        timeout: int
            The timeout for requests in seconds.
        verify: bool
            Whether to verify SSL certificates.
        max_connections: int
            The maximum number of connections to allow.
        max_keepalive_connections: int
            The maximum number of connections to keep alive.
        keepalive_expiry: int
            The number of seconds to keep a connection alive.
        request_retries: int
            The number of times to retry a request in case of failure.
        '''

        self.url = url
        self.request_retries = request_retries

        try:
            self._loop = asyncio.get_running_loop()
            self._initialized_loop = False
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
            self._initialized_loop = True

        self.client = AsyncClient(limits=Limits(max_connections=max_connections,
                                                max_keepalive_connections=max_keepalive_connections,
                                                keepalive_expiry=keepalive_expiry),
                                  timeout=timeout, verify=verify)


    def __del__(self):
        if self._initialized_loop:
            self._loop.close()


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc, tb):
        try:
            self._loop.run_until_complete(self.client.aclose())
        except RuntimeError:
            asyncio.run(self.client.aclose())
        if self._initialized_loop:
            self._loop.close()


    async def __aenter__(self):
        return self


    async def __aexit__(self, exc_type, exc, tb):
        try:
            await self.client.aclose()
        except RuntimeError:
            asyncio.run(self.client.aclose())
        if self._initialized_loop:
            self._loop.close()


    def __await__(self):
        async def closure():
            return self
        return closure().__await__()


    async def _post(self, query: str) -> dict:
        query = re.sub(r'\s+', ' ', query.strip())
        for _ in range(self.request_retries):
            try:
                response = await self.client.post(self.url, json={'query': query})
                if response.status_code == 200:
                    return response.json()
            # if response.status_code >= 400 and response.status_code < 500:
            #     break
            except ConnectError:
                LOGGER.error('Invalid URL: %s', self.url, stacklevel=2)
                return
        LOGGER.error('Error in query:\n\t%s\n\tstatus_code: %s\n\ttext: %s',
                     query, response.status_code, response.text,
                     stacklevel=2)
        return None


    async def _get(self, query) -> dict:
        query = re.sub(r'\s+', ' ', query.strip())
        for _ in range(self.request_retries):
            try:
                response = await self.client.get(f'{self.url}?{query}')
                if response.status_code == 200:
                    return response.json()
            # if response.status_code >= 400 and response.status_code < 500:
            #     break
            except ConnectError:
                LOGGER.error('Invalid URL: %s', self.url, stacklevel=2)
                return
        LOGGER.error('Error in query:\n\t%s\n\tstatus_code: %s\n\ttext: %s',
                     query, response.status_code, response.text,
                     stacklevel=2)
        return None


    @staticmethod
    def _log_post_errors(errors: list):
        ''' Write _post errors to LOGGER '''

        LOGGER.error('API query failed with response(s):', stacklevel=2)
        for error in errors:
            LOGGER.error('\n\tCode: %s\n\tEndpoint: %s\n\tMessage: %s\n',
                        error['extensions']['code'] if 'extensions' in error else None,
                        ', '.join(error['path']),
                        error['message'],
                        stacklevel=2)


    @staticmethod
    def _study_catalog_query(pdc_study_id):
        return '''query={
        studyCatalog (pdc_study_id: "%s" acceptDUA: true){
            versions { study_id is_latest_version }
        }}''' % pdc_study_id


    async def async_get_study_catalog(self, pdc_study_id: str) -> list:
        '''
        Get studyCatalog for a pdc_study_id.

        Parameters
        ----------
        pdc_study_id: str
            The PDC study ID.

        Returns
        -------
        study_catalog: list
            A list of dictionaries where each dictionary is a version of the study.
        '''
        query = self._study_catalog_query(pdc_study_id)
        data = await self._get(query)

        if data is None or len(data['data']['studyCatalog']) == 0:
            return None

        if len(data['data']['studyCatalog']) > 1:
            raise RuntimeError('More studies than expected for pdc_study_id!')

        versions = data['data']['studyCatalog'][0]
        for study in versions['versions']:
            if study['is_latest_version'] == 'yes':
                study['is_latest_version'] = True
            else:
                study['is_latest_version'] = False
        return versions


    def get_study_catalog(self, pdc_study_id: str) -> list:
        '''
        Get studyCatalog for a pdc_study_id.

        Parameters
        ----------
        pdc_study_id: str
            The PDC study ID.

        Returns
        -------
        study_catalog: list
            A list of dictionaries where each dictionary is a version of the study.
        '''
        return self._loop.run_until_complete(self.async_get_study_catalog(pdc_study_id))


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
        data = await self.async_get_study_catalog(pdc_study_id)
        if data is None:
            return None

        for version in data['versions']:
            if version['is_latest_version']:
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
        return self._loop.run_until_complete(self.async_get_study_id(pdc_study_id))


    @staticmethod
    def _study_metadata_query(id_name, query_id):
        return '''query={
            study (%s: "%s" acceptDUA: true) {
                study_id pdc_study_id study_name study_submitter_id
                analytical_fraction experiment_type
                cases_count aliquots_count
            }
        } ''' % (id_name, query_id)


    async def async_get_study_metadata(self, pdc_study_id: str|None=None,
                                       study_id: str|None=None,
                                       only_latest: bool=True) -> dict|list|None:
        '''
        Async version of get_study_metadata

        Parameters
        ----------
        pdc_study_id: str
            If None the study_id must be specified.
        study_id: str
            If None the pdc_study_id must be specified.
        only_latest: bool
            If True only the latest version of the study is returned.

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

        study_query = self._study_metadata_query(id_name, _id)
        data = await asyncio.create_task(self._get(study_query))

        if data is None:
            return None

        if study_id_task is None:
            if data['data']['study'] is None or len(data['data']['study']) == 0:
                return None
            return data['data']['study'][0]

        study_id = await study_id_task

        if study_id is None:
            return None

        if only_latest:
            for study in data['data']['study']:
                if study['study_id'] == study_id:
                    return study
            raise RuntimeError('Could not find latest study for pdc_study_id!')

        # add is_latest field to each study
        for study in data['data']['study']:
            if study['study_id'] == study_id:
                study['is_latest_version'] = True
            else:
                study['is_latest_version'] = False
        return data['data']['study']


    def get_study_metadata(self, pdc_study_id: str|None=None,
                           study_id: Optional[str]=None,
                           **kwargs) -> Optional[dict]:
        '''
        Parameters
        ----------
        pdc_study_id: str
            If None the study_id must be specified.
        study_id: str
            If None the pdc_study_id must be specified.
        kwargs: dict
            Additional kwargs passed to async_get_study_metadata.

        Returns
        -------
        metadata: dict
            The metadata or None if no metadata could be found for study_id
        '''
        return self._loop.run_until_complete(
            self.async_get_study_metadata(pdc_study_id=pdc_study_id,
                                          study_id=study_id, **kwargs)
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


    @staticmethod
    def _experimental_metadata_query(study_submitter_id: str):
        return '''query={
            experimentalMetadata (study_submitter_id: "%s") {
                study_run_metadata { study_run_metadata_id study_run_metadata_submitter_id
                    aliquot_run_metadata {
                        aliquot_id
                        aliquot_run_metadata_id
                    }
                }
            } }''' % study_submitter_id


    async def async_get_experimental_metadata(self, study_submitter_id: str) -> dict|None:
        '''
        Async version of get_experimental_metadata

        Parameters
        ----------
        study_submitter_id: str
            The study submitter ID.

        Returns
        -------
        metadata: dict
            A dictionary with the experimental metadata or None if no metadata could be found.
        '''

        query = self._experimental_metadata_query(study_submitter_id)
        data = await self._get(query)

        if data['data']['experimentalMetadata'] is None or len(data['data']['experimentalMetadata']) == 0:
            LOGGER.error("No experimental metadata found for study_submitter_id: '%s'", study_submitter_id)
            return None

        if len(data['data']['experimentalMetadata']) > 1:
            LOGGER.error("More than 1 experiment found for study_submitter_id: '%s'",
                         study_submitter_id)
            return None

        data = data['data']['experimentalMetadata'][0]['study_run_metadata']
        for run in data:
            run['study_run_metadata_submitter_id'] = run['study_run_metadata_submitter_id'].lstrip()

        return data


    def get_experimental_metadata(self, study_submitter_id: str) -> dict|None:
        '''
        Get experimental metadata for a study_submitter_id.

        Parameters
        ----------
        study_submitter_id: str
            The study submitter ID.

        Returns
        -------
        metadata: dict
            A dictionary with the experimental metadata or None if no metadata could be found.
        '''
        return self._loop.run_until_complete(self.async_get_experimental_metadata(study_submitter_id))


    async def _get_paginated_data(self,
                                  query_f: Callable[[str, str, int], str],
                                  data_name: str,
                                  study_id: str,
                                  page_limit: int=100) -> list:

        endpoint_name = f'paginated{data_name[0].upper()}{data_name[1:]}'

        data = list()
        query = query_f(study_id, 0, page_limit)
        first_page = await self._get(query)

        if first_page is None:
            return None

        if first_page['data'][endpoint_name] is None:
            LOGGER.error("Invalid query for study_id: '%s'", study_id)
            return None

        total = first_page['data'][endpoint_name]['total']

        if total <= page_limit:
            data = first_page['data'][endpoint_name][data_name]
            if len(data) != total:
                raise RuntimeError(f"Expected {total} items, but got {len(data)} items.")
            return data

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

        if len(data) != total:
            raise RuntimeError(f"Expected {total} items, but got {len(data)} items.")
        return data


    @staticmethod
    def _case_aliquot_query(study_id: str, offset: int, limit: int):
        '''query to get study, cases, samples, and aliquots'''
        return '''query={
            paginatedCasesSamplesAliquots (study_id: "%s" offset: %u limit: %u acceptDUA: true) {
                total
                casesSamplesAliquots {
                    case_id
                    samples { sample_id sample_submitter_id sample_type tissue_type
                        aliquots { aliquot_id analyte_type }
                    }
                }
                pagination { count from total }
            } }''' % (study_id, offset, limit)


    @staticmethod
    def _file_aliquot_query(file_id):
        ''' query to get aliquot IDs associated with each file. '''
        return '''query={
            fileMetadata (file_id: "%s" acceptDUA: true) {
                file_id study_run_metadata_id aliquots { aliquot_id } }
                }''' % file_id


    @staticmethod
    def _study_file_id_query(study_id):
        ''' query to get all file_ids in study '''
        return '''query {
            filesPerStudy (study_id: "%s", data_category: "Raw Mass Spectra" acceptDUA: true) {
                file_id }
            } ''' % study_id


    async def async_get_study_samples(self, study_id: str,
                                      file_ids: Optional[list]=None,
                                      page_limit: int=100) -> list:
        '''
        Async version of get_study_samples.

        Parameters
        ----------
        study_id: str
            The study ID.
        file_ids: list
            A list of file IDs to retreive data for. If None, all the files in the study are used.
        page_limit: int
            Page size limit passed to _get_paginated_data.

        Returns
        -------
        aliquots: list
            A list of aliquots in the study where each list element is metadata for an aliquot
            or None if no cases could be found for study_id.
        '''

        aliquot_task = asyncio.create_task(
                self._get_paginated_data(self._case_aliquot_query, 'casesSamplesAliquots', study_id,
                                         page_limit=page_limit)
            )

        if file_ids is None:
            file_id_query = self._study_file_id_query(study_id)
            file_id_data = await self._post(file_id_query)

            if file_id_data is None:
                return None

            if 'errors' in file_id_data:
                self._log_post_errors(file_id_data['errors'])
                return None

            file_ids = [f['file_id'] for f in file_id_data['data']['filesPerStudy']]

            if len(file_ids) != len(set(file_ids)):
                raise RuntimeError('Duplicate file_ids in study!')

        aliquot_id_tasks = list()
        async with asyncio.TaskGroup() as tg:
            for file_id in file_ids:
                aliquot_id_tasks.append(
                    tg.create_task(self._get(self._file_aliquot_query(file_id)))
                )

        # construct dictionary of aliquot_ids maped to file_ids
        file_aliquot_ids = dict()
        for file_id, task in zip(file_ids, aliquot_id_tasks):
            data = task.result()
            if data is None or data['data']['fileMetadata'] is None:
                LOGGER.error("Error getting aliquot_ids for file: '%s'", file_id)
                continue
            query_file_id = data['data']['fileMetadata'][0]['file_id']
            srm_id = data['data']['fileMetadata'][0]['study_run_metadata_id']
            assert(file_id == query_file_id)
            for aliquot in data['data']['fileMetadata'][0]['aliquots']:
                if aliquot['aliquot_id'] not in file_aliquot_ids:
                    file_aliquot_ids[aliquot['aliquot_id']] = set()
                file_aliquot_ids[aliquot['aliquot_id']].add((file_id, srm_id))

        # convert sets of tuples to dicts
        for aliquot_id in file_aliquot_ids:
            file_aliquot_ids[aliquot_id] = {file_id: srm_id
                                            for file_id, srm_id in file_aliquot_ids[aliquot_id]}

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

                    if aliquot['aliquot_id'] in file_aliquot_ids:
                        new_a['file_id_to_run_metadata_id'] = file_aliquot_ids[new_a['aliquot_id']]
                        aliquots.append(new_a)

        return aliquots


    def get_study_samples(self, study_id: str, **kwargs) -> list|None:
        '''
        Get metadata for all samples and aliquots in a study.

        Parameters
        ----------
        study_id: str
            The study ID.
        kwargs: dict
            Additional kwargs passed to async_get_study_samples

        Returns
        -------
        aliquots: list
            A list of aliquots in the study where each list element is metadata for each aliquot
            or None if no cases could be found for study_id.
        '''
        return self._loop.run_until_complete(self.async_get_study_samples(study_id, **kwargs))


    @staticmethod
    def _study_case_query(study_id, offset, limit):
        return '''query={
            paginatedCaseDemographicsPerStudy (study_id: "%s" offset: %u limit: %u acceptDUA: true) {
            total
            caseDemographicsPerStudy {
                case_id
                demographics { demographic_id ethnicity gender race cause_of_death
                               vital_status year_of_birth year_of_death }
            }
            pagination { count from total }
        } }''' % (study_id, offset, limit)


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

        data = await self._get_paginated_data(self._study_case_query, 'caseDemographicsPerStudy', study_id,
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
        return self._loop.run_until_complete(self.async_get_study_cases(study_id, **kwargs))


    @staticmethod
    def _study_raw_file_query(study_id, data_category='Raw Mass Spectra'):
        data_category_str = '' if data_category is None else f'data_category: "{data_category}"'
        return '''query {
            filesPerStudy (study_id: "%s" %s acceptDUA: true) {
                file_id file_name file_submitter_id md5sum file_size
                data_category file_type file_format file_location signedUrl {url}}
            }''' % (study_id, data_category_str)


    async def async_get_study_raw_files(self, study_id: str,
                                        use_s3_path: bool=False,
                                        n_files: Optional[int]=None) -> list|None:
        '''
        Async versio of get_study_raw_files

        Parameters
        ----------
        study_id: str
            The study id.
        use_s3_path: bool
            If True, use the S3 path for the file URL. If False, use the signed URL.
        n_files: int
            Limit metadata to n files. If None metadata is returned for all files.

        Returns
        -------
        files: list
            A list of files in the study where each list element is metadata for a file
            or None if no files could be found for study_id.
        '''

        # get a list of .raw files in study
        query = self._study_raw_file_query(study_id)
        payload = await self._post(query)

        if payload is None:
            return None

        if 'errors' in payload:
            self._log_post_errors(payload['errors'])
            return None

        keys = ('file_id', 'file_name', 'file_submitter_id', 'md5sum', 'file_size',
                'data_category', 'file_type', 'file_format', 'file_location')
        data = list()
        for file in payload['data']['filesPerStudy']:
            if file['data_category'] == 'Raw Mass Spectra':
                new_file = {k: file[k] for k in keys}

                if use_s3_path:
                    new_file['url'] = f"s3://pdcdatastore/{file['file_location']}"
                else:
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
        return self._loop.run_until_complete(self.async_get_study_raw_files(study_id, **kwargs))


    @staticmethod
    def _file_metadata_query(file_id):
        return '''query={
            fileMetadata (file_id: "%s" acceptDUA: true) {
                file_name file_type data_category file_format md5sum file_size
            }}''' % file_id


    @staticmethod
    def _file_url_query(file_name, file_type, data_category, file_format):
        return '''query { filesPerStudy (
            file_name: "%s" file_type: "%s" data_category: "%s" file_format: "%s" acceptDUA: true) {
                file_id, md5sum, file_size, signedUrl { url }
            }}''' % (file_name, file_type, data_category, file_format)


    async def async_get_file_url(self, file_id: str) -> dict|None:
        '''
        Async version of get_file_url

        Parameters
        ----------
        file_id: str
            The file ID.

        Returns
        -------
        file_data: dict
            A dictionary with the file_name, file_size, url, and md5
            or None if no url could be found for file_id.
        '''
        # get file metadata
        file_data = await self._get(self._file_metadata_query(file_id))
        if file_data is None or file_data['data']['fileMetadata'] is None or \
           len(file_data['data']['fileMetadata']) == 0:
            LOGGER.error("No file found for file_id: '%s'", file_id)
            return None
        file_data = file_data['data']['fileMetadata'][0]

        # get file url
        query = self._file_url_query(*[file_data[k] for k in ('file_name', 'file_type',
                                                              'data_category', 'file_format')])
        payload = await self._post(query)

        if 'errors' in payload:
            self._log_post_errors(payload['errors'])
            return None

        if len(payload['data']['filesPerStudy']) == 0:
            LOGGER.error("No file found for file_id: '%s'", file_id)
            return None

        url_datas = [file for file in payload['data']['filesPerStudy'] if file['file_id'] == file_id]
        if len(url_datas) != 1:
            raise RuntimeError(f'Ambigious file_id: {file_id}. Multiple files found!')
        url_data = url_datas[0]

        if not all(file_data[k] == url_data[k] for k in ('file_size', 'md5sum')):
            raise RuntimeError(f'file_data does not match url_data for file_id: {file_id}')

        ret = dict()
        ret['file_name'] = file_data['file_name']
        ret['file_size'] = url_data['file_size']
        ret['md5sum'] = url_data['md5sum']
        ret['url'] = url_data['signedUrl']['url']
        return ret


    def get_file_url(self, file_id: str) -> dict|None:
        '''
        Get file url, name, size and md5 for a file_id

        Parameters
        ----------
        file_id: str
            The file ID.

        Returns
        -------
        file_data: dict
            A dictionary with the file_name, file_size, url, and md5
            or None if no url could be found for file_id.
        '''
        return self._loop.run_until_complete(self.async_get_file_url(file_id))
