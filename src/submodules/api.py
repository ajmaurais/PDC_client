
import re
import asyncio
import sys
import httpx

from .logger import LOGGER

BASE_URL ='https://proteomic.datacommons.cancer.gov/graphql'
FILE_METADATA_KEYS = ['file_id', 'file_name', 'file_submitter_id', 'md5sum', 'file_size',
                      'data_category', 'file_type', 'file_format', 'url']


async def _post(client, query, url, retries=5):
    query = re.sub(r'\s+', ' ', query.strip())
    for _ in range(retries):
        response = await client.post(url, json={'query': query})
        if response.status_code == 200:
            return response.json()
        if response.status_code >= 400 and response.status_code < 500:
            break
    sys.stderr.write(f'url:\n"{url}?{query}"\n')
    raise RuntimeError(f'Failed with response code {response.status_code}!')


async def _get(client, query, url, retries=5):
    query = re.sub(r'\s+', ' ', query.strip())

    for _ in range(retries):
        response = await client.get(f'{url}?{query}')
        if response.status_code == 200:
            return response.json()
        if response.status_code >= 400 and response.status_code < 500:
            break
    sys.stderr.write(f'url:\n"{url}?{query}"\n')
    raise RuntimeError(f'Failed with response code {response.status_code}!')


async def _async_get_study_id(client, pdc_study_id, url=BASE_URL, **kwargs):
    query = '''query={
        studyCatalog (pdc_study_id: "%s" acceptDUA: true){
            versions { study_id is_latest_version }
        }}''' % pdc_study_id

    data = await _get(client, query, url, **kwargs)

    if len(data['data']['studyCatalog']) == 0:
        return None

    for version in data['data']['studyCatalog'][0]['versions']:
        if version['is_latest_version'] == 'yes':
            return version['study_id']
    return None


async def async_get_study_id(pdc_study_id, **kwargs):
    ''' async version of get_study_id '''
    async with httpx.AsyncClient() as client:
        return await _async_get_study_id(client, pdc_study_id, **kwargs)


def get_study_id(pdc_study_id, **kwargs):
    '''
    Get latest study_id from a pdc_study_id.

    Parameters:
        pdc_study_id (str)

    Returns:
        study_id (str)
    '''
    return asyncio.run(async_get_study_id(pdc_study_id, **kwargs))


async def _async_get_study_metadata(client, pdc_study_id=None, study_id=None, url=BASE_URL, **kwargs):
    if study_id is not None:
        _id = study_id
        id_name = 'study_id'
        study_id_task = None
    elif pdc_study_id is not None:
        _id = pdc_study_id
        id_name = 'pdc_study_id'
        study_id_task = asyncio.create_task(
                _async_get_study_id(client, pdc_study_id, url=url, **kwargs)
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

    study_task = asyncio.create_task(_get(client, study_query, url, **kwargs))
    data = await study_task

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


async def async_get_study_metadata(pdc_study_id=None, study_id=None, **kwargs):
    async with httpx.AsyncClient() as client:
        return await _async_get_study_metadata(client,
                                               pdc_study_id=pdc_study_id,
                                               study_id=study_id,
                                               **kwargs)


def get_study_metadata(pdc_study_id=None, study_id=None, **kwargs):
    return asyncio.run(async_get_study_metadata(pdc_study_id=pdc_study_id, study_id=study_id,
                                                **kwargs))


def get_pdc_study_id(study_id, **kwargs):
    '''
    Get pdc_study_id from a study_id.

    Parameters:
        study_id (str)

    Returns:
        pdc_study_id (str)
    '''
    data = get_study_metadata(study_id=study_id, **kwargs)
    if data is not None:
        return data['pdc_study_id']
    return None


def get_study_name(study_id, **kwargs):
    '''
    Get study_name for a study_id.

    Parameters:
        study_id (str)

    Returns:
        study_name (str)
    '''
    data = get_study_metadata(study_id=study_id, **kwargs)
    if data is not None:
        return data['study_name']
    return None


async def _get_paginated_data(query_f, url, data_name, study_id,
                              client=None, page_limit=10, verify=True):

    endpoint_name = f'paginated{data_name[0].upper()}{data_name[1:]}'

    init_client = client is None
    if init_client:
        client = httpx.AsyncClient(verify=verify)

    data = list()
    try:
        query = query_f(study_id, 0, page_limit)
        first_page = await _get(client, query, url)

        total = first_page['data'][endpoint_name]['total']

        if total <= page_limit:
            return [first_page]

        offset = page_limit
        page_tasks = list()
        async with asyncio.TaskGroup() as tg:
            while offset < total:
                page_tasks.append(
                    tg.create_task(_get(client, query_f(study_id, offset, page_limit), url))
                )
                offset += page_limit
    finally:
        if init_client:
            await client.aclose()

    data = [first_page] + [task.result() for task in page_tasks]

    return data


async def _async_get_study_biospecimens(client, study_id, url=BASE_URL, **kwargs):
    def query_f(study_id, offset, limit):
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
            } } ''' % (study_id, offset, limit)

    pages = await _get_paginated_data(query_f, url, 'casesSamplesAliquots',
                                      study_id, client=client, **kwargs)

    # Flatten pages into 1 list
    data = list()
    for page in pages:
        data += page['data']['paginatedCasesSamplesAliquots']['casesSamplesAliquots']

    # flatten aliquots into 1 list
    aliquots = list()
    for case in data:
        for sample in case['samples']:
            for aliquot in sample['aliquots']:
                new_a = {k: aliquot[k] for k in ('aliquot_id', 'analyte_type')}
                new_a.update({k: sample[k] for k in ('sample_id', 'sample_submitter_id',
                                                     'sample_type', 'tissue_type')})
                new_a['case_id'] = case['case_id']

                aliquots.append(new_a)

    return aliquots


async def async_get_study_biospecimens(study_id, verify=True, timeout=15, **kwargs):
    async with httpx.AsyncClient(verify=verify, timeout=timeout) as client:
        return await _async_get_study_biospecimens(client, study_id, **kwargs)


def get_study_biospecimens(study_id, **kwargs):
    return asyncio.run(async_get_study_biospecimens(study_id, **kwargs))


def case_metadata(study_id, url, **kwargs):

    def make_case_query(study_id, page_offset, page_limit=100):
        return '''query={
            paginatedCaseDemographicsPerStudy (study_id: "%s" offset: %u limit: %u acceptDUA: true) {
            total
            caseDemographicsPerStudy {
                case_id
                demographics {
                    ethnicity
                    gender
                    race
                    cause_of_death
                    vital_status
                    year_of_birth
                    year_of_death
                    }
            } pagination { count sort from page total pages size } }}''' % (study_id, page_offset, page_limit)

    data = _get_paginated_data(make_case_query, url, 'caseDemographicsPerStudy', study_id, **kwargs)

    keys = ['ethnicity', 'gender', 'race', 'cause_of_death', 'vital_status', 'year_of_birth', 'year_of_death']

    with ThreadPoolExecutor(max_workers=max_threads) as pool:
        case_data = dict(pool.map(lambda x: single_case(x, url, **kwargs), set([d['case_id'] for d in data])))

    cases = list()
    for case in data:
        if len(case['demographics']) != 1:
            RuntimeError(f'Incorrect number of demographics in case {case["case_id"]}')
        newData = {key: case['demographics'][0][key] for key in keys}
        newData['case_id'] = case['case_id']

        for k, v in case_data[newData['case_id']].items():
            newData[k] = v
        cases.append(newData)

    return cases


async def _async_get_raw_files(client, study_id, url=BASE_URL, n_files=None, **kwargs):
    query = '''query {
       filesPerStudy (study_id: "%s" data_category: "Raw Mass Spectra" acceptDUA: true) {
            file_id file_name file_submitter_id md5sum file_size
            data_category file_type file_format signedUrl {url}}
        }''' % study_id

    # get a list of .raw files in study
    payload = await _post(client, query, url, **kwargs)

    if 'errors' in payload:
        LOGGER.error('API query failed with response(s):')
        for error in payload['errors']:
            LOGGER.error('\n\tCode: %s\n\tEndpoint: %s\n\tMessage: %s\n',
                         error['extensions']['code'],
                         ', '.join(error['path']),
                         error['message'])
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


async def async_get_raw_files(study_id, **kwargs):
    async with httpx.AsyncClient() as client:
        return await _async_get_raw_files(client, study_id, **kwargs)


def get_raw_files(study_id, **kwargs):
    ''' Get metadata for raw files in a study '''
    return asyncio.run(async_get_raw_files(study_id, **kwargs))


