
import re
import asyncio
# from concurrent.futures import ThreadPoolExecutor
# from multiprocessing import cpu_count
import sys
# import aiohttp
import aiohttp

from .logger import LOGGER

# MAX_THREADS = cpu_count()
MAX_THREADS = 14
BASE_URL ='https://proteomic.datacommons.cancer.gov/graphql'
FILE_METADATA_KEYS = ['file_id', 'file_name', 'md5sum', 'file_location', 'file_size',
                      'data_category', 'file_type', 'file_format', 'url']

async def _post(session, query, url, retries=5):
    query = re.sub(r'\s+', ' ', query.strip())
    # try:
        # for _ in range(retries):
    async with session.post(url, json={'query': query}) as response:
        if response.status == 200:
            return await response.json()
        # if response.status >= 400 and response.status < 500:
        #     break
    # except aiohttp.ClientSSLError as e:
    #     message = "SSL certificate verification failed! Use --skipVerify to skip SSL verification."
    #     raise RuntimeError(message) from e
    sys.stderr.write(f'url:\n"{url}?{query}"\n')
    raise RuntimeError(f'Failed with response code {response.status}!')


async def _get(session, query, url, retries=5, **kwargs):
    query = re.sub(r'\s+', ' ', query.strip())
    # try:
    async with session.get(f'{url}?{query}', **kwargs) as response:
        if response.status == 200:
            return await response.json()
            # if response.status >= 400 and response.status < 500:
            #     break
    # except aiohttp.ClientSSLError as e:
    #     message = "SSL certificate verification failed! Use --skipVerify to skip SSL verification."
    #     raise RuntimeError(message) from e
    sys.stderr.write(f'url:\n"{url}?{query}"\n')
    raise RuntimeError(f'Failed with response code {response.status}!')


async def _async_get_study_id(session, pdc_study_id, url=BASE_URL, **kwargs):
    query = '''query={
        studyCatalog (pdc_study_id: "%s" acceptDUA: true){
            versions { study_id is_latest_version }
        }}''' % pdc_study_id

    data = await _get(session, query, url, **kwargs)

    if len(data['data']['studyCatalog']) == 0:
        return None

    for version in data['data']['studyCatalog'][0]['versions']:
        if version['is_latest_version'] == 'yes':
            return version['study_id']
    return None


async def async_get_study_id(pdc_study_id, **kwargs):
    ''' async version of get_study_id '''
    async with aiohttp.ClientSession() as session:
        return await _async_get_study_id(session, pdc_study_id, **kwargs)


def get_study_id(pdc_study_id, **kwargs):
    '''
    Get latest study_id from a pdc_study_id.

    Parameters:
        pdc_study_id (str)

    Returns:
        study_id (str)
    '''
    return asyncio.run(async_get_study_id(pdc_study_id, **kwargs))


async def _async_get_study_metadata(session, pdc_study_id=None, study_id=None, url=BASE_URL, **kwargs):
    if study_id is not None:
        _id = study_id
        id_name = 'study_id'
        study_id_task = None
    elif pdc_study_id is not None:
        _id = pdc_study_id
        id_name = 'pdc_study_id'
        study_id_task = asyncio.create_task(_async_get_study_id(session, pdc_study_id, url=url, **kwargs))
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

    study_task = asyncio.create_task(_get(session, study_query, url, **kwargs))
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
    async with aiohttp.ClientSession() as session:
        return await _async_get_study_metadata(session,
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


def _get_paginated_data(query_f, url, data_name, study_id,
                        page_limit=10, no_change_iterations_limit=2, **kwargs):

    ret = list()
    done = False
    total_entries = None
    entry_i = 0
    offset = 0
    endpoint_name = 'paginated{}{}'.format(data_name[0].upper(), data_name[1:])
    no_change_iterations = 0
    previous_len = 0

    while True:
        data = _get(query_f(study_id, offset, page_limit), url, **kwargs)
        ret += data['data'][endpoint_name][data_name]

        offset += page_limit

        # probably will delete this later
        if total_entries is None:
            total_entries = data['data'][endpoint_name]['total']
        else:
            if total_entries != data['data'][endpoint_name]['total']:
                raise RuntimeError('Something is wrong...')

        entry_i += len(data['data'][endpoint_name][data_name])
        if entry_i >= total_entries:
            return ret

        # check that we are not in an infinite loop
        if len(ret) == previous_len:
            no_change_iterations += 1
        previous_len = len(ret)
        if no_change_iterations >= no_change_iterations_limit:
            raise RuntimeError('Something is wrong...')


def case_metadata(study_id, url, max_threads=MAX_THREADS, **kwargs):

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


def single_case(case_id, url, **kwargs):

    query = '''query={case (case_id: "%s" acceptDUA: true) {
    case_id
    primary_site
    samples {
        sample_id
        aliquots { aliquot_id }
    }
    diagnoses{
        tissue_or_organ_of_origin
        primary_diagnosis
        tumor_grade
        tumor_stage
        }
    }} ''' % case_id

    r = _get(query, url, **kwargs)
    data = r['data']['case'][0]

    if len(data['diagnoses']) != 1:
        raise RuntimeError(f'More than 1 diagnoses in case_id: {case_id}')
    data['diagnoses'] = data['diagnoses'][0]

    keys = ['case_id', 'primary_site', 'samples']
    ret = {key: data[key] for key in keys}
    diagnosis_keys = ['tissue_or_organ_of_origin', 'primary_diagnosis', 'tumor_grade', 'tumor_stage']
    for key in diagnosis_keys:
        ret[key] = data['diagnoses'][key]

    return case_id, ret


def aliquot_id(file_id, url, **kwargs):
    '''
    Get the aliquot ID for a file.
    '''

    query = '''query={
    fileMetadata (file_id: "%s" acceptDUA: true) {
        aliquots { aliquot_id } }
        }''' % file_id

    r = _get(query, url, **kwargs)
    if len(r['data']['fileMetadata']) != 1:
        raise RuntimeError(f'Too many files in query for file_id: {file_id}')
    if len(r['data']['fileMetadata'][0]['aliquots']) != 1:
        raise RuntimeError(f'Too many aliquot IDs for file_id: {file_id}')
    return file_id, r['data']['fileMetadata'][0]['aliquots'][0]['aliquot_id']


async def _async_get_raw_files(session, study_id, url=BASE_URL, n_files=None, **kwargs):
    query = '''query {
       filesPerStudy (study_id: "%s" data_category: "Raw Mass Spectra" acceptDUA: true) {
            file_id file_name md5sum file_location file_size
            data_category file_type file_format signedUrl {url}}
        }''' % study_id

    # get a list of .raw files in study
    payload = await _post(session, query, url, **kwargs)

    if 'errors' in payload:
        LOGGER.error('API query failed with response(s):')
        for error in payload['errors']:
            LOGGER.error('\n\tCode: %s\n\tEndpoint: %s\n\tMessage: %s\n',
                         error['extensions']['code'],
                         ', '.join(error['path']),
                         error['message'])
        return None

    keys = ('file_id', 'file_name', 'md5sum', 'file_location',
            'file_size', 'data_category', 'file_type', 'file_format')
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
    async with aiohttp.ClientSession() as session:
        data = await _async_get_raw_files(session, study_id, **kwargs)
    return data


def get_raw_files(study_id, **kwargs):
    ''' Get metadata for raw files in a study '''
    return asyncio.run(async_get_raw_files(study_id, **kwargs))


def metadata(study_id, url=BASE_URL, n_files=None, max_threads=MAX_THREADS, **kwargs):
    '''
    Get metadata for each raw file in a study

    Parameters:
        study_id (str): The PDC study id.
        n_files (int): The number of files to get data for. If None data for all files are retreived.
        max_threads (int): The max number of threads to use for making api calls.

        kwargs: additional arguments passed to requests.get

    Returns:
        file_dat (list): A list of dicts where each list element is a file.
    '''

    # Get file metadata
    file_data = raw_files(study_id, url, n_files=n_files, **kwargs)
    if file_data is None:
        return None

    # add aliquot_id to file metadata
    with ThreadPoolExecutor(max_workers=max_threads) as pool:
        aliquot_ids = dict(pool.map(lambda x: aliquot_id(x, url, **kwargs),
                                    set([f['file_id'] for f in file_data])))
    for file in file_data:
        file['aliquot_id'] = aliquot_ids[file['file_id']]

    # Get metadata for cases in files
    cases = case_metadata(study_id, url, max_threads=max_threads, **kwargs)
    cases_per_aliquot = dict()
    for case in cases:
        samples = case.pop('samples')
        for sample in samples:
            for aliquot in sample['aliquots']:
                assert aliquot['aliquot_id'] not in cases_per_aliquot
                cases_per_aliquot[aliquot['aliquot_id']] = case

    # Add case metadata to file metadata
    for file in file_data:
        file.update(cases_per_aliquot[file['aliquot_id']])

        # set None values to 'NA'
        for k in file.keys():
            if file[k] is None:
                file[k] = 'NA'

    return file_data

