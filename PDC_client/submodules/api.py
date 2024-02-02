
import requests
import json
import re
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
import sys

MAX_THREADS = cpu_count()
BASE_URL ='https://proteomic.datacommons.cancer.gov/graphql'
FILE_METADATA_KEYS = [ "file_id", "file_name", "md5sum", "file_location", "file_size",
                       "data_category", "file_type", "file_format", "url"]

def _post(query, url, retries=5, **kwargs):
    for i in range(retries):
        try:
            r = requests.post(url, json = {'query': query}, **kwargs)
        except requests.exceptions.SSLError as e:
            raise RuntimeError("SSL certificate verification failed! Use --skipVerify to skip SSL verification.")
        if r.status_code == 200:
            return r.json()
    sys.stderr.write(f'url:\n"{url}?{query}"\n')
    raise RuntimeError(f'Failed with response code {r.status_code}!')


def _get(query, url, retries=5, **kwargs):
    query = re.sub('\s+', ' ', query)
    for i in range(retries):
        try:
            r = requests.get(f'{url}?{query}', **kwargs)
        except requests.exceptions.SSLError as e:
            raise RuntimeError("SSL certificate verification failed! Use --skipVerify to skip SSL verification.")
        if r.status_code == 200:
            return r.json()
    sys.stderr.write(f'url:\n"{url}?{query}"\n')
    raise RuntimeError(f'Failed with response code {r.status_code}!')
 

def pdc_study_id(study_id, url, **kwargs):
    '''
    Get pdc_study_id from a study_id.

    Parameters:
        study_id (str)

    Returns:
        pdc_study_id (str)
    '''

    query = '''query {
        study (study_id: "%s" acceptDUA: true) {pdc_study_id}
    }''' % study_id

    data = _post(query, url, **kwargs)
    if len(data['data']['study']) > 0:
        return data["data"]["study"][0]["pdc_study_id"]
    return None


def study_id(pdc_study_id, url, **kwargs):
    '''
    Get study_id from a pdc_study_id.

    Parameters:
        pdc_study_id (str)

    Returns:
        study_id (str)
    '''

    query = '''query {
        study (pdc_study_id: "%s" acceptDUA: true) {study_id}
    }''' % pdc_study_id

    data = _post(query, url, **kwargs)
    if len(data['data']['study']) > 0:
        return data["data"]["study"][0]["study_id"]
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
        RuntimeError(f'More than 1 diagnoses in case_id: {case_id}')
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
        RuntimeError(f'Too many files in query for file_id: {file_id}')
    if len(r['data']['fileMetadata'][0]['aliquots']) != 1:
        RuntimeError(f'Too many aliquot IDs for file_id: {file_id}')
    return file_id, r['data']['fileMetadata'][0]['aliquots'][0]['aliquot_id']
    

def raw_files(study_id, url, n_files=None, **kwargs):
    ''' Get metadata for raw files in a study '''

    query = '''query {
       filesPerStudy (study_id: "%s" acceptDUA: true) {
            file_id
            file_name
            md5sum
            file_location
            file_size
            data_category
            file_type
            file_format
            signedUrl {url}} 
    }''' % study_id

    # get a list of .raw files in study
    payload = _post(query, url, **kwargs)
    if 'errors' in payload:
        sys.stderr.write('ERROR: API query failed with response(s):\n')
        for error in payload['errors']:
            sys.stderr.write('Code: {}\nEndpoint: {}\nMessage: {}\n'.format(error['extensions']['code'],
                                                                            ', '.join(error['path']),
                                                                            error['message']))
        return None

    keys = ('file_id', 'file_name', 'md5sum', 'file_location', 'file_size', 'data_category', 'file_type', 'file_format')
    data = list()
    file_count = 0
    for file in payload['data']['filesPerStudy']:
        if file['data_category'] == 'Raw Mass Spectra':
            if n_files is not None and file_count >= n_files:
                break

            newFile = {k: file[k] for k in keys}
            newFile['url'] = file['signedUrl']['url']
            data.append(newFile)
            file_count += 1

    return data


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

