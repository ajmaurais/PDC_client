
import requests
import json
import re
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count

MAX_THREADS = cpu_count()
_URL ='https://proteomic.datacommons.cancer.gov/graphql'

def _post(query, retries=5):
    for i in range(retries):
        r = requests.post(_URL, json = {'query': query})
        if r.status_code == 200:
            return r.json()
    raise RuntimeError(f'Failed with response code {r.status_code}!')


def _get(query, retries=5):
    query = re.sub('\s+', ' ', query)
    for i in range(retries):
        r = requests.get(f'{_URL}?{query}')
        if r.status_code == 200:
            return r.json()
    print(f'url:\n"{_URL}?{query}"')
    raise RuntimeError(f'Failed with response code {r.status_code}!')
  

def study_id(pdc_study_id):
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

    data = _post(query)
    return data["data"]["study"][0]["study_id"]


def _get_paginated_data(query_f, data_name, study_id, page_limit=10, no_change_iterations_limit=2):

    ret = list()
    done = False
    total_entries = None
    entry_i = 0
    offset = 0
    endpoint_name = 'paginated{}{}'.format(data_name[0].upper(), data_name[1:])
    no_change_iterations = 0
    previous_len = 0
    
    while not done:
        data = _get(query_f(study_id, offset, page_limit))
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
            done = True

        # check that we are not in an infinite loop
        if len(ret) == previous_len:
            no_change_iterations += 1
        previous_len = len(ret)
        if no_change_iterations >= no_change_iterations_limit:
            raise RuntimeError('Something is wrong...')

    return ret


def case_metadata(study_id, max_threads=MAX_THREADS):

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
    
    data = _get_paginated_data(make_case_query, 'caseDemographicsPerStudy', study_id)

    keys = ['ethnicity', 'gender', 'race', 'cause_of_death', 'vital_status', 'year_of_birth', 'year_of_death']
    
    with ThreadPoolExecutor(max_workers=max_threads) as pool:
        case_data = dict(pool.map(single_case, set([d['case_id'] for d in data])))

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


def single_case(case_id):

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

    r = _get(query)
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


def aliquot_id(file_id):
    '''
    Get the aliquot ID for a file.
    '''

    query = '''query={
    fileMetadata (file_id: "%s" acceptDUA: true) {
        aliquots { aliquot_id } }
        }''' % file_id

    r = _get(query)
    if len(r['data']['fileMetadata']) != 1:
        RuntimeError(f'Too many files in query for file_id: {file_id}')
    if len(r['data']['fileMetadata'][0]['aliquots']) != 1:
        RuntimeError(f'Too many aliquot IDs for file_id: {file_id}')
    return file_id, r['data']['fileMetadata'][0]['aliquots'][0]['aliquot_id']
    

def metadata(study_id, nFiles=None, max_threads=MAX_THREADS):
    ''' Get file metadata for a study '''

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

    payload = _post(query)

    keys = ('file_id', 'file_name', 'md5sum', 'file_location', 'file_size', 'data_category', 'file_type', 'file_format')
    data = list()
    file_count = 0
    for file in payload['data']['filesPerStudy']:
        if file['data_category'] == 'Raw Mass Spectra':
            if nFiles is not None and file_count >= nFiles:
                break

            newFile = {k: file[k] for k in keys}
            newFile['url'] = file['signedUrl']['url']
            data.append(newFile)
            file_count += 1

    cases = case_metadata(study_id, max_threads=max_threads)
    with ThreadPoolExecutor(max_workers=max_threads) as pool:
        aliquot_ids = dict(pool.map(aliquot_id, set([f['file_id'] for f in data])))

    for file in data:
        file['aliquot_id'] = aliquot_ids[file['file_id']]

    return data


