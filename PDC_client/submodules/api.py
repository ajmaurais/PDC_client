
import requests
import json

_ENDPOINT ='https://proteomic.datacommons.cancer.gov/graphql'

def _post(query):
    r = requests.post(_ENDPOINT , json = {'query': query})
    if r.status_code == 200:
        return r.json()
    else:
        raise RuntimeError(f'Failed to retrieve failed with response code {r.status_code}!')
  

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


def metadata(study_id):
    ''' Get file metadata for a study '''

    query = '''query {
       filesPerStudy (study_id: "%s" acceptDUA: true) {
            file_id
            file_name
            md5sum
            file_location
            file_size
            data_category
            signedUrl {url}} 
    }''' % study_id

    payload = _post(query)

    keys = ('file_id', 'file_name', 'md5sum', 'file_location', 'file_size', 'data_category')
    data = list()
    for file in payload['data']['filesPerStudy']:
        newFile = {k: file[k] for k in keys}
        newFile['url'] = file['signedUrl']['url']
        data.append(newFile)

    return data


