
import json

import setup_tests


def get_study_id(pdc_study_id, **kwargs):
    with open(setup_tests.STUDY_METADATA, 'r', encoding='utf-8') as inF:
        data = json.load(inF)

    for study in data:
        if study['pdc_study_id'] == pdc_study_id:
            return study['study_id']

        return None


def get_study_metadata(pdc_study_id=None, study_id=None, **kwargs):
    if pdc_study_id is not None:
        _id = pdc_study_id
        id_name = 'pdc_study_id'
    elif study_id is not None:
        _id = study_id
        id_name = 'study_id'
    else:
        raise ValueError('Both pdc_study_id and study_id cannot be None!')

    with open(setup_tests.STUDY_METADATA, 'r', encoding='utf-8') as inF:
        data = json.load(inF)

    for study in data:
        if study[id_name] == _id:
            return study

        return None


def get_pdc_study_id(study_id, **kwargs):
    with open(setup_tests.STUDY_METADATA, 'r', encoding='utf-8') as inF:
        data = json.load(inF)

    for study in data:
        if study['study_id'] == study_id:
            return study['pdc_study_id']

        return None


def get_study_name(study_id, **kwargs):
    with open(setup_tests.STUDY_METADATA, 'r', encoding='utf-8') as inF:
        data = json.load(inF)

    for study in data:
        if study['study_id'] == study_id:
            return study['study_name']

        return None


def get_study_raw_files(study_id, **kwargs):
    with open(setup_tests.FILE_METADATA, 'r', encoding='utf-8') as inF:
        data = json.load(inF)

    pdc_study_id = get_pdc_study_id(study_id)
    return data.get(pdc_study_id, None)


def get_study_aliquots(study_id, **kwargs):
    with open(setup_tests.ALIQUOT_METADATA, 'r', encoding='utf-8') as inF:
        data = json.load(inF)

    pdc_study_id = get_pdc_study_id(study_id)
    return data.get(pdc_study_id, None)


def get_study_cases(study_id, **kwargs):
    with open(setup_tests.CASE_METADATA, 'r', encoding='utf-8') as inF:
        data = json.load(inF)

    pdc_study_id = get_pdc_study_id(study_id)
    return data.get(pdc_study_id, None)
