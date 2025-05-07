
from os.path import splitext

import graphene

from .data import Data
from .models import Study, Url, StudyVersion, StudyCatalog, FilesPerStudy
from .models import ExperimentalMetadata, StudyRunMetadata, AliquotRunMetadata
from .models import FileMetadata, Aliquot
from .models import PaginatedCasesSamplesAliquots, CasesSamplesAliquots, Pagination
from .models import SAMPLE_STRING_KEYS, Sample
from .models import Demographics, PaginatedCaseDemographicsPerStudy, CaseDemographicsPerStudy

class QueryError(Exception):
    pass

# Query Type
class Query(graphene.ObjectType):
    api_data = Data()

    study = graphene.List(Study, id=graphene.ID(name='study_id'),
                          pdc_study_id=graphene.String(name='pdc_study_id'),
                          acceptDUA=graphene.Boolean())

    studyCatalog = graphene.List(StudyCatalog, id=graphene.ID(name='pdc_study_id'),
                                 acceptDUA=graphene.Boolean())

    filesPerStudy = graphene.List(FilesPerStudy, id=graphene.ID(name='study_id'),
                                  pdc_study_id=graphene.String(name='pdc_study_id'),
                                  data_category=graphene.String(name='data_category'),
                                  file_name=graphene.String(name='file_name'),
                                  file_type=graphene.String(name='file_type'),
                                  file_format=graphene.String(name='file_format'),
                                  acceptDUA=graphene.Boolean())

    fileMetadata = graphene.List(FileMetadata, id=graphene.ID(name='file_id'),
                                 acceptDUA=graphene.Boolean())

    experimentalMetadata = graphene.List(ExperimentalMetadata,
                                         id=graphene.ID(name='study_id'),
                                         study_submitter_id=graphene.String(name='study_submitter_id'))

    paginatedCasesSamplesAliquots = graphene.Field(PaginatedCasesSamplesAliquots,
                                                   id=graphene.ID(name='study_id'),
                                                   offset=graphene.Int(), limit=graphene.Int(),
                                                   acceptDUA=graphene.Boolean())

    paginatedCaseDemographicsPerStudy = graphene.Field(PaginatedCaseDemographicsPerStudy,
                                                       id=graphene.ID(name='study_id'),
                                                       offset=graphene.Int(), limit=graphene.Int(),
                                                       acceptDUA=graphene.Boolean())

    def resolve_study(self, info, id=None, pdc_study_id=None, acceptDUA=False):
        if not acceptDUA:
            raise RuntimeError('You must accept the DUA to access this data!')
        return [Study(**study) for study in Query.api_data.get_studies(study_id=id, pdc_study_id=pdc_study_id)]


    def resolve_experimentalMetadata(self, info, id=None, study_submitter_id=None):
        if id is None and study_submitter_id is None:
            raise RuntimeError('You must provide either a study_submitter_id or an id!')

        if study_submitter_id is not None and id is None:
            id = Query.api_data.get_study_id_by_submitter_id(study_submitter_id)

        study_data = Query.api_data.studies.get(id)
        if study_data is None:
            return []

        experiment_data = Query.api_data.experiments.get(id)

        exp_metadata = ExperimentalMetadata(study_run_metadata=[])
        for run in experiment_data:
            srm_id = run['study_run_metadata_id']
            ar_metadata = run['aliquot_run_metadata']
            ss_id = run['study_run_metadata_submitter_id']

            sr_metadata = StudyRunMetadata(study_run_metadata_id=srm_id,
                                           study_run_metadata_submitter_id=ss_id,
                                           aliquot_run_metadata=[AliquotRunMetadata(**a) for a in ar_metadata])
            exp_metadata.study_run_metadata.append(sr_metadata)

        return [exp_metadata]


    def resolve_studyCatalog(self, info, id, acceptDUA=False):
        if not acceptDUA:
            raise RuntimeError('You must accept the DUA to access this data!')

        if (study := Query.api_data.study_catalog.get(id)) is None:
            return []

        return [StudyCatalog(pdc_study_id=id,
                             versions=[StudyVersion(**version) for version in study['versions']])]


    def resolve_filesPerStudy(self, info, id=None,
                              data_category=None,
                              file_name=None,
                              file_type=None,
                              file_format=None,
                              acceptDUA=False):
        if not acceptDUA:
            raise RuntimeError('You must accept the DUA to access this data!')

        ret = []
        for file in Query.api_data.get_files_per_study(
                study_id=id,
                data_category=data_category,
                file_name=file_name,
                file_type=file_type,
                file_format=file_format
            ):
            ret.append(FilesPerStudy(**file, signedUrl=Url('file_does_not_exist')))

        if len(ret) == 0:
            raise QueryError(f"Incorrect string value: '{id}' for function uuid_to_bin")
        return ret


    def resolve_paginatedCasesSamplesAliquots(self, info, id, offset=0, limit=100,
                                              acceptDUA=False):
        if not acceptDUA:
            raise RuntimeError('You must accept the DUA to access this data!')

        total = Query.api_data.get_total_cases_per_study(id)
        ret = PaginatedCasesSamplesAliquots(total=total)

        casesSamplesAliquots = list()
        aliquots = dict()
        i = 0
        for case in Query.api_data.get_cases_per_study(id, offset):
            if i >= limit:
                break

            samples = list()
            for sample in case['samples'].values():
                samples.append(Sample(**{k: sample[k] for k in SAMPLE_STRING_KEYS}))
                aliquots = list()
                for aliquot in sample['aliquots']:
                    aliquots.append(Aliquot(**aliquot))
                samples[-1].aliquots = aliquots

            casesSamplesAliquots.append(CasesSamplesAliquots(case_id=case['case_id'], samples=samples))
            i += 1

        ret.casesSamplesAliquots = casesSamplesAliquots
        ret.pagination = Pagination(total=total, offset=offset, count=len(casesSamplesAliquots))
        return ret


    def resolve_fileMetadata(self, info, id, acceptDUA=False):
        if not acceptDUA:
            raise RuntimeError('You must accept the DUA to access this data!')

        if (file := Query.api_data.get_file_metadata(id)) is None:
            return None

        return [FileMetadata(file_id=id,
                             data_category=file['data_category'],
                             file_name=file['file_name'],
                             file_type=file['file_type'],
                             file_format=file['file_format'],
                             file_size=file['file_size'],
                             md5sum=file['md5sum'],
                             study_run_metadata_id=file['study_run_metadata_id'],
                             aliquots=[ Aliquot(**aliquot) for aliquot in file['aliquots'] ])]


    def resolve_paginatedCaseDemographicsPerStudy(self, info, id, offset=0, limit=100,
                                                  acceptDUA=False):
        if not acceptDUA:
            raise RuntimeError('You must accept the DUA to access this data!')

        total = Query.api_data.get_total_cases_per_study(id)
        ret = PaginatedCaseDemographicsPerStudy(total=total)

        caseDemographicsPerStudy = list()
        i = 0
        for case in Query.api_data.get_cases_per_study(id, offset):
            if i >= limit:
                break
            caseDemographicsPerStudy.append(
                CaseDemographicsPerStudy(case_id=case['case_id'],
                                         demographics=[Demographics(**case['demographics'])])
                )
            i += 1

        ret.caseDemographicsPerStudy = caseDemographicsPerStudy
        ret.pagination = Pagination(total=total, offset=offset,
                                    count=len(caseDemographicsPerStudy))
        return ret