import graphene

from .data import api_data
from .models import Study, Url, StudyVersion, StudyCatalog, FilesPerStudy
from .models import FileMetadata, Aliquot
from .models import PaginatedCasesSamplesAliquots, CasesSamplesAliquots, Pagination
from .models import SAMPLE_STRING_KEYS, Sample
from .models import Demographics, PaginatedCaseDemographicsPerStudy, CaseDemographicsPerStudy

class QueryError(Exception):
    pass

# Query Type
class Query(graphene.ObjectType):
    study = graphene.List(Study, id=graphene.ID(name='study_id'),
                          pdc_study_id=graphene.String(name='pdc_study_id'),
                          acceptDUA=graphene.Boolean())

    studyCatalog = graphene.List(StudyCatalog, id=graphene.ID(name='pdc_study_id'),
                                 acceptDUA=graphene.Boolean())

    filesPerStudy = graphene.List(FilesPerStudy, id=graphene.ID(name='study_id'),
                                  pdc_study_id=graphene.String(name='pdc_study_id'),
                                  data_category=graphene.String(name='data_category'),
                                  acceptDUA=graphene.Boolean())

    fileMetadata = graphene.List(FileMetadata, id=graphene.ID(name='file_id'),
                                 acceptDUA=graphene.Boolean())

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
        return [Study(**study) for study in api_data.get_studies(study_id=id, pdc_study_id=pdc_study_id)]


    def resolve_studyCatalog(self, info, id, acceptDUA=False):
        if not acceptDUA:
            raise RuntimeError('You must accept the DUA to access this data!')

        if (study := api_data.study_catalog.get(id)) is None:
            return []

        return [StudyCatalog(pdc_study_id=id,
                             versions=[StudyVersion(**version) for version in study['versions']])]


    def resolve_filesPerStudy(self, info, id=None, data_category=None, acceptDUA=False):
        if not acceptDUA:
            raise RuntimeError('You must accept the DUA to access this data!')

        ret = [FilesPerStudy(**file, signedUrl=Url('file_does_not_exist'))
               for file in api_data.get_files_per_study(study_id=id)]
        if len(ret) == 0:
            raise QueryError(f"Incorrect string value: '{id}' for function uuid_to_bin")
        return ret


    def resolve_paginatedCasesSamplesAliquots(self, info, id, offset=0, limit=100,
                                              acceptDUA=False):
        if not acceptDUA:
            raise RuntimeError('You must accept the DUA to access this data!')

        total = api_data.get_total_cases_per_study(id)
        ret = PaginatedCasesSamplesAliquots(total=total)

        casesSamplesAliquots = list()
        aliquots = dict()
        i = 0
        for case in api_data.get_cases_per_study(id, offset):
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
        ret.pagination = Pagination(total=total, offset=offset, size=i, count=len(casesSamplesAliquots))
        return ret


    def resolve_fileMetadata(self, info, id, acceptDUA=False):
        if not acceptDUA:
            raise RuntimeError('You must accept the DUA to access this data!')

        if (file := api_data.get_file_metadata(id)) is None:
            return None

        return [FileMetadata(file_id=id,
                             aliquots=[Aliquot(**aliquot) for aliquot in file['aliquots']])]


    def resolve_paginatedCaseDemographicsPerStudy(self, info, id, offset=0, limit=100,
                                                  acceptDUA=False):
        if not acceptDUA:
            raise RuntimeError('You must accept the DUA to access this data!')

        total = api_data.get_total_cases_per_study(id)
        ret = PaginatedCaseDemographicsPerStudy(total=total)

        caseDemographicsPerStudy = list()
        i = 0
        for case in api_data.get_cases_per_study(id, offset):
            if i >= limit:
                break
            caseDemographicsPerStudy.append(
                CaseDemographicsPerStudy(case_id=case['case_id'],
                                         demographics=[Demographics(**case['demographics'])])
                )
            i += 1

        ret.caseDemographicsPerStudy = caseDemographicsPerStudy
        ret.pagination = Pagination(total=total, offset=offset, size=i,
                                    count=len(caseDemographicsPerStudy))
        return ret