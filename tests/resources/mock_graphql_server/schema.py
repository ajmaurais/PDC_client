import graphene

from .data import api_data
from .models import Study, Url, StudyVersion, StudyCatalog, FilesPerStudy, FileMetadata

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

    aliquot = graphene.List(FileMetadata, id=graphene.ID(name='file_id'),
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
            return None
        return ret


    def resolve_aliquot(self, info, id, acceptDUA=False):
        if not acceptDUA:
            raise RuntimeError('You must accept the DUA to access this data!')
