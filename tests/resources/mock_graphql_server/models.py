
import graphene

class StudyVersion(graphene.ObjectType):
    study_id = graphene.String(name='study_id')
    is_latest_version = graphene.String(name='is_latest_version')


class StudyCatalog(graphene.ObjectType):
    pdc_study_id = graphene.ID(name='pdc_study_id')
    versions = graphene.List(StudyVersion, name='versions')


class Study(graphene.ObjectType):
    study_id = graphene.ID(name='study_id')
    pdc_study_id = graphene.String(name='pdc_study_id')
    study_name = graphene.String(name='study_name')
    analytical_fraction = graphene.String(name='analytical_fraction')
    experiment_type = graphene.String(name='experiment_type')
    cases_count = graphene.Int(name='cases_count')
    aliquots_count = graphene.Int(name='aliquots_count')


class Url(graphene.ObjectType):
    url = graphene.String(name='url')


class FilesPerStudy(graphene.ObjectType):
    study_id = graphene.ID(name='study_id')
    pdc_study_id = graphene.String(name='pdc_study_id')
    file_id = graphene.String(name='file_id')
    file_name = graphene.String(name='file_name')
    file_submitter_id = graphene.String(name='file_submitter_id')
    md5sum = graphene.String(name='md5sum')
    file_size = graphene.String(name='file_size')
    data_category = graphene.String(name='data_category')
    file_type = graphene.String(name='file_type')
    file_format = graphene.String(name='file_format')
    signedUrl = graphene.Field(Url, name='signedUrl')


class Aliquot(graphene.ObjectType):
    aliquot_id = graphene.ID(name='aliquot_id')


class FileMetadata(graphene.ObjectType):
    file_id = graphene.ID(name='file_id')
    aliquots = graphene.List(Aliquot, name='aliqupts')
