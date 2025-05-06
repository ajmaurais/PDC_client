
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
    study_submitter_id = graphene.String(name='study_submitter_id')
    analytical_fraction = graphene.String(name='analytical_fraction')
    experiment_type = graphene.String(name='experiment_type')
    cases_count = graphene.Int(name='cases_count')
    aliquots_count = graphene.Int(name='aliquots_count')


class Url(graphene.ObjectType):
    url = graphene.String(name='url')


class AliquotRunMetadata(graphene.ObjectType):
    aliquot_id = graphene.String(name='aliquot_id')
    aliquot_run_metadata_id = graphene.String(name='aliquot_run_metadata_id')


class StudyRunMetadata(graphene.ObjectType):
    study_run_metadata_id = graphene.ID(name='study_run_metadata_id')
    study_run_metadata_submitter_id = graphene.String(name='study_run_metadata_submitter_id')
    aliquot_run_metadata = graphene.List(AliquotRunMetadata, name='aliquot_run_metadata')


class ExperimentalMetadata(graphene.ObjectType):
    study_run_metadata = graphene.List(StudyRunMetadata, name='study_run_metadata')


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
    file_location = graphene.String(name='file_location')
    signedUrl = graphene.Field(Url, name='signedUrl')


class Aliquot(graphene.ObjectType):
    aliquot_id = graphene.ID(name='aliquot_id')
    analyte_type = graphene.String(name='analyte_type')


class FileMetadata(graphene.ObjectType):
    file_id = graphene.ID(name='file_id')
    study_run_metadata_id = graphene.String(name='study_run_metadata_id')
    aliquots = graphene.List(Aliquot, name='aliquots')
    file_name = graphene.String(name='file_name')
    file_type = graphene.String(name='file_type')
    file_format = graphene.String(name='file_format')
    data_category = graphene.String(name='data_category')
    md5sum = graphene.String(name='md5sum')
    file_size = graphene.String(name='file_size')


SAMPLE_STRING_KEYS = ['sample_id', 'sample_submitter_id', 'sample_type', 'tissue_type']

class Sample(graphene.ObjectType):
    sample_id = graphene.ID(name='sample_id')
    sample_submitter_id = graphene.String(name='sample_submitter_id')
    sample_type = graphene.String(name='sample_type')
    tissue_type = graphene.String(name='tissue_type')
    aliquots = graphene.List(Aliquot, name='aliquots')


class Demographics(graphene.ObjectType):
    demographic_id = graphene.ID(name='demographic_id')
    cause_of_death = graphene.String(name='cause_of_death')
    ethnicity = graphene.String()
    gender = graphene.String()
    race = graphene.String()
    year_of_birth = graphene.String(name='year_of_birth')
    year_of_death = graphene.String(name='year_of_death')
    vital_status = graphene.String(name='vital_status')


class CasesSamplesAliquots(graphene.ObjectType):
    case_id = graphene.ID(name='case_id')
    samples = graphene.List(Sample, name='samples')


class CaseDemographicsPerStudy(graphene.ObjectType):
    case_id = graphene.ID(name='case_id')
    demographics = graphene.List(Demographics, name='demographics')


class Pagination(graphene.ObjectType):
    count = graphene.Int(name='count')
    offset = graphene.Int(name='from')
    total = graphene.Int(name='total')


class PaginatedCasesSamplesAliquots(graphene.ObjectType):
    total = graphene.Int(name='total')
    casesSamplesAliquots = graphene.List(CasesSamplesAliquots,
                                         name='casesSamplesAliquots')
    pagination = graphene.Field(Pagination, name='pagination')


class PaginatedCaseDemographicsPerStudy(graphene.ObjectType):
    total = graphene.Int(name='total')
    caseDemographicsPerStudy = graphene.List(CaseDemographicsPerStudy,
                                             name='caseDemographicsPerStudy')
    pagination = graphene.Field(Pagination, name='pagination')
