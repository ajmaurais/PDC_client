import json
from typing import Generator
import logging

from flask import Flask, request, jsonify
from flask_graphql import GraphQLView
import graphene

import setup_tests

logging.basicConfig(
    level=logging.WARNING ,
    format='%(asctime)s - %(filename)s %(funcName)s:%(lineno)d - %(levelname)s: %(message)s'
)
LOGGER = logging.getLogger()

class Data:
    def __init__(self):
        # read study metadata
        with open(setup_tests.STUDY_METADATA, 'r', encoding='utf-8') as inF:
            study_data = json.load(inF)
        self.studies = {study['study_id']: study for study in study_data}

        # read study catalog
        with open(setup_tests.STUDY_CATALOG, 'r', encoding='utf-8') as inF:
            self.study_catalog = json.load(inF)
            for study in self.study_catalog.values():
                for version in study['versions']:
                    version['is_latest_version'] = 'yes' if version['is_latest_version'] else 'no'


        # read file metadata
        with open(setup_tests.FILE_METADATA, 'r', encoding='utf-8') as inF:
            file_per_study = json.load(inF)
        # rearange study_id and pdc_study_id keys
        self.files_per_study = dict()
        for pdc_study_id, files in file_per_study.items():
            study_id = self.get_study_id(pdc_study_id)
            self.files_per_study[study_id] = list()
            for file in files:
                file['pdc_study_id'] = pdc_study_id
                self.files_per_study[study_id].append(file)


    def get_study_id(self, pdc_study_id):
        '''
        Retrieve the study_id based on the provided pdc_study_id.

        Args:
            pdc_study_id (str): The PDC study ID.
        Returns:
            str: The study ID if found, otherwise None.
        '''
        for study in self.studies.values():
            if study['pdc_study_id'] == pdc_study_id:
                return study['study_id']
        return None


    def get_studies(self, study_id=None, pdc_study_id=None) -> Generator[dict, None, None]:
        '''
        Retrieve study information based on study_id or pdc_study_id.

        Args:
            study_id (str, optional): The ID of the study to retrieve. Defaults to None.
            pdc_study_id (str, optional): The PDC study ID to retrieve. Defaults to None.

        Yields:
            dict: A dictionary containing the study information if found, otherwise None.
        '''
        if pdc_study_id is not None or study_id is not None:
            if study_id is None:
                study_id = self.get_study_id(pdc_study_id)
            if study_id in self.studies:
                yield self.studies[study_id]
        else:
            yield from self.studies.values()


    def get_files_per_study(self, study_id=None,
                            pdc_study_id=None,
                            data_category=None) -> Generator[dict, None, None]:
        '''
        Retrieve file information based on study_id or pdc_study_id.

        Args:
            study_id (str, optional): The ID of the study to retrieve. Defaults to None.
            pdc_study_id (str, optional): The PDC study ID to retrieve. Defaults to None.
            data_category (str, optional): The data category to filter files. Defaults to None.

        Yields:
            dict: A dictionary containing the file information if found, otherwise None.
        '''
        match sum(v is not None for v in (study_id, pdc_study_id)):
            case 0:
                for study in self.files_per_study.values():
                    for file in study:
                        if data_category is None or file['data_category'] == data_category:
                            yield file
            case 1:
                if pdc_study_id is not None:
                    study_id = self.get_study_id(pdc_study_id)
                    if study_id is None:
                        return None

                study = self.files_per_study.get(study_id)
                if study is None:
                    return None

                for file in study:
                    if data_category is None or file['data_category'] == data_category:
                        yield file
            case _:
                raise ValueError('Both study_id and pdc_study_id cannot be provided!')


api_data = Data()


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


    def resolve_study(self, info, id=None, pdc_study_id=None, acceptDUA=False):
        if not acceptDUA:
            raise RuntimeError('You must accept the DUA to access this data!')
        # return [Study(**study) for study in api_data.get_studies(study_id=id, pdc_study_id=pdc_study_id)]
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


def get_server():
    # Create the schema
    schema = graphene.Schema(query=Query)

    # Set up Flask app
    app = Flask(__name__)

    # Add GraphQL endpoint
    app.add_url_rule(
        "/graphql",
        view_func=GraphQLView.as_view("graphql", schema=schema, graphiql=True)
    )

    @app.route('/graphql', methods=['GET'])
    def graphql_get():
        query = request.args.get('query')
        try:
            result = schema.execute(query)
            return jsonify(result.data)
        except Exception as e:
            response = jsonify(f'Unexpected error occurred: {e}')
            response.status_code = 500

    @app.route('/graphql/filesPerStudy', methods=['POST'])
    def graphql_files_per_study():
        query = request.json.get('query')
        try:
            result = schema.execute(query)
            return jsonify(result.data)
        except Exception as e:
            response = jsonify(f'Unexpected error occurred: {e}')
            response.status_code = 500
            return response

    # app.run(debug=True, port=5000)
    return app

if __name__ == "__main__":
    app = get_server()
    app.run(debug=True, port=5000)
