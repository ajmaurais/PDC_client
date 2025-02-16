import json
from typing import Generator

from flask import Flask, request
from flask_graphql import GraphQLView
import graphene

import setup_tests


class Data:
    def __init__(self):
        with open(setup_tests.STUDY_METADATA, 'r', encoding='utf-8') as inF:
            study_data = json.load(inF)

        self.studies = {study['study_id']: study for study in study_data}

        with open(setup_tests.STUDY_CATALOG, 'r', encoding='utf-8') as inF:
            self.study_catalog = json.load(inF)
            for study in self.study_catalog.values():
                for version in study['versions']:
                    version['is_latest_version'] = 'yes' if version['is_latest_version'] else 'no'


    def get_studies(self, study_id=None, pdc_study_id=None) -> Generator[dict, None, None]:
        '''
        Retrieve study information based on study_id or pdc_study_id.

        Args:
            study_id (str, optional): The ID of the study to retrieve. Defaults to None.
            pdc_study_id (str, optional): The PDC study ID to retrieve. Defaults to None.

        Yields:
            dict: A dictionary containing the study information if found, otherwise None.
        '''
        if study_id is not None:
            if study_id in self.studies:
                yield self.studies[study_id]
        elif pdc_study_id is not None:
            for study in self.studies.values():
                if study['pdc_study_id'] == pdc_study_id:
                    yield study
        else:
            yield from self.studies.values()


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


# Query Type
class Query(graphene.ObjectType):
    study = graphene.List(Study, id=graphene.ID(name='study_id'),
                          pdc_study_id=graphene.String(name='pdc_study_id'),
                          acceptDUA=graphene.Boolean())

    studyCatalog = graphene.List(StudyCatalog, id=graphene.ID(name='pdc_study_id'),
                                 acceptDUA=graphene.Boolean())


    def resolve_study(self, info, id=None, pdc_study_id=None, acceptDUA=False):
        if not acceptDUA:
            raise RuntimeError('You must accept the DUA to access this data!')
        return [Study(**study) for study in api_data.get_studies(study_id=id, pdc_study_id=pdc_study_id)]


    def resolve_studyCatalog(self, info, id, acceptDUA=False):
        if not acceptDUA:
            raise RuntimeError('You must accept the DUA to access this data!')

        if (study := api_data.study_catalog[id]) is None:
            return None

        return [StudyCatalog(pdc_study_id=id,
                             versions=[StudyVersion(**version) for version in study['versions']])]


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
        result = schema.execute(query)
        return json.dumps(result.data)

    # app.run(debug=True, port=5000)
    return app

if __name__ == "__main__":
    app = get_server()
    app.run(debug=True, port=5000)