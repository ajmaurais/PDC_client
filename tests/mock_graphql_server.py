import json
from flask import Flask, request
from flask_graphql import GraphQLView
import graphene

import setup_tests


class Data:
    def __init__(self):
        with open(setup_tests.STUDY_METADATA, 'r', encoding='utf-8') as inF:
            study_data = json.load(inF)

        self.studies_by_id = {study['study_id']: study for study in study_data}
        self.study_ids_by_pdc_id = {study['pdc_study_id']: study['study_id'] for study in study_data}


    def get_study(self, study_id):
        return self.studies_by_id.get(study_id, None)


    def get_study_id(self, pdc_study_id):
        return self.study_ids_by_pdc_id.get(pdc_study_id, None)


api_data = Data()


class Study(graphene.ObjectType):
    study_id = graphene.ID(name='study_id')
    study_name = graphene.String(name='study_name')
    analytical_fraction = graphene.String(name='analytical_fraction')
    experiment_type = graphene.String(name='experiment_type')
    cases_count = graphene.Int(name='cases_count')
    aliquots_count = graphene.Int(name='aliquots_count')


# Query Type
class Query(graphene.ObjectType):
    study = graphene.Field(Study, id=graphene.ID())

    def resolve_study(self, info, id):
        study_data = api_data.get_study(id)
        if study_data:
            return Study(
                study_id=study_data['study_id'],
                study_name=study_data['study_name'],
                analytical_fraction=study_data['analytical_fraction'],
                experiment_type=study_data['experiment_type'],
                cases_count=study_data['cases_count'],
                aliquots_count=study_data['aliquots_count']
            )
        return None


# def main():
def run_server():
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

# if __name__ == "__main__":
#     main()

app = run_server()
client = app.test_client()

r = client.get('/graphql?query={__schema{queryType{name}}}')

study_query = 'query={ study(id: "eb6aae30-9b42-4fe1-b3ed-22b55d730dfa") { study_id study_name analytical_fraction experiment_type cases_count aliquots_count } }'

r2 = client.get(f'/graphql?{study_query}')

print(r2.text)