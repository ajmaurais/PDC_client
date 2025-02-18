
from flask import Flask, request, jsonify
from flask_graphql import GraphQLView
import graphene

from .schema import Query
from .schema import QueryError

def get_server():
    # Create the schema
    schema = graphene.Schema(query=Query, auto_camelcase=False)

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
            if result.errors:
                raise QueryError(result.errors)
            return jsonify(result.data)
        except QueryError:
            response = jsonify({
                'data': result.data,
                'errors': [{
                    'extensions': {'code': 'INTERNAL_SERVER_ERROR'},
                    'path': request.path,
                    'message': 'An error that I have set POOP'
                } for e in result.errors]
            })
            return response
        except Exception as e:
            response = jsonify(f'Unexpected error occurred: {e}')
            response.status_code = 500
            return response

    return app


if __name__ == "__main__":
    app = get_server()
    app.run(debug=True, port=5000)