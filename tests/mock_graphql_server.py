
from flask import Flask
from flask_graphql import GraphQLView
import graphene

# Define a sample GraphQL schema
class User(graphene.ObjectType):
    id = graphene.ID()
    name = graphene.String()
    email = graphene.String()

# Query Type
class Query(graphene.ObjectType):
    user = graphene.Field(User, id=graphene.ID())

    def resolve_user(self, info, id):
        # Dummy data for testing
        return User(id=id, name="John Doe", email="john@example.com")


def main():
    # Create the schema
    schema = graphene.Schema(query=Query)

    # Set up Flask app
    app = Flask(__name__)

    # Add GraphQL endpoint
    app.add_url_rule(
        "/graphql",
        view_func=GraphQLView.as_view("graphql", schema=schema, graphiql=True)
    )

    app.run(debug=True, port=5000)

if __name__ == "__main__":
    main()