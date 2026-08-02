"""
GraphQL API gateway placeholder.

This will host the GraphQL schema and map mutations to Nuts ES commands,
and queries to projection read models.
"""


def create_gateway():
    raise NotImplementedError("GraphQL gateway wiring is pending.")


def gateway_services():
    return [
        "iam",
        "profiles",
        "projects",
        "tasks",
        "hiring",
        "notifications",
    ]
