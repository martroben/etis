import json

def create_publication_node(transaction, **kwargs):
    property_placeholders_string = ", ".join([f"{key}: ${key}" for key in kwargs.keys()])
    cypher_pattern = f"CREATE (pub:Publication {{{property_placeholders_string}}}) RETURN id(pub)"
    values = dict()
    for key, value in kwargs.items():
        values[key] = json.dumps(value) if isinstance(value, (list, dict)) else value
    node_id = transaction.run(cypher_pattern, values).single().value()
    # Return id of the new node as verification
    return node_id

def delete_all(transaction):
    transaction.run("MATCH (n) DETACH DELETE n")

def get_all(transaction):
    result = transaction.run("MATCH (n) RETURN n")
    values = [record.values() for record in result]
    return values

def get_publications(transaction):
    result = transaction.run("MATCH (pub) RETURN pub.Guid, pub.Authors, pub.AuthorsText")
    values = [record.values() for record in result]
    return values