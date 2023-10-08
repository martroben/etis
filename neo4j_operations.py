from neo4j import GraphDatabase

neo4j_uri = str()
neo4j_user = str()
neo4j_password = str()

neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
neo4j_driver.verify_connectivity()

with neo4j_driver.session() as session:
    ...

############################################################


def add_person(driver, name):
    with driver.session() as session:
        # Caller for transactional unit of work
        return session.write_transaction(create_person_node, name)


# Simple implementation of the unit of work
def create_person_node(tx, name):
    return tx.run("CREATE (a:Person {name: $name}) RETURN id(a)", name=name).single().value()


# Alternative implementation, with timeout
@unit_of_work(timeout=0.5)
def create_person_node_within_half_a_second(tx, name):
    return tx.run("CREATE (a:Person {name: $name}) RETURN id(a)", name=name).single().value()