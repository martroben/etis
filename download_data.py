# local
import api_operations
import log
import sql_operations
# standard
import json
import time
import logging
import sys
# external
import tqdm


#####################
# Initialize logger #
#####################

logger = logging.getLogger("etis")
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.getLevelName("INFO"))


##########################
# Pull ETIS Publications #
##########################

base_url = "https://www.etis.ee:2346/api/"
publication_session = api_operations.PublicationSession(base_url)

log_message_frequency_cycles = 20
i = 0
items_per_request = 500
limit = 10000
publications = list()
start_time = time.time()
with tqdm.tqdm(total=limit/items_per_request) as progress_bar:
    while i < limit:
        items = publication_session.get_items(items_per_request, i)
        publications += items
        i += items_per_request
        _ = progress_bar.update()
        if i % (log_message_frequency_cycles * items_per_request) == 0:
            log.api_result(i, start_time, logging.getLogger("etis"))
        if not items:
            break

log.api_result(i, start_time, logging.getLogger("etis"))


############################
# Save publications to SQL #
############################

database_path = "./data.sql"
settings_path = "./settings.json"
publications_raw_table = "PublicationRaw"

# Create Publication table
with open(settings_path) as settings_file:
    settings = json.loads("\n".join(settings_file.readlines()))

sql_connection = sql_operations.get_connection(database_path)
sql_operations.create_table(
    table=publications_raw_table,
    columns=settings["publication_columns"],
    connection=sql_connection)

succeeded_rows = 0
for row in publications:
    succeeded_rows += sql_operations.insert_row(
        table=publications_raw_table,
        connection=sql_connection,
        **row)

sql_connection.commit()


##############################
# Save publications to neo4j #
##############################

from neo4j import GraphDatabase

neo4j_uri = str()
neo4j_user = str()
neo4j_password = str()

neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
neo4j_driver.verify_connectivity()

def create_publication_node(transaction, **kwargs):
    properties_string = ", ".join([f"{key}: ${key}" for key in kwargs.keys()])
    cypher_pattern = f"CREATE (pub:Publication {properties_string}) RETURN id(a)"
    # Return id of the new node as verification
    node_id = transaction.run(cypher_pattern, **kwargs).single().value()
    return node_id

with neo4j_driver.session() as session:
    for pub in publications:
        session.write_transaction(create_publication_node, pub)
