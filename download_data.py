# local
import api_operations
import sql_operations
# standard
import json
import time
import logging
import sys


#####################
# Initialize logger #
#####################

logger = logging.getLogger("etis")
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.getLevelName("INFO"))


##########################
# Pull ETIS Publications #
##########################

publication_session = api_operations.PublicationSession()

publications = list()
i = 0
n = 500
limit = 10000
start_time = time.time()
while i < limit:
    publications += publication_session.get_items(n, i)
    i += n
    log_message = f"records pulled: {i}, time elapsed: {round((time.time() - start_time) / 60, 2)} minutes"
    logger.info(log_message)


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
