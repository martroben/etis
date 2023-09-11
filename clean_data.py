import logging

# local
import sql_operations
# standard
import json
import sys
# external
import regex
import transliterate

#################
# Setup logging #
#################

logger = logging.Logger("etis")
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.getLevelName("INFO"))


###########################
# Pull data from database #
###########################

database_path = "./data.sql"
publications_raw_table = "PublicationRaw"

sql_connection = sql_operations.get_connection(database_path)
authors_sql = "SELECT Guid, Authors, AuthorsText FROM PublicationRaw"
publication_authors = sql_connection.cursor().execute(authors_sql).fetchall()

authors = []
for publication in publication_authors:
    authors += str.split(publication[2], ";")


##################################
# Transliterate cyrillic strings #
##################################

# Using transliterate package: https://pypi.org/project/transliterate/

transliteration_header_log = """

CYRILLIC NAMES TRANSLITERATED TO LATIN:
+==========================================+==========================================+
| original cyrillic                        | latin replacement                        |
+==========================================+==========================================+\
"""

logger.info(transliteration_header_log)

transliterate_ru = transliterate.get_translit_function("ru")
cyrillic_pattern = regex.compile(r"\p{IsCyrillic}")
authors_latin = list()
for author in authors:
    if cyrillic_pattern.search(author):
        original = author
        # Removing signs to avoid ' characters in transliteration
        author_signs_removed = regex.sub(r"[ьъ]", "", author, regex.IGNORECASE)
        author_latin = transliterate.translit(
            author_signs_removed,
            language_code='ru',
            reversed=True)
        authors_latin += [author_latin]

        transliteration_log = f"| {original :<40} | {author_latin :<40} |"
        logger.info(transliteration_log)
        logger.info(f"{'+' :-<43}{'+' :-<43}+")
    else:
        authors_latin += [author]
