
# local
import sql_operations
# standard
import json
import logging
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


#################
# Clean strings #
#################

discard_substrings = regex.compile(
    r"\s*appendix\s*|,*\s*и\s+др|,*\s*juhendaja|,*\s*koostaja\s*|,*\s+et\s+al\s*",
    regex.IGNORECASE)

authors_cleaned = list()
for uuid, authors, authors_string in publication_authors:
    # Remove all substrings in parentheses: "Peel, E. (Random text.)" --> "Peel, E. "
    authors_string = regex.sub(r"\s*\(.*?\)\s*", " ", authors_string)
    # Replace line breaks with name delimiters: "\r\n" --> ";"
    authors_string = regex.sub(r"\r\n|\n", ";", authors_string)
    # Replace ampersands with name delimiters: "Quick & Easy" --> "Quick; Easy"
    authors_string = regex.sub(r"\s*&\s*", "; ", authors_string)
    # Replace "and" with name delimiter: "Quick and Easy" --> "Quick; Easy"
    authors_string = regex.sub(r"\s+and\s+", "; ", authors_string)
    # Remove all digits and periods following digits: " 1990. " --> " "
    authors_string = regex.sub(r"\s*\d+\.*\s*", " ", authors_string)
    # Replace ellipsis with name delimiteres: "Thomas,  D.. ..." --> "Thomas,  D. ;"
    authors_string = regex.sub(r"(?!\s\p{Lu})\.\.*\s+\.{3}\.*", ". ;", authors_string)
    # Remove all remaining instances of several periods in a row: "...." --> ""
    authors_string = regex.sub(r"\s*\.{2,}\s*", " ", authors_string)
    # Remove unwanted substrings
    authors_string = discard_substrings.sub(" ", authors_string)

    authors_cleaned += [(uuid, authors, authors_string)]


##################################
# Transliterate cyrillic strings #
##################################

# Using transliterate package: https://pypi.org/project/transliterate/

transliterate_ru = transliterate.get_translit_function("ru")
cyrillic_pattern = regex.compile(r"\p{IsCyrillic}")
transliteration_header_log = """\n
CYRILLIC NAMES TRANSLITERATED TO LATIN:
+==========================================+==========================================+
| original cyrillic                        | latin replacement                        |
+==========================================+==========================================+\
"""

authors_latin = list()
logger.info(transliteration_header_log)
for uuid, authors, authors_string in authors_cleaned:
    if cyrillic_pattern.search(authors_string):
        original = authors_string
        # Remove sign characters to avoid ' characters in transliteration
        authors_string = regex.sub(r"[ьъ]", "", authors_string, regex.IGNORECASE)
        authors_string = transliterate_ru(authors_string, reversed=True)

        transliteration_log = f"| {original :<50} | {authors_string :<50} |"
        logger.info(transliteration_log)
        logger.info(f"{'+' :-<53}{'+' :-<53}+")

    authors_latin += [(uuid, authors, authors_string)]


###############
# Parse names #
###############

authors = list()
for _, _, authors_string in authors_latin:
    authors += authors_string.split(";")

# regex.search("^[\pL-]+\.*,\s*[\pL-]+\.*$", author)
# regex.search("[^,]*,[^,]*,.*", "")

# Maybe?:
# Tiiu Kuurme, Gertrud Kasemaa, Elo-Maria Roots
# Sotgiu G, D'Ambrosio L, Centis R
# Migliori GB, Zellweger JP, Abubakar I
# Liuhto, K. Sõrg, M.
# Spiridonov A., Brazauskas A., Radzevičius S.


yes_match = list()
no_match = list()
for author in authors:
    author = author.strip()
    author = regex.sub("^\s*,|,\s*$", "", author)
    author = regex.sub("\s{2,}", " ", author)
    # Handle cases where names are sepparated by commas ("Chapajev, V., Pustota, P.")
    if regex.search("[^,]*,[^,]*,.*", author):
        split_by_commas = author.split(",")
        if (len(split_by_commas) % 2 == 0):
            name_element_iterator = iter(split_by_commas)
            yes_match += [name_element + ", " + next(name_element_iterator, "") for name_element in name_element_iterator]
        else:
            no_match += [author]
