
# local
import sql_operations
# standard
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
    "|".join([
        r"\s*appendix\s*",
        r",*\s*и\s+др",
        r",*\s*juhendaja",
        r",*\s*koostaja\s*",
        r",*\s+et\s+al",
        r"\s*DIRECT\s*",
        r"\s*Programme\s*",
        r"\s*Study\s*",
        r"\s*Group\s*",
        r"\s*küsitl\s*"]),
    regex.IGNORECASE)

authors_cleaned = list()
for uuid, authors, authors_string in publication_authors:
    # Remove all substrings in parentheses: "Peel, E. (Random text.)" --> "Peel, E. "
    authors_string = regex.sub(r"\s*\(.*?\)\s*", " ", authors_string)
    # Replace line breaks with name delimiters: "\r\n" --> ";"
    authors_string = regex.sub(r"\r\n|\n", ";", authors_string)
    # Replace ampersands with name delimiters: "Quick & Easy" --> "Quick; Easy"
    authors_string = regex.sub(r"\s*&\s*", "; ", authors_string)
    # Replace "and" or "with" by name delimiter: "Quick and Easy" --> "Quick; Easy"
    authors_string = regex.sub(r"\s+(and|with)\s+", "; ", authors_string)
    # Remove all digits and periods following digits: " 1990. " --> " "
    authors_string = regex.sub(r"\s*\d+\.*\s*", " ", authors_string)
    # Replace ellipsis with name delimiteres: "Thomas,  D.. ..." --> "Thomas,  D. ;"
    authors_string = regex.sub(r"(?!\s\p{Lu})\.\.*\s+\.{3}\.*", ". ;", authors_string)
    # Remove whitespace from inbetween initials
    authors_string = regex.sub(r"(\p{Lu}\.)\s+(-?\p{Lu}\.)", "\g<1>\g<2>", authors_string)
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
+------------------------------------------+------------------------------------------+
| original cyrillic                        | latin replacement                        |
+------------------------------------------+------------------------------------------+\
"""
###################### Don't use separators between rows
###################### Save changes as tuples or dicts to a namespace variable. Log after cycle
############## globals()["_log_transliterated"]
# if globals().get("initial"):
#     print("jah")


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

    authors_latin += [(uuid, authors, authors_string)]

logger.info(f"{'+' :-<53}{'+' :-<53}+")


###############
# Parse names #
###############

authors = list()
for _, _, authors_string in authors_latin:
    authors += authors_string.split(";")


# Maybe?:
# Ess , Margus  <-- remove whitespace before comma
# De Pascale, Stefania, de Tommasi, Nunziatina
# van Name, von der Gathen, P., van de Vijver, Ewijk, H. van, Van der Auwera, L., von Dossow, V., Hellermann, Dorothee von
# La Torre, M.
# 'E.Eltang, A.Kabakov, G.Kanevskij, M.Gofaisen'
# 'National Contact Points for the ECD, Collective', 'Hospital Contact Points for the ECD, Collective'
# O’Hea, Brendan
# 'Surname , Plekhanov', 'Firstname , Vladimir'
# Make russian initials only one letter
# 'Riet, Ene küsitl', 'Kruus, Ülle jne', Vasama, J. jt., Haljasorg, Heiki jt

name = r"\p{Lu}[\p{L}'-]+"
initial = r"(\p{Lu}\.*-*){1,2}"

# Name, initial (e.g. Dickus, B.)
name_initial = f"{name}[,\s]\s*{initial}"
# Initial, name (e.g. F.G. Superman)
initial_name = f"{initial}[,\s]\s*{name}"
# Full name (e.g. David Thomas Shore)
full_name = f"{name}\s+{name}(\s+{name})?"
# Last name, first name (e.g. Shore, David Thomas)
last_first = f"{name},\s*{name}(\s*{name})?"
# Single last name, first name (e.g. Lewis, Fiona)
last_first_single = f"{name},\s*{name}"

def parse_patterns(string, pattern):
    matches = []
    while match := regex.search(pattern, string):
        matches += match.captures()
        string = string[match.span()[1]:]
    # Check if there is anything left unparsed
    residue = regex.sub(r"[\.,\s]", "", string)
    if len(residue) > 0:
        return list()
    return matches


yes_match = list()
no_match = list()
for author in authors:
    author = author.strip()
    # Remove trailing and leading commas
    author = regex.sub("^\s*,+|,+\s*$", "", author)
    # Trim consecutive whitespaces
    author = regex.sub("\s{2,}", " ", author)
    # Detect strings that have a single known name format
    if regex.match(f"^{name_initial}$", author):
        yes_match += [author]
    elif regex.match(f"^{initial_name}$", author):
        yes_match += [author]
    elif regex.match(f"^{full_name}$", author):
        yes_match += [author]
    elif regex.match(f"^{last_first}$", author):
        yes_match += [author]
    # Detect the first name pattern and capture the split character
    elif regex.match(f"^{name_initial}[\s,]+{name_initial}", author):
        if parsed := parse_patterns(author, name_initial):
            yes_match += parsed
        else:
            no_match += [author]
    elif regex.match(f"^{initial_name}[\s,]+{initial_name}", author):
        if parsed := parse_patterns(author, initial_name):
            yes_match += parsed
        else:
            no_match += [author]
    elif regex.match(f"^{full_name}[\s,]+{full_name}", author):
        if parsed := parse_patterns(author, full_name):
            yes_match += parsed
        else:
            no_match += [author]
    elif regex.match(f"^{last_first}[\s,]+{last_first}", author):
        if parsed := parse_patterns(author, last_first_single):
            yes_match += parsed
        else:
            no_match += [author]
    else:
        no_match += [author]



regex.match(f"^{name_initial}[\s,]{name_initial}", "Liiv, Juhan Runnel, Hando")
regex.match(f"^{initial_name}[\s,]+{initial_name}", "Liiv, Juhan Runnel, Hando")
regex.match(f"^{full_name}[\s,]+{full_name}", "Liiv, Juhan Runnel, Hando")
regex.match(f"^{last_first}[\s,]+{last_first}", "Liiv, Juhan Runnel, Hando")


parse_patterns("Liiv, Juhan Runnel, Hando", last_first_single)
