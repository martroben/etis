# local
import data_operations
import sql_operations
import log
# standard
import json
import logging
import sys
# external
import Levenshtein
import regex

#################
# Setup logging #
#################

logger = logging.getLogger("etis")
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


#####################################
# Parse authors from authors string #
#####################################

unwanted_substrings = [
    r"\s*appendix\s*",
    r",*\s*и\s+др",
    r"\s*\bteksti\sautor\b\s*",
    r"\s*\bautor\b\s*",
    r",*\s*juhendaja",
    r",*\s*koostaja\s*",
    r",*\s*koostanud\s*",
    r",*\s+et\s+al\.?",
    r"\s*DIRECT\s*",
    r"\s*Programme\s*",
    r"\s*Study\s*",
    r"\s*Group\s*",
    r"\s*\bküsitl\.?\s*",
    r"\s*tõlkija\s*",
    r"\s*tõlge\s.+?\skeelest\s*",
    r"\s*\bkoost\.\s*",
    r"\s*surname\s*",
    r"\s*firstname\s*",
    r"\s*\bjne\b\.?",
    r"\s*\bjt\b\.?"]

# Parsing patterns
prefix = r"|".join([
"[Vv]an\s[Dd]er",
"[Vv]an",
"[Vv]on",
"[Ll]a",
"[Dd]e"])
name = r"\p{Lu}[\p{L}'’\-\—]+"
initial = r"(\p{Lu}\.*\-*\—*){1,2}(?!\p{Ll})"

# Name, initial (e.g. Dickus, B.)
name_initial = rf"({prefix})?\s?{name}[,\s]\s*{initial}"
# Initial, name (e.g. F.G. Superman)
initial_name = rf"{initial}[,\s]*\s*({prefix})?\s?{name}"
# Full name (e.g. David Thomas Shore)
full_name = rf"{name}\s+({prefix})?\s?{name}(\s+{name})?"
# Last name, first name (e.g. Shore, David Thomas)
last_first = rf"({prefix})?\s?{name},\s*{name}(\s*{name})?"
# Single last name, first name (e.g. Lewis, Fiona)
last_first_single = rf"({prefix})?\s?{name},\s*{name}"

patterns_extract = [
    name_initial,
    initial_name,
    full_name,
    last_first]

# tuple: (detect_pattern, corresponding extract pattern)
patterns_detect = [
    (name_initial, name_initial),
    (initial_name, initial_name),
    (full_name, full_name),
    (last_first_single, last_first)]


authors_cleaner = data_operations.AuthorsCleaner(
    delimiter=";",
    unwanted_substrings=unwanted_substrings)

author_parser = data_operations.AuthorParser(
    patterns_extract=patterns_extract,
    patterns_detect=patterns_detect,
    secondary_delimiters=[r"\s", ","])

globals()["log_latinized"] = list()
globals()["log_parse_fail"] = list()

publication_authors_parsed = list()
for id, authors, authors_string in publication_authors:
    # clean for splitting
    authors_string_cleaned = authors_cleaner.clean_delimiter(authors_string)
    # latinize
    authors_latinized = authors_cleaner.latinize(authors_string_cleaned)
    if authors_latinized != authors_string_cleaned and "log_latinized" in globals():
        globals()["log_latinized"] += [(id, authors_string_cleaned, authors_latinized)]
    # split
    authors_split = authors_cleaner.split(authors_latinized)
    # remove unwanted substrings
    authors_substrings_removed = [authors_cleaner.remove_substrings(author) for author in authors_split]
    # clean split strings
    authors_cleaned = [authors_cleaner.clean(author) for author in authors_substrings_removed]

    # parse authors
    exact_matches = [author_parser.check_exact_match(author) for author in authors_cleaned]
    authors_parsed = [author for (author, is_exact_match) in zip(authors_cleaned, exact_matches) if is_exact_match]
    if not_parsed_authors := [author for (author, is_exact_match) in zip(authors_cleaned, exact_matches) if not is_exact_match]:
        for author in not_parsed_authors:
            successful_parse = author_parser.parse_bad_delimiter(author)
            if not successful_parse:
                parsed_authors = list()
                break
            authors_parsed += successful_parse
    if not authors_parsed and "log_parse_fail" in globals():
        globals()["log_parse_fail"] += [(authors_string, authors_cleaned)]
    publication_authors_parsed += [(id, authors, authors_parsed)]

# Log relevant information
log.latinized(globals().get("log_latinized"), logging.getLogger("etis"))
total_entries = len(publication_authors_parsed)
log.parse_fail(total_entries, globals().get("log_parse_fail"), logging.getLogger("etis"))


############################
# Standardize parsed names #
############################

name_initial_groups = rf"(?P<last>({prefix})?\s?{name})[,\s]\s*(?P<first>{initial})"
initial_name_groups = rf"(?P<first>{initial})[,\s]*\s*(?P<last>({prefix})?\s?{name})"
last_first_groups = rf"(?P<last>({prefix})?\s?{name}),\s*(?P<first>{name}(\s*{name})?)"
patterns_standardize = [name_initial_groups, initial_name_groups, last_first_groups]

author_standardizer = data_operations.AuthorStandardizer(patterns_standardize, initial)

####################### Could use some Pandas mutate equivalent?
####################### Or just make into a dict and modify in place?

publication_authors_standardized = list()
for id, authors, authors_parsed in publication_authors_parsed:
    authors_parsed_new = [author_standardizer.standardize(author) for author in authors_parsed]
    publication_authors_standardized += [(id, authors, authors_parsed_new)]

publication_authors_json_parsed = list()
for id, authors, authors_parsed in publication_authors_standardized:
    publication_authors_json_parsed += [
        (id,
         json.loads(authors),
         authors_parsed)]

publication_authors_match_scores = list()
for id, authors, authors_parsed in publication_authors_json_parsed:
    authors_parsed_new = list()
    for parsed_author in authors_parsed:
        levenshtein_scores = dict()
        for true_author in authors:
            levenshtein_scores[true_author["Guid"]] = Levenshtein.ratio(true_author["Name"], parsed_author, processor=lambda x: x.replace(".", ""))
        authors_parsed_new += [(parsed_author, levenshtein_scores)]
    publication_authors_match_scores += [(id, authors, authors_parsed_new)]

for id, authors, authors_parsed in publication_authors_match_scores:
    for author in authors:
        matches = sorted(authors_parsed, key=lambda x: x[1][author["Guid"]])
        author["best_match"] = (matches[-1][0], matches[-1][1][author["Guid"]]) if matches else (str(), 0)


matches = list()
for _, authors, _ in publication_authors_match_scores:
    for author in authors:
        item = (author["Name"], author["best_match"])
        if item not in matches:
            matches += [item]

matches_sorted = sorted(matches, key=lambda x: x[1][1], reverse=True)
for match in matches_sorted:
    print(f"| {round(match[1][1], 2):<5} | {match[0]:<30} | {match[1][0]:<30} |")

# Next up
# Create reference table for known authors / their authorstrings.
# Try parsing additional authorstrings with that.