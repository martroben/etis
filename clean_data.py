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
import tqdm
import uuid

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
publications_raw = sql_connection.cursor().execute(authors_sql).fetchall()

publications = list()
for id, authors_data, authors_text in publications_raw:
    authors_data_cleaned = list()
    for author in json.loads(authors_data):
        authors_data_cleaned += [dict(
            id=author["Guid"],
            name=author["Name"],
            role=author["RoleNameEng"])]

    publications += [dict(
        id=id,
        authors_data=authors_data_cleaned,
        authors_text=authors_text)]


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


author_cleaner = data_operations.AuthorCleaner(
    delimiter=";",
    unwanted_substrings=unwanted_substrings)


author_parser = data_operations.AuthorParser(
    patterns_extract=patterns_extract,
    patterns_detect=patterns_detect,
    secondary_delimiters=[r"\s", ","])

name_initial_groups = rf"(?P<last>({prefix})?\s?{name})[,\s]\s*(?P<first>{initial})"
initial_name_groups = rf"(?P<first>{initial})[,\s]*\s*(?P<last>({prefix})?\s?{name})"
last_first_groups = rf"(?P<last>({prefix})?\s?{name}),\s*(?P<first>{name}(\s*{name})?)"
patterns_standardize = [name_initial_groups, initial_name_groups, last_first_groups]

author_standardizer = data_operations.AuthorStandardizer(patterns_standardize, initial)

globals()["log_latinized"] = list()
globals()["log_parse_fail"] = list()

for pub in publications:
    # clean for splitting
    text_cleaned = author_cleaner.clean_delimiter(pub["authors_text"])
    # latinize
    text_latinized = author_cleaner.latinize(text_cleaned)
    if text_latinized != text_cleaned and "log_latinized" in globals():
        globals()["log_latinized"] += [(id, text_cleaned, text_latinized)]
    # split
    authors_split = author_cleaner.split(text_latinized)
    # remove unwanted substrings
    authors_substrings_removed = [author_cleaner.remove_substrings(author) for author in authors_split]
    # clean split strings
    authors_cleaned = [author_cleaner.clean(author) for author in authors_substrings_removed]

    # parse authors
    exact_matches = [author_parser.check_exact_match(author) for author in authors_cleaned]
    authors_parsed = [author for (author, is_exact_match) in zip(authors_cleaned, exact_matches) if is_exact_match]
    if not_parsed_authors := [author for (author, is_exact_match) in zip(authors_cleaned, exact_matches) if not is_exact_match]:
        for non_standard_author in not_parsed_authors:
            successful_parse = author_parser.parse_bad_delimiter(non_standard_author)
            if not successful_parse:
                parsed_authors = list()
                break
            authors_parsed += successful_parse
    if not authors_parsed and "log_parse_fail" in globals():
        globals()["log_parse_fail"] += [(pub["authors_text"], authors_cleaned)]
    # Add a new key with the result
    pub["authors_parsed"] = authors_parsed

    # Bring to unified format: 'I. Name' or 'First Last'
    pub["authors_clean"] = [author_standardizer.standardize(parsed_author) for parsed_author in pub["authors_parsed"]]

    # Parse matches with best Levenshtein ratios
    for author in pub["authors_data"]:
        maximum_levenshtein_ratio = 0
        best_levenshtein_match = str()
        for parsed_author in pub["authors_clean"]:
            levenshtein_ratio = Levenshtein.ratio(author["name"], parsed_author, processor=lambda x: x.replace(".", ""))
            if levenshtein_ratio > maximum_levenshtein_ratio:
                maximum_levenshtein_ratio = levenshtein_ratio
                best_levenshtein_match = parsed_author
        author["match"] = best_levenshtein_match
        author["match_score"] = maximum_levenshtein_ratio


# Log relevant information
log.latinized(globals().get("log_latinized"), logging.getLogger("etis"))
total_entries = len(publications)
log.parse_fail(total_entries, globals().get("log_parse_fail"), logging.getLogger("etis"))


#########################################
# Create entries for all parsed authors #
#########################################

levenshtein_threshold_author_alias = 0.6

author_reference_raw = dict()
for pub in publications:
    for author in pub["authors_data"]:
        if author["id"] not in author_reference_raw:
            author_reference_raw[author["id"]] = list()
        if author["match_score"] >= levenshtein_threshold_author_alias and author["match"] not in author_reference_raw[author["id"]]:
            author_reference_raw[author["id"]] += [author["match"]]

# Remove duplicate names that map to two different authors
checked = list()
duplicates = list()
for aliases in author_reference_raw.values():
    for alias in aliases:
        if alias in checked:
            duplicates += [alias]
            continue
        checked += [alias]

author_reference = dict()
for id, aliases in author_reference_raw.items():
    author_reference[id] = [name for name in aliases if name not in duplicates]

alias_reference = dict()
for id, aliases in author_reference.items():
    for alias in aliases:
        alias_reference[alias] = id

publication_authors = list()
for pub in publications:
    authors_data = pub["authors_data"]
    matched_authors = [author["match"] for author in pub["authors_data"] if author["match_score"] >= levenshtein_threshold_author_alias]
    unmatched_authors = [author for author in pub["authors_clean"] if author not in matched_authors]
    for author in unmatched_authors:
        if author not in alias_reference:
            generated_id = str(uuid.uuid4())
            alias_reference[author] = generated_id
        authors_data += [dict(
            id=alias_reference[author],
            name=author,
            role=str())]
    authors = [dict(id=author["id"], name=author["name"], role=author["role"]) for author in authors_data]
    publication_authors += [dict(id=pub["id"], authors=authors)]

# Identify similar aliases
levenshtein_threshold_alias_alias = 0.8
similar_aliases = list()
for alias1 in tqdm.tqdm(alias_reference):
    for alias2 in alias_reference:
        # Only find alias pairs that start with the same letter
        if alias1[0] != alias2[0]:
            continue
        if alias1 == alias2:
            continue
        if Levenshtein.ratio(alias1.replace(".", ""), alias2.replace(".", "")) > levenshtein_threshold_alias_alias and (alias1, alias2) not in similar_aliases:
            similar_aliases += [(alias1, alias2)]

# Identify collaborators
collaborator_reference_raw = dict()
for pub in publication_authors:
    author_ids = [author["id"] for author in pub["authors"]]
    for id in author_ids:
        if id not in collaborator_reference_raw:
            collaborator_reference_raw[id] = list()
        collaborator_reference_raw[id] += author_ids

collaborator_reference = {key: set([id for id in value if id != key]) for key, value in collaborator_reference_raw.items()}


for alias1, alias2 in similar_aliases:
    id1 = alias_reference[alias1]
    id2 = alias_reference[alias2]

    not collaborator_reference[id1].isdisjoint(collaborator_reference[id2])


# Match criteria:
# Common coathors
# Begins with same letter

# isdisjoint

publication_authors[158]