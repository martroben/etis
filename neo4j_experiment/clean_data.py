# local
import data_operations
import log
import neo4j_operations
import sql_operations
# standard
import json
import logging
import sys
import time
# external
from neo4j import GraphDatabase
import networkx
import tqdm
import uuid

#################
# Setup logging #
#################

logger = logging.getLogger("etis")
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.getLevelName("INFO"))


########################
# Pull data from neo4j #
########################

# neo4j
neo4j_uri = "bolt://localhost:7687"
neo4j_user = str()
neo4j_password = str()

neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
# Raises exception if connection can't be established
neo4j_driver.verify_connectivity()

with neo4j_driver.session() as session:
    publications_raw = session.execute_read(neo4j_operations.get_publications)

publications = list()
for pub in publications_raw:
    id = pub[0]
    authors_data = pub[1]
    authors_text = pub[2]

    authors_data_cleaned = list()
    for author in json.loads(authors_data):
        authors_data_cleaned += [dict(
            id=author["Guid"],
            name=author["Name"],
            role=author["RoleNameEng"])]

    publications += [{
        "id": id,
        "authors_processed": authors_data_cleaned,
        "authors_text": authors_text}]


######################
# Pull data from sql #
######################

# database_path = "./data.sql"
# publications_raw_table = "PublicationRaw"

# sql_connection = sql_operations.get_connection(database_path)
# authors_sql = "SELECT Guid, Authors, AuthorsText FROM PublicationRaw"
# publications_raw = sql_connection.cursor().execute(authors_sql).fetchall()

# publications = list()
# for id, authors_data, authors_text in publications_raw:
#     authors_data_cleaned = list()
#     for author in json.loads(authors_data):
#         authors_data_cleaned += [dict(
#             id=author["Guid"],
#             name=author["Name"],
#             role=author["RoleNameEng"])]

#     publications += [{
#         "id": id,
#         "authors_processed": authors_data_cleaned,
#         "authors_text": authors_text}]


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
    r",*\s*toimet(\.|aja|anud)\s*",
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

author_cleaner = data_operations.AuthorStringCleaner(
    delimiter=";",
    unwanted_substrings=unwanted_substrings)

author_parser = data_operations.AuthorStringParser(
    patterns_extract=patterns_extract,
    patterns_detect=patterns_detect,
    secondary_delimiters=[r"\s", ","])

name_initial_groups = rf"(?P<last>({prefix})?\s?{name})[,\s]\s*(?P<first>{initial})"
initial_name_groups = rf"(?P<first>{initial})[,\s]*\s*(?P<last>({prefix})?\s?{name})"
last_first_groups = rf"(?P<last>({prefix})?\s?{name}),\s*(?P<first>{name}(\s*{name})?)"
patterns_standardize = [name_initial_groups, initial_name_groups, last_first_groups]

author_standardizer = data_operations.AuthorStringStandardizer(patterns_standardize, initial)

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
    pub["authors_raw"] = [author_standardizer.standardize(parsed_author) for parsed_author in pub["authors_parsed"]]

# Log relevant information
log.latinized(globals().get("log_latinized"), logging.getLogger("etis"))
total_entries = len(publications)
log.parse_fail(total_entries, globals().get("log_parse_fail"), logging.getLogger("etis"))


#############################################################
# Match parsed aliases to authors given in publication data #
#############################################################

def generate_id(handle: str = str()) -> str:
    return handle + str(uuid.uuid4())[len(handle):]

# Identificator to distinguish auto generated id-s
# Assures that actual id-s are alphabetically preferred (relevant in later code)
generated_id_handle = "ffffffff"

# Structure: {author id: Author object}
all_authors = dict()
# Structure: {pub id: {"processed": {author1 id, author2 id, ...}, "raw": {author3 id, author4 id, ...}}}
authors_by_publication = dict()

# Create a source dict of all authors
# Re-create publications so that authors would be references to all_authors dict
for pub in publications:
    authors_by_publication[pub["id"]] = {"processed": set(), "raw": set()}
    for processed_author in pub["authors_processed"]:
        id = processed_author["id"]
        if id not in all_authors:
            all_authors[id] = data_operations.Author(**processed_author)
        all_authors[id].publications.update({pub["id"]})
        authors_by_publication[pub["id"]]["processed"].update({id})
    for raw_author in pub["authors_raw"]:
        id = generate_id(generated_id_handle)
        all_authors[id] = data_operations.Author(id=id, alias=raw_author)
        all_authors[id].publications.update({pub["id"]})
        authors_by_publication[pub["id"]]["raw"].update({id})

# Keep track of how many authors are merged
n_aliases_initial = len(all_authors)
n_aliases_merged_within_publication = 0

# Match parsed aliases to authors given in data
match_pairs = set()
similarity_threshold_within_publication = 0.6
for pub_id, authors in authors_by_publication.items():
    for processed_author in authors["processed"]:
        # Merge best match if it's above similarity threshold
        best_match = (None, 0)
        for raw_author in authors["raw"]:
            if raw_author not in all_authors:
                continue
            similarity_ratio = all_authors[processed_author].similarity_ratio(all_authors[raw_author])
            if similarity_ratio > best_match[1]:
                best_match = (raw_author, similarity_ratio)
        if best_match[1] > similarity_threshold_within_publication:
            match_pairs.add((processed_author, best_match[0]))

# Use matched pairs as network graph edges
# Then connected component fragments are aliases
aliases_graph = networkx.Graph()
aliases_graph.add_edges_from(match_pairs)
equivalent_alias_ids = list(networkx.connected_components(aliases_graph))

for merge_group in equivalent_alias_ids:
    # Number of authors in each group minus the one they're merged into
    n_aliases_merged_within_publication += len(merge_group) - 1

# Keep a record of what was merged
merged_alias_ids = dict()
for alias_ids in equivalent_alias_ids:
    # Uses the fact that the Author.merge method keeps min id
    merged_id = min(alias_ids)
    for alias_id in alias_ids:
        # Bypass alias that others will be merged to
        if alias_id == merged_id:
            continue
        all_authors[merged_id].merge(all_authors[alias_id])
        del all_authors[alias_id]
        merged_alias_ids[alias_id] = merged_id

# Update 'authors by publication' with merged aliases
authors_by_publication_old = authors_by_publication
authors_by_publication = dict()
for pub_id, authors in authors_by_publication_old.items():
    values = dict(
        # Get merged id if available, otherwise use original id
        processed = {merged_alias_ids.get(author_id, author_id) for author_id in authors["processed"]},
        raw = {merged_alias_ids.get(author_id, author_id) for author_id in authors["raw"]})
    authors_by_publication[pub_id] = values

log.within_publication_merge_result(n_aliases_initial, n_aliases_merged_within_publication, logger)


######################################
# Match authors between publications #
######################################

n_aliases_initial_between_publication = len(all_authors)
n_aliases_merged_between_publication = 0

# Cycle until no more authors are merged
while(True):
    # Create a reference for coauthors
    # Structure: {author id: set()}
    coauthors = dict()
    for authors in authors_by_publication.values():
        all_publication_authors = authors["processed"].union(authors["raw"])
        for author in all_publication_authors:
            if author not in coauthors:
                coauthors[author] = set()
            coauthors[author] = coauthors[author].union({coauthor for coauthor in all_publication_authors if coauthor != author})

    # Counters for logging
    n_total_aliases_initial = len(all_authors)
    n_initial_merged = n_aliases_merged_between_publication
    start_time = time.time()

    match_pairs = set()
    coauthors_helper = dict()
    levenshtein_threshold_alias_vs_alias = 0.8
    for author_id1, coauthor_ids1 in tqdm.tqdm(coauthors.items()):
        coauthors_helper[author_id1] = coauthor_ids1
        for author_id2, coauthor_ids2 in coauthors_helper.items():
            if author_id1 == author_id2:
                continue
            if all_authors[author_id1].similarity_ratio(all_authors[author_id2], match_firstletter = True) < levenshtein_threshold_alias_vs_alias:
                continue
            match_found = False
            for coauthor_id1 in coauthor_ids1:
                for coauthor_id2 in coauthor_ids2:
                    coauthor_similarity_ratio = all_authors[coauthor_id1].similarity_ratio(
                        other=all_authors[coauthor_id2],
                        match_firstletter=True)
                    if coauthor_similarity_ratio > levenshtein_threshold_alias_vs_alias:
                        match_pairs.add((author_id1, author_id2))
                        match_found = True
                        break
                if match_found:
                    break

    # Use matched pairs as network graph edges
    # Then connected component fragments are aliases
    aliases_graph = networkx.Graph()
    aliases_graph.add_edges_from(match_pairs)
    equivalent_alias_ids = list(networkx.connected_components(aliases_graph))

    for merge_group in equivalent_alias_ids:
        # Number of authors in each group minus the one they're merged into
        n_aliases_merged_between_publication += len(merge_group) - 1

    # Keep a record of what was merged
    merged_alias_ids = dict()
    for alias_ids in equivalent_alias_ids:
        # Uses the fact that the Author.merge method keeps min id
        merged_id = min(alias_ids)
        for alias_id in alias_ids:
            if alias_id == merged_id:
                continue
            all_authors[merged_id].merge(all_authors[alias_id])
            del all_authors[alias_id]
            merged_alias_ids[alias_id] = merged_id

    # Update 'authors by publication' with merged aliases
    authors_by_publication_old = authors_by_publication
    authors_by_publication = dict()
    for pub_id, authors in authors_by_publication_old.items():
        values = dict(
            # Get merged id if available, otherwise use original id
            processed = {merged_alias_ids.get(author_id, author_id) for author_id in authors["processed"]},
            raw = {merged_alias_ids.get(author_id, author_id) for author_id in authors["raw"]})
        authors_by_publication[pub_id] = values

    if len(match_pairs) == 0:
        break
    log.merge_cycle_result(
        n_total_aliases_initial,
        n_aliases_merged_between_publication - n_initial_merged,
        time.time() - start_time,
        logging.getLogger("etis"))

log.merge_total_result(
    n_aliases_initial,
    n_aliases_merged_between_publication + n_aliases_merged_within_publication,
    logging.getLogger("etis"))


#########################
# Save authors to neo4j #
#########################

neo4j_driver.verify_connectivity()

with neo4j_driver.session() as session:
    for author in tqdm.tqdm(all_authors.values()):
        author_insert_values = {
            "id": author.id,
            "name": author.name,
            "aliases": list(author.aliases)}
        _ = session.execute_write(
            neo4j_operations.create_author_node,
            **author_insert_values)


############################################
# Save author - publication edges to neo4j #
############################################

neo4j_driver.verify_connectivity()

with neo4j_driver.session() as session:
    for author in tqdm.tqdm(all_authors.values()):
        for publication_id in author.publications:
            _ = session.execute_write(
                neo4j_operations.create_author_publication_edge,
                author_id=author.id,
                publication_id=publication_id)


######################################################
# Problem
# Can't see any connections in query environment
