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


##########################################
# Clean and transliterate authors string #
##########################################

class AuthorsCleaner():

    # transliterate package: https://pypi.org/project/transliterate/
    transliterate_ru = transliterate.get_translit_function("ru")
    cyrillic_pattern = regex.compile(r"\p{IsCyrillic}")

    def __init__(self, delimiter: str, unwanted_substrings: list[str]) -> None:
        self.delimiter = delimiter
        self.unwanted_substrings = unwanted_substrings

    def clean_delimiter(self, string: str) -> str:
        # Replace line breaks with name delimiters: "\r\n" --> ";"
        string = regex.sub(r"\r\n|\n", f"{self.delimiter}", string)
        # Replace ampersands with name delimiters: "Quick & Easy" --> "Quick; Easy"
        string = regex.sub(r"\s*&\s*", f"{self.delimiter} ", string)
        # Replace "and" "with" or "ja" by name delimiter: "Quick and Easy" --> "Quick; Easy"
        string = regex.sub(r"\s+(and|with|ja)\s+", "; ", string)
        # Replace ellipsis with name delimiteres: "Thomas,  D.. ..." --> "Thomas,  D. ;"
        string = regex.sub(r"(?!\s\p{Lu})\.\.*\s+\.{3}\.*", f". {self.delimiter}", string)
        return string

    def split(self, string: str) -> list[str]:
        return string.split(self.delimiter)

    def remove_substrings(self, string: str) -> str:
        string = regex.sub(
            pattern="|".join(self.unwanted_substrings),
            repl=" ",
            string=string,
            flags=regex.IGNORECASE)
        return string

    def clean(self, string: str) -> str:
        # Remove substrings in parentheses: "Peel, E. (text in parentheses.)" --> "Peel, E. "
        string = regex.sub(r"\s*\(.*?\)\s*", " ", string)
        # Remove substrings in brackets: "Steed, J. [text in brackets.]" --> "Steed, J. "
        string = regex.sub(r"\s*\[.*?\]\s*", " ", string)
        # Remove all digits and periods following digits: " 1990. " --> " "
        string = regex.sub(r"\s*\d+\.*\s*", " ", string)
        # Remove repeating periods: "...." --> ""
        string = regex.sub(r"\s*\.{2,}\s*", " ", string)
        # Remove whitespace from inbetween initials
        string = regex.sub(r"(\p{Lu}\.)\s+(-?\p{Lu}\b\.?)", "\g<1>\g<2>", string)
        # Remove whitespace before comma: "Duke , Raoul" --> "Duke, Raoul"
        string = regex.sub(r"\s+,\s", r", ", string)
        # Remove consecutive, trailing and leading whitespaces: " Dornic,  G" --> "Dornic, G"
        string = regex.sub(r"\s{2,}", " ", string).strip()
        # Remove duplicate commas: "Malyanov,,, Dmitri" --> "Malyanov, Dmitri"
        string = regex.sub(r",{2,}", ",", string)
        # Remove quotation marks: ""Kreek, Valdis" --> "Kreek, Valdis"
        string = regex.sub(r"\"", "", string)
        # Preserve only first letter from two letter initials: "Systra, Ju.J" --> "Systra, J.J"
        # string = regex.sub(r"\b(\p{Lu})\p{L}\b", "\g<1>", string)
        # Remove trailing and leading commas and periods: ", Sergey Vecherovski." --> " Sergey Vecherovski"
        string = regex.sub(r"^\s*[,\.]+|[,\.]+\s*$", "", string)
        return string
    
    def latinize(self, string: str) -> str:
        if self.cyrillic_pattern.search(string):
            string = self.transliterate_ru(string, reversed=True)
            # Remove accent characters: "Natal'ya" --> "Natalya"
            string = regex.sub(r"\'", "", string)
            # Preserve only first letter from two letter initials: "Systra, Ju.J" --> "Systra, J.J"
            string = regex.sub(r"\b(\p{Lu})\p{L}\b", "\g<1>", string)
        return string


class AuthorParser():

    def __init__(
            self,
            patterns_extract: list[str],
            patterns_detect: list[(str,str)],
            secondary_delimiters: list[str]) -> None:
        
        self.patterns_extract = patterns_extract
        self.patterns_detect = patterns_detect
        self.secondary_delimiters = secondary_delimiters

    def parse_pattern(self, pattern: str, string: str) -> list[str]:
        matches = []
        while match := regex.search(pattern, string):
            matches += match.captures()
            # Extract the match, remove delimiters and continue the cycle with remaining string
            string = regex.sub(rf"^[{''.join(self.secondary_delimiters)}]", "", string[match.span()[1]:]).strip()
        # Check if there is anything left unparsed
        residue = regex.sub(r"[\.,\s]", "", string)
        if len(residue) > 0:
            return list()
        return matches

    def check_exact_match(self, string: str) -> bool:
        for pattern in self.patterns_extract:
            if regex.match(rf"^{pattern}$", string):
                return True
        return False

    def parse_bad_delimiter(self, string: str) -> list[str]:
        for pattern_detect, pattern_extract in self.patterns_detect:
            if regex.match(rf"^{pattern_detect}[\s,]+{pattern_detect}", string):
                if parsed := self.parse_pattern(rf"^{pattern_extract}", string):
                    return parsed
        return list()



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
initial = r"(\p{Lu}\.*-*){1,2}(?!\p{Ll})"

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

# (detect_pattern, corresponding extract pattern)
patterns_detect = [
    (name_initial, name_initial),
    (initial_name, initial_name),
    (full_name, full_name),
    (last_first_single, last_first)]


authors_cleaner = AuthorsCleaner(
    delimiter=";",
    unwanted_substrings=unwanted_substrings)

author_parser = AuthorParser(
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
    publication_authors_parsed += [(id, authors, authors_string, authors_parsed)]

print("\n".join([original for original, cleaned in log_parse_fail if original != ""]))

failed_parses = list()
for original, cleaned in log_parse_fail:
    failed_parse = f"{original} | {cleaned}"
    if failed_parse not in failed_parses:
        failed_parses += [failed_parse]

print("\n".join(failed_parses))
