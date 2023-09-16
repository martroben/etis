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

class AuthorsStringCleaner():

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
            r"\s*\bküsitl\.?\s*",
            r"\s*tõlkija\s*",
            r"\s*tõlge\s.+?\skeelest\s*",
            r"\s*\bkoost\.\s*",
            r"\s*surname\s*",
            r"\s*firstname\s*",
            r"\s*\bjne\b\.?",
            r"\s*\bjt\b\.?"]),
        regex.IGNORECASE)

    # transliterate package: https://pypi.org/project/transliterate/
    transliterate_ru = transliterate.get_translit_function("ru")
    cyrillic_pattern = regex.compile(r"\p{IsCyrillic}")

    # Parsing patterns
    prefix = r"|".join([
    "[Vv]an\s[Dd]er",
    "[Vv]an",
    "[Vv]on",
    "[Ll]a",
    "[Dd]e"])
    name = r"\p{Lu}[\p{L}'’-—]+"
    initial = r"(\p{Lu}\.*-*){1,2}"

    # Name, initial (e.g. Dickus, B.)
    name_initial = rf"({prefix})?\s?{name}[,\s]\s*{initial}"
    # Initial, name (e.g. F.G. Superman)
    initial_name = rf"{initial}[,\s]\s*({prefix})?\s?{name}"
    # Full name (e.g. David Thomas Shore)
    full_name = rf"{name}\s+({prefix})?\s?{name}(\s+{name})?"
    # Last name, first name (e.g. Shore, David Thomas)
    last_first = rf"({prefix})?\s?{name},\s*{name}(\s*{name})?"
    # Single last name, first name (e.g. Lewis, Fiona)
    last_first_single = rf"({prefix})?\s?{name},\s*{name}"

    patterns_exact = [
        rf"^{name_initial}$",
        rf"^{initial_name}$",
        rf"^{full_name}$",
        rf"^{last_first}$"]
    
    patterns_detect = [
        rf"^{name_initial}[\s,]+{name_initial}"
        rf"^{initial_name}[\s,]+{initial_name}",
        rf"^{full_name}[\s,]+{full_name}",
        rf"^{last_first}[\s,]+{last_first}"]

    def __init__(self) -> None:
        pass

    def clean_delimiter(self, string: str) -> str:
        # Replace line breaks with name delimiters: "\r\n" --> ";"
        string = regex.sub(r"\r\n|\n", ";", string)
        # Replace ampersands with name delimiters: "Quick & Easy" --> "Quick; Easy"
        string = regex.sub(r"\s*&\s*", "; ", string)
        # Replace "and" or "with" by name delimiter: "Quick and Easy" --> "Quick; Easy"
        string = regex.sub(r"\s+(and|with)\s+", "; ", string)
        # Replace ellipsis with name delimiteres: "Thomas,  D.. ..." --> "Thomas,  D. ;"
        string = regex.sub(r"(?!\s\p{Lu})\.\.*\s+\.{3}\.*", ". ;", string)
        return string

    def clean(self, string: str) -> str:
        # Remove unwanted substrings
        string = self.discard_substrings.sub(" ", string)

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
        string = regex.sub(r"\b(\p{Lu})\p{L}\b", "\g<1>", string)
        # Remove trailing and leading commas and periods: ", Sergey Vecherovski." --> " Sergey Vecherovski"
        string = regex.sub(r"^\s*[,\.]+|[,\.]+\s*$", "", string)
        return string
    
    def latinize(self, string: str) -> str:
        if self.cyrillic_pattern.search(string):
            original = string
            string = self.transliterate_ru(string, reversed=True)
            # Remove accent characters: "Natal'ya" --> "Natalya"
            string = regex.sub(r"\'", "", string)
            # Preserve only first letter from two letter initials: "Systra, Ju.J" --> "Systra, J.J"
            string = regex.sub(r"\b(\p{Lu})\p{L}\b", "\g<1>", string)
        return string

    def parse_pattern(string: str, pattern: str) -> list[str]:
        """Helper to parse names"""
        matches = []
        while match := regex.search(pattern, string):
            matches += match.captures()
            string = string[match.span()[1]:]
        # Check if there is anything left unparsed
        residue = regex.sub(r"[\.,\s]", "", string)
        if len(residue) > 0:
            return list()
        return matches

    def parse(self, string: str) -> list[str]:
        authors = string.split(";")
        cleaned_authors = [self.clean(author) for author in authors]

        parsed_authors = list()
        exact_match_failed = list()
        for author in authors:
            exact_match = str()
            for pattern in self.patterns_exact:
                if exact_match := regex.match(pattern, author):
                    break
            if exact_match:
                parsed_authors += [exact_match]
                continue
            # If no exact match, add to failed list for algorythmic processing
            exact_match_failed += [author]

        for element in exact_match_failed:
            for pattern in self.patterns_detect:
                # If matches, parse elements with function and add to parsed_authors
                # If not or parse fails, make parsed_authors an empty list
                pass

        return parsed_authors


latinized_log_variable = "log_latinized"
authors_cleaner = AuthorsStringCleaner()
publication_authors_cleaned = list()
for id, authors, authors_string in publication_authors:
    authors_cleaned = authors_cleaner.clean_delimiter(authors_string)
    authors_latinized = authors_cleaner.latinize(authors_cleaned)
    # Record changes to log
    if authors_latinized != authors_cleaned:
        globals()[latinized_log_variable] += (id, authors_cleaned, authors_latinized)
    publication_authors_cleaned += [(id, authors, authors_latinized)]

