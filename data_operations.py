# external
import regex
import transliterate

class AuthorCleaner():

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
        string = regex.sub(r"\s*\(.*?\)\s*|^[^\(]*\)\s*|\s*\([^\)]*$", " ", string)
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
        # Remove consecutive whitespaces: " Dornic,  G" --> "Dornic, G"
        string = regex.sub(r"\s{2,}", " ", string)
        # Remove duplicate commas: "Malyanov,,, Dmitri" --> "Malyanov, Dmitri"
        string = regex.sub(r",{2,}", ",", string)
        # Remove quotation marks: ""Kreek, Valdis" --> "Kreek, Valdis"
        string = regex.sub(r"\"", "", string)
        # Remove trailing and leading commas, periods and whitespaces: ", Sergey Vecherovski." --> "Sergey Vecherovski"
        string = regex.sub(r"^\s*[,\.]+|[,\.]+\s*$", "", string).strip()
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


class AuthorStandardizer():

    def __init__(self,patterns_standardize: list[(str,str)], pattern_initial: str) -> None:
        self.patterns_standardize = patterns_standardize
        self.pattern_initial = pattern_initial

    def standardize(self, name: str) -> str:
        """Standardize names to format First Last or F. Last"""
        for pattern in self.patterns_standardize:
            if match:= regex.match(pattern, name):
                first = match.group("first")
                last = match.group("last")
                if regex.match(self.pattern_initial, first):
                    # Initials format to: I. Name, I. J. Name or I-J. Name
                    first = regex.sub("[\s\.]", "", first)
                    first = regex.sub(r"(\p{Lu})(\p{Lu})", "\g<1>. \g<2>", first)
                    first = f"{first}."
                return f"{first} {last}"
        return name