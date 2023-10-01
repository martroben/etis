# external
import regex
import transliterate
import Levenshtein


class AuthorStringCleaner():

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


class AuthorStringParser():

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


class AuthorStringStandardizer():

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


class Author:
    def __init__(self, id: str, alias: str = str(), **kwargs) -> None:
        self.id = id
        self.aliases = {alias} if alias else set()
        self.name = str() if "name" not in kwargs else kwargs["name"]
        if self.name:
            self.aliases.update({self.name})
        self.to_be_removed = False

    def __repr__(self) -> str:
        if self.name:
            aliases = '; '.join({alias for alias in self.aliases if alias != self.name})
            return f"{self.name}{(len(aliases) > 0) * f' ({aliases})'}"
        return "; ".join(self.aliases)

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, Author):
            return self.id == __value.id
        return self.id == __value
    
    def __hash__(self) -> int:
        return hash(self.id)
    
    def similarity_ratio(self, other: 'Author', match_firstletter: bool = False) -> float:
        ratios = list()
        for alias1 in self.aliases:
            for alias2 in other.aliases:
                if match_firstletter and alias1[0] != alias2[0]:
                    ratios += [0.0]
                    continue
                if alias1 == alias2:
                    return 1.0
                ratios += [Levenshtein.ratio(alias1, alias2, processor=lambda x: x.replace(".", ""))]
        return max(ratios)
    
    def merge(self, other: 'Author') -> None:
        if self.name and other.name and self.name != other.name:
            # Don't merge authors with different names from given data
            return
        if not self.name and other.name:
            # Use name and id from the one that has name defined
            self.id = other.id
            self.name = other.name
        else:
            # Otherwise use id that's alphabetically first
            self.id = sorted([self.id, other.id])[0]
        # Union aliases
        self.aliases.update(other.aliases)