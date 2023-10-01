# external
import Levenshtein

class Author:
    def __init__(self, id: str, alias: str = str(), **kwargs) -> None:
        self.id = id
        self.aliases = {alias}
        self.name = str()
        if "name" in kwargs:
            self.name = kwargs["name"]
            self.aliases.update({self.name})

    def __repr__(self) -> str:
        if self.name:
            return f"{self.name} ({'; '.join(alias for alias in self.aliases if alias != self.name)})"
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
        # Don't merge authors with different names from given data
        if self.name and other.name and self.name != other.name:
            return
        # Use name and id from the one that has name defined
        if not self.name and other.name:
            self.id = other.id
            self.name = other.name
        # Otherwise use id that's alphabetically first
        else:
            self.id = sorted([self.id, other.id])[0]
        # Union aliases
        self.aliases.update(other.aliases)
        