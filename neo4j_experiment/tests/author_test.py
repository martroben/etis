from data_operations import Author

def test_init_by_dict():
    author = Author(**{"id":"123", "alias":"J. Smith", "name":"John F. Smith"})
    assert author.id == "123"
    assert author.aliases == {"J. Smith", "John F. Smith"}
    assert author.name == "John F. Smith"

def test_regular_init():
    author = Author(id="223", alias="Peter Fletcher", name="Peter Fletcher the 3rd")
    assert author.id == "223"
    assert author.aliases == {"Peter Fletcher", "Peter Fletcher the 3rd"}
    assert author.name == "Peter Fletcher the 3rd"

def test_equivalency():
    author1 = Author(id="123", alias="J. Smith", name="John F. Smith")
    author2 = Author(id="123", alias="J. Smith", name="John D. Smith")
    author3 = Author(id="223", alias="Peter Fletcher", name="Peter Fletcher the 3rd")
    assert author1 == author2
    assert author1 != author3

def test_hashing():
    author1 = Author(id="123", alias="J. Smith", name="John F. Smith")
    author2 = Author(id="123", alias="J. Smith", name="John D. Smith")
    author3 = Author(id="223", alias="Peter Fletcher", name="Peter Fletcher the 3rd")
    # Hash is calculated by id, therefore set shouldn't take two authors with same id.
    assert len({author1, author2, author3}) == 2

def test_similarity_ratio():
    author1 = Author(id="123", alias="J. Smith", name="John F. Smith")
    author2 = Author(id="223", alias="Peter Fletcher")
    author3 = Author(id="423", alias ="R. Smith")

    assert author1.similarity_ratio(author1) == 1
    assert author1.similarity_ratio(author2) < author1.similarity_ratio(author3)
    assert author1.similarity_ratio(author3, match_firstletter=True) == 0

def test_merging():
    author1 = Author(id="123", alias="J. Smith", name="John F. Smith")
    author2 = Author(id="323", alias="John Smith")
    assert author1 != author2

    author1.merge(author2)
    author2.merge(author1)

    assert author1 == author2
    assert author1.aliases == author2.aliases
    assert author1.name == author2.name
