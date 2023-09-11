
# standard
import os
import sqlite3


def get_connection(path: str) -> sqlite3.Connection:
    """
    Get SQLite connection to a given database path.
    If database doesn't exist, creates a new database and path directories to it (unless path is :memory:).
    :param path: Path to SQLite database
    :return: sqlite3 Connection object to input path
    """
    if path != ":memory:":
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
    connection = sqlite3.connect(path)
    return connection


def create_table(table: str, columns: dict, connection: sqlite3.Connection) -> None:
    """
    Creates a table in SQLite
    :param table: table name
    :param columns: A dict in the form of {column name: SQLite type name}
    :param connection: SQLite connection object.
    :return: None
    """
    sql_cursor = connection.cursor()
    columns_string = ",".join([f"{key} {value}" for key, value in columns.items()])
    sql_statement = f"CREATE TABLE IF NOT EXISTS {table} ({columns_string})"
    sql_cursor.execute(sql_statement)
    return


def get_sqlite_type(python_type: str) -> str:
    """
    Get SQLite data type name that corresponds to input python data type name.
    :param python_type: Name of Python type to convert
    :return: SQLite data type name
    """
    sql_type_reference = {
        "int": "INTEGER",
        "float": "REAL",
        "str": "TEXT",
        "NoneType": "NULL"}
    return sql_type_reference.get(python_type, "BLOB")


def insert_row(table: str, connection: sqlite3.Connection, **kwargs) -> int:
    """
    Inserts a single row to a SQLite table
    :param table: Name of the table to insert to
    :param connection: SQLite connection object
    :param kwargs: Key-value pairs to insert. In the form of column name: value
    :return: Number of rows inserted (1 or 0)
    """
    column_names = list()
    values = tuple()
    for column_name, value in kwargs.items():
        column_names += [column_name]
        values += (str(value),) if isinstance(value, (list, str)) else (value,)
    column_names_string = ",".join(column_names)
    placeholder_string = ", ".join(["?"] * len(column_names))  # As many placeholders as columns. E.g (?, ?, ?, ?)
    sql_statement = f"""
        INSERT INTO {table}
            ({column_names_string})
        VALUES
            ({placeholder_string});
        """
    sql_cursor = connection.cursor()
    sql_cursor.execute(sql_statement, values)
    return sql_cursor.rowcount





# database = sqlite3.Connection("data.sql")
# sql_cursor = sqlite3.Cursor(database)
#
# sql_create_publication_raw = '''
#     CREATE TABLE PublicationRaw (
#         Guid TEXT PRIMARY KEY,
#         DisplayInfo TEXT,
#         PublicationTypeName TEXT,
#         PublicationTypeNameEng TEXT,
#         OpenAccessTypeName TEXT,
#         OpenAccessTypeNameEng TEXT,
#         OpenAccessLicenceName TEXT,
#         OpenAccessLicenceNameEng TEXT,
#         Authors BLOB,
#         AuthorsText TEXT,
#         Languages TEXT,
#         LanguagesCode TEXT,
#         LanguagesEng TEXT,
#         Title TEXT,
#         TitleHtml TEXT,
#         TitleTranslation TEXT,
#         IssueTitle TEXT,
#         University TEXT,
#         Editors TEXT,
#         Periodical TEXT,
#         ConferenceDescription TEXT,
#         PublishingPlace TEXT,
#         PublishingHouse TEXT,
#         Issn TEXT,
#         Isbn TEXT,
#         Binding TEXT,
#         Number TEXT,
#         Part TEXT,
#         SupplementIssue TEXT,
#         SpecialIssue TEXT,
#         Series TEXT,
#         SeriesBinding TEXT,
#         PublishingYear FLOAT,
#         PagesCount TEXT,
#         PagesEnd TEXT,
#         PagesStart TEXT,
#         PublicationStatus TEXT,
#         PublicationStatusEng TEXT,
#         IsOpenAccess TEXT,
#         IsOpenAccessEng TEXT,
#         IsPublic INT,
#         ClassificationCode TEXT,
#         ClassificationDatabaseSubtype TEXT,
#         ClassificationName TEXT,
#         ClassificationNameEng TEXT,
#         PublicFile INT,
#         Url TEXT,
#         Doi TEXT,
#         BookDoi TEXT,
#         Institutions BLOB,
#         InstitutionsAsFreeText TEXT,
#         Comment TEXT,
#         Keywords TEXT,
#         KeywordsEng TEXT,
#         AbstractEst TEXT,
#         AbstractEng TEXT,
#         KeywordsAsFreeText TEXT,
#         UserKeywords TEXT,
#         Projects TEXT,
#         FullTextLocation TEXT,
#         ReferencingDatabase TEXT,
#         DissertationTypeName TEXT,
#         DissertationTypeNameEng TEXT,
#         DateCreated FLOAT,
#         DateModified FLOAT,
#         WOSdocumentType TEXT,
#         WOSfieldsOfResearch TEXT)
# '''

# sql_cursor.execute(sql_create_publication_raw)
# database.commit()
#
# sql_cursor.execute("INSERT INTO PublicationRaw VALUES ()")

# sql_cursor.execute("SELECT * FROM test").fetchall()

# Booleans
# IsPublic
# PublicFile

# Lists
# Authors
# Institutions

# Dates
# DateCreated: '2012-09-24T10:58:17.907',
# DateModified' '2019-01-02T19:30:49.8328688',

# Don't know type
# UserKeywords
# WOSdocumentType
# WOSfieldsOfResearch

# sample_publication = {
#     'Guid': 'ceaeecac-725e-42f5-8e20-27af23448256',
#     'DisplayInfo': 'Andresen, Lembit (2008). Eellugu ja algus. – Tallinna Ülikooli Haapsalu Kolledži arengulugu 1998-2008. Haapsalu. Tallinn.',
#     'PublicationTypeName': 'raamat / monograafia',
#     'PublicationTypeNameEng': 'book / monograph',
#     'OpenAccessTypeName': '',
#     'OpenAccessTypeNameEng': '',
#     'OpenAccessLicenceName': '',
#     'OpenAccessLicenceNameEng': '',
#     'Authors': [
#         {'Guid': '672c9987-4fe0-4b55-b3f2-490a7a68f14f',
#          'IdCode': None,
#          'Name': 'Lembit Andresen',
#          'RoleName': 'Autor',
#          'RoleNameEng': 'Author'}],
#     'AuthorsText': 'Andresen, Lembit',
#     'Languages': '',
#     'LanguagesCode': '',
#     'LanguagesEng': '',
#     'Title': 'Eellugu ja algus. – Tallinna Ülikooli Haapsalu Kolledži arengulugu 1998-2008. Haapsalu.',
#     'TitleHtml': '',
#     'TitleTranslation': '',
#     'IssueTitle': '',
#     'University': '',
#     'Editors': '',
#     'Periodical': '',
#     'ConferenceDescription': '',
#     'PublishingPlace': '',
#     'PublishingHouse': 'Tallinn',
#     'Issn': '',
#     'Isbn': '',
#     'Binding': '',
#     'Number': None,
#     'Part': None,
#     'SupplementIssue': '',
#     'SpecialIssue': '',
#     'Series': '',
#     'SeriesBinding': '',
#     'PublishingYear': 2008,
#     'PagesCount': '9-12',
#     'PagesEnd': '',
#     'PagesStart': '',
#     'PublicationStatus': 'Ilmunud',
#     'PublicationStatusEng': 'Published',
#     'IsOpenAccess': 'Ei',
#     'IsOpenAccessEng': 'No',
#     'IsPublic': True,
#     'ClassificationCode': '6.2.',
#     'ClassificationDatabaseSubtype': '',
#     'ClassificationName': 'Õpikud ja muud õppeotstarbelised publikatsioonid, v.a. kõrgkooliõpikud; ',
#     'ClassificationNameEng': 'Textbooks and other study materials (excluding university textbooks)',
#     'PublicFile': False,
#     'Url': '',
#     'Doi': '',
#     'BookDoi': '',
#     'Institutions': [
#         {'BusinessRegNo': '74000122',
#          'Guid': 'c0f19c59-7c30-47ee-a0b0-6f8d6445d032',
#          'Name': 'Tallinna Ülikool, Kasvatusteaduste Instituut',
#          'NameEng': 'Tallinn University, Institute of Educational Sciences'}],
#     'InstitutionsAsFreeText': None,
#     'Comment': '',
#     'Keywords': '',
#     'KeywordsEng': '',
#     'AbstractEst': '',
#     'AbstractEng': '',
#     'KeywordsAsFreeText': None,
#     'UserKeywords': None,
#     'Projects': [],
#     'FullTextLocation': '',
#     'ReferencingDatabase': '',
#     'DissertationTypeName': '',
#     'DissertationTypeNameEng': '',
#     'DateCreated': '2012-09-24T10:58:17.907',
#     'DateModified': '2019-01-02T19:30:49.8328688',
#     'WOSdocumentType': None,
#     'WOSfieldsOfResearch': None}
