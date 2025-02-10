# standard
import datetime
import json
import logging
import os
import re
import sys
# external
import requests
import tqdm


##########
# Inputs #
##########

ETIS_SCIENTIFIC_ARTICLES_CLASSIFICATION_CODES = [
    "1.1.",     # Web of Science & Scopus scientific articles
    "1.2.",     # Other international scientific articles
    "1.3."      # scientific articles in Estonian journals
]
ETIS_FINISHED_PROJECT_STATUS_CODE = 3
    # 1 - all projects
    # 2 - ongoing projects
    # 3 - finished projects

ETIS_INSTITUTION_IDS = {
    "University of Tartu": "2ca97556-276a-49f3-a225-789f48beaa00",
    "Tallinn University of Technology": "4f907bd8-9919-4906-989b-d913b93837f8",
    "Tallinn University": "8a1f0a81-fba5-4276-9a5e-99bf4e21869f",
    "Estonian University of Life Sciences": "72d2775c-5744-49bf-8bab-629b4e8da721"
}

RAW_DATA_DIRECTORY_PATH = "./data/raw/"
RESULTS_DATA_DIRECTORY_PATH = "./data/results/"


#########################
# Classes and functions #
#########################

class EtisSession(requests.Session):
    """
    Class for requesting info from ETIS API.
    """
    # https://www.etis.ee:2346/api - test
    # https://www.etis.ee:7443/api - live
    BASE_URL = "https://www.etis.ee:7443/api"

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service_URL = f'{self.BASE_URL}/{service}'

    def get_items(self, n: int = 1, i_start: int = None, parameters: dict = None) -> requests.Response:
        """
        Get items from service that the session was initiated with.
        Start from item i and request n items.
        """
        endpoint = "getitems"
        query_parameters = {
            "Format": "json",
            "Take": n,
        }
        if i_start:
            query_parameters.update({"Skip": i_start})
        if parameters:
            query_parameters.update(parameters)

        URL = f'{self.service_URL}/{endpoint}'
        response = self.get(URL, params=query_parameters)
        return response


def get_timestamp_string() -> str:
    """
    Gives a standard current timestamp string to use in filenames.
    """
    timestamp_format = "%Y%m%d%H%M%S%Z"

    timestamp = datetime.datetime.now(datetime.timezone.utc)
    timestamp_string = datetime.datetime.strftime(timestamp, timestamp_format)
    return timestamp_string


def read_latest_file(dir_path: str, file_handle: str = None) -> list[dict]:
    """
    Reads file with the latest timestamp in filename from given dir_path.
    If file_handle is given, checks only filenames with the given file_handle followed by a timestamp.
    """
    if not file_handle:
        file_handle = ".+"
    name_pattern = file_handle + r'_(\d+)'

    files = [file for file in os.listdir(dir_path) if re.match(name_pattern, file)]
    files_latest = sorted(files, key=lambda x: re.match(name_pattern, x).group(1))[-1]
    path = f'{dir_path.strip("/")}/{files_latest}'

    with open(path, encoding="utf8") as read_file:
        data = json.loads(read_file.read())
    
    return data


#####################
# Environment setup #
#####################

# Create directories
if not os.path.exists(RAW_DATA_DIRECTORY_PATH):
    os.makedirs(RAW_DATA_DIRECTORY_PATH)

if not os.path.exists(RESULTS_DATA_DIRECTORY_PATH):
    os.makedirs(RESULTS_DATA_DIRECTORY_PATH)

# Logger
logger = logging.getLogger()
logger.setLevel("INFO")
logger.addHandler(logging.StreamHandler(sys.stdout))


######################
# Pull ETIS Projects #
######################

ETIS_project_session = EtisSession(service="project")
ETIS_project_parameters = {
    "ProjectStatus": ETIS_FINISHED_PROJECT_STATUS_CODE
}

n_bad_responses = 0
bad_response_threshold = 10         # Throw after this threshold of bad responses (don't spam API)
items_per_request = 500             # Get items in batches

bad_responses = []
projects = []
with tqdm.tqdm() as ETIS_progress_bar:
    _ = ETIS_progress_bar.set_description_str("Requesting ETIS projects")
    for institution_ID in ETIS_INSTITUTION_IDS.values():
        ETIS_project_parameters["InstitutionId"] = institution_ID
        i = 0
        while True:
            response = ETIS_project_session.get_items(
                n=items_per_request,
                i_start=i,
                parameters=ETIS_project_parameters)
            
            if not response:
                bad_responses += [response]
                n_bad_responses += 1
                if n_bad_responses >= bad_response_threshold:
                    raise ConnectionError(f'Reached bad response threshold: {bad_response_threshold}')
                continue

            items = response.json()
            if not items:
                break

            projects += items
            i += items_per_request
            _ = ETIS_progress_bar.update()


projects_save_path = f'{RAW_DATA_DIRECTORY_PATH.strip("/")}/projects_{get_timestamp_string()}.json'
with open(projects_save_path, "w", encoding="utf8") as save_file:
    save_file.write(json.dumps(projects, indent=2, ensure_ascii=False))

info_string = f'Found {len(projects)} relevant projects in ETIS. Saved to {projects_save_path}'
logger.info(info_string)


############################
# Filter relevant projects #
############################

# Reload data from save file
projects = read_latest_file(RAW_DATA_DIRECTORY_PATH, "projects")

# Filter projects with publications and duration between 2.5 and 3.5 years
relevant_projects = []
for project in projects:
    if not project["Publications"]:
        continue

    start_date = datetime.datetime.strptime(project["ProjectStartDate"], "%d.%m.%Y")
    end_date = datetime.datetime.strptime(project["ProjectEndDate"], "%d.%m.%Y")
    project_duration = end_date - start_date
    project_duration_months = project_duration.days // 30
    # Skip projects with duration outside the interval of 2.5 years to 3.5 years
    if not (2.5 * 12 <= project_duration_months <= 3.5 * 12):
        continue

    relevant_projects += [project]

relevant_projects_save_path = f'{RAW_DATA_DIRECTORY_PATH.strip("/")}/relevant_projects_{get_timestamp_string()}.json'
with open(relevant_projects_save_path, "w", encoding="utf8") as save_file:
    save_file.write(json.dumps(relevant_projects, indent=2, ensure_ascii=False))


################################
# Get project publication info #
################################

relevant_projects = read_latest_file(RAW_DATA_DIRECTORY_PATH, "relevant_projects")

# Select unique publications (same publications can be reported under several projects)
n_publications = 0
publications_index = {}
for project in relevant_projects:
    project_GUID = project["Guid"]
    for publication in project["Publications"]:
        n_publications += 1
        GUID = publication["Guid"]
        publication_data = publications_index.get(GUID) or {}

        if not publication_data:
            publication_data["GUID"] = GUID
            publication_data["PROJECT_GUIDS"] = []

        publication_data["PROJECT_GUIDS"] += [project_GUID]
        publications_index[GUID] = publication_data

publications = list(publications_index.values())

info_string = f'Found {n_publications} publications under the projects. {len(publications)} of these are unique.'
logger.info(info_string)


###################################
# Pull publication info from ETIS #
###################################

ETIS_publication_session = EtisSession(service="publication")

n_bad_responses = 0
bad_response_threshold = 100        # Throw after this threshold of bad responses (don't spam API)

bad_responses = []
publications_with_no_data = []
for publication in tqdm.tqdm(publications, desc="Requesting ETIS publications"):
    response = ETIS_publication_session.get_items(
        parameters={"Guid": publication["GUID"]}
    )
    if not response:
        bad_responses += [response]
        n_bad_responses += 1
        if n_bad_responses >= bad_response_threshold:
            raise ConnectionError(f'Reached bad response threshold: {bad_response_threshold}')
        continue

    publication["DATA"] = {}
    try:
        publication["DATA"] = response.json()[0]
    except Exception as exception:
        publications_with_no_data += [publication]

publications_save_path = f'{RAW_DATA_DIRECTORY_PATH.strip("/")}/publications_{get_timestamp_string()}.json'
with open(publications_save_path, "w", encoding="utf8") as save_file:
    save_file.write(json.dumps(publications, indent=2, ensure_ascii=False))

info_string1 = f'Pulled publication data from ETIS. Saved to {publications_save_path}'
info_string2 = f'ETIS API failed to return data for {len(publications_with_no_data)} of the {len(publications)} publications'
logger.info(info_string1)
logger.info(info_string2)


################
# Process data #
################

# Reload data from save file
relevant_projects = read_latest_file(RAW_DATA_DIRECTORY_PATH, "relevant_projects")
publications = read_latest_file(RAW_DATA_DIRECTORY_PATH, "publications")

# Select only already published scientific articles
relevalt_publications = []
for publication in publications:
    if not publication["DATA"]:
        continue
    if not publication["DATA"]["ClassificationCode"] in ETIS_SCIENTIFIC_ARTICLES_CLASSIFICATION_CODES:
        continue
    if not publication["DATA"]["PublicationStatusEng"].lower() == "published":
        continue

    relevalt_publications += [publication]

publication_timestamps = {publication["GUID"]: datetime.datetime.fromisoformat(publication["DATA"]["DateCreated"]) for publication in relevalt_publications}

# Get relative times
project_publication_relative_times = []
for project in relevant_projects:
    project_relevant_publications = [publication for publication in project["Publications"] if publication["Guid"] in publication_timestamps]
    if not project_relevant_publications:
        continue

    result = {}
    project_publication_timestamps = [publication_timestamps[publication["Guid"]] for publication in project_relevant_publications]
    end_date = datetime.datetime.strptime(project["ProjectEndDate"], "%d.%m.%Y")
    start_date = datetime.datetime.strptime(project["ProjectStartDate"], "%d.%m.%Y")
    duration_days = (end_date - start_date).days
    # Days from project end to publication
    relative_time_days = [(end_date - timestamp).days for timestamp in project_publication_timestamps]

    result["DURATION_DAYS"] = duration_days
    result["RELATIVE_TIME_DAYS"] = relative_time_days
    result["EARLIEST_RELATIVE_TIME_DAYS"] = min(
        [n_days for n_days in relative_time_days if n_days + duration_days > 0],
        default=None
    )
    result["FUNDING_EUR"] = project["FinancingInPeriodsTotal"]
    result["INSTITUTION"] = project["Institutions"]
    project_publication_relative_times += [result]

# Process institutions
n_ambiguous_institution_projects = 0
for project in project_publication_relative_times:
    unique_institutions = set([institution["HeadInstitutionNameEng"].strip(" ") for institution in project["INSTITUTION"]])
    known_institutions = [institution for institution in unique_institutions if institution in ETIS_INSTITUTION_IDS.keys()]
    if len(known_institutions) > 1:
        n_ambiguous_institution_projects += 1
    project["INSTITUTION"] = known_institutions[0]

info_string = f'Found {n_ambiguous_institution_projects} projects out of {len(project_publication_relative_times)} with more than 1 institutions. Selected only the first one.'
logger.info(info_string)

project_publication_relative_times_path = f'{RESULTS_DATA_DIRECTORY_PATH.strip("/")}/project_publication_relative_times_{get_timestamp_string()}.json'
with open(project_publication_relative_times_path, "w", encoding="utf8") as save_file:
    save_file.write(json.dumps(project_publication_relative_times, indent=2, ensure_ascii=False))
