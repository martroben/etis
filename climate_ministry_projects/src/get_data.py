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

ETIS_PROJECT_STATUS_CODE = 1
    # 1 - all projects
    # 2 - ongoing projects
    # 3 - finished projects

ETIS_FINANCIER_GUIDS = {
    "Kliimaministeerium": "ea4e072d-a4e0-4b2c-b5db-1621565103da",
    "Ministry of Environment": "95bc4da5-4930-4e71-a3b5-237fb0e820e1",
    "Environmental Board": "f9971634-b46c-4795-9791-5ea8d2cd8975",
    "Estonian Environment Agency": "73bfbf6f-09d7-4155-93f1-1614e2311c56",
    "Geological Survey of Estonia": "394672db-57c9-4aac-909a-b62cd9efa061",
    "Estonian Museum of Natural History": "d166c963-669b-449e-bac5-b4d303516393",
    "Environmental Investment Center": "0b76fca9-890a-43cf-a98e-02222f2aa511",
    "State Forest Management Centre": "fe4be95e-6a35-4371-b7ff-43379bce0ad5",
    "Elering AS, Estonian Transmission System Operator": "00717fd4-5255-4971-899b-17f3430705db",
    "Estonian Environmental Research Centre": "d7d185ff-c625-4209-8c34-8f2ac6239600",
    "Port of Tallinn": "f3da0642-42ed-41d1-89be-34d32a67fefe",
    "AS Tallinn Airport": "27bbfca2-956d-4c06-a747-baefa4880313",
    "Estonian Air Navigation Services": "8f60019a-7665-449f-9c6f-ca3548ba7bfe",
    "Saarte Liinid Ltd": "440435d3-85d5-45fa-9747-11d5abb1c54b",
    "Technical Center of Estonian Roads Ltd": "fe012ed5-d8fe-4e0b-97e7-20754dfdc204"
}

RAW_DATA_DIRECTORY_PATH = "./climate_ministry_projects/data/raw/"
RESULTS_DATA_DIRECTORY_PATH = "./climate_ministry_projects/data/results/"


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
ETIS_project_parameters = {}

n_bad_responses = 0
bad_response_threshold = 10         # Throw after this threshold of bad responses (don't spam API)
items_per_request = 500             # Get items in batches

bad_responses = []
projects = []
with tqdm.tqdm() as ETIS_progress_bar:
    _ = ETIS_progress_bar.set_description_str("Requesting ETIS projects")
    i = 0
    # Since there is no way to request projects by financier uuid, we have to request all projects
    # and filter them afterwards
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

info_string = f'Found {len(projects)} projects in ETIS. Saved to {projects_save_path}'
logger.info(info_string)


############################
# Filter relevant projects #
############################

# Reload data from save file
projects = read_latest_file(RAW_DATA_DIRECTORY_PATH, "projects")

# Filter projects with relevant financiers
relevant_projects = []
for project in projects:
    financier_GUIDs = [financier["Guid"] for financier in project["FinancingInstitutions"]]

    # Select only climate ministry related projects
    if not any(guid in ETIS_FINANCIER_GUIDS.values() for guid in financier_GUIDs):
        continue

    # Select projects started in last 10 years
    # if datetime.datetime.strptime(project["ProjectStartDate"], "%d.%m.%Y").year < (datetime.datetime.now().year - 10):
    #     continue
    
    relevant_projects += [project]

relevant_projects_save_path = f'{RAW_DATA_DIRECTORY_PATH.strip("/")}/relevant_projects_{get_timestamp_string()}.json'
with open(relevant_projects_save_path, "w", encoding="utf8") as save_file:
    save_file.write(json.dumps(relevant_projects, indent=2, ensure_ascii=False))

info_string = f'Found {len(relevant_projects)} relevant projects. Saved to {relevant_projects_save_path}'
logger.info(info_string)


################
# Process data #
################

# Reload data from save file
relevant_projects = read_latest_file(RAW_DATA_DIRECTORY_PATH, "relevant_projects")

# Select relevant data
n_projects_by_year = {}
total_budget_eur_by_year = {}
for project in relevant_projects:
    start_year = datetime.datetime.strptime(project["ProjectStartDate"], "%d.%m.%Y").year
    if start_year not in n_projects_by_year:
        n_projects_by_year[start_year] = 0
        total_budget_eur_by_year[start_year] = 0
    
    n_projects_by_year[start_year] += 1
    total_budget_eur_by_year[start_year] += project["FinancingInPeriodsTotal"]

n_projects_by_year = dict(sorted(n_projects_by_year.items()))
total_budget_eur_by_year = dict(sorted(total_budget_eur_by_year.items()))

# Save results
n_projects_by_year_save_path = f'{RESULTS_DATA_DIRECTORY_PATH.strip("/")}/n_projects_by_year_{get_timestamp_string()}.json'
with open(n_projects_by_year_save_path, "w", encoding="utf8") as save_file:
    save_file.write(json.dumps(n_projects_by_year, indent=2, ensure_ascii=False))

total_budget_eur_by_year_save_path = f'{RESULTS_DATA_DIRECTORY_PATH.strip("/")}/total_budget_eur_by_year_{get_timestamp_string()}.json'
with open(total_budget_eur_by_year_save_path, "w", encoding="utf8") as save_file:
    save_file.write(json.dumps(total_budget_eur_by_year, indent=2, ensure_ascii=False))
