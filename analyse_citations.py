# standard
import json
import re
import time
import urllib
import warnings
# external
import requests
import yaml
import tqdm


#########################
# Classes and functions #
#########################

class EtisSession(requests.Session):
    BASE_URL = "https://www.etis.ee:2346/api"

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service_URL = f"{self.BASE_URL}/{service}"

    def get_items(self, n: int, i_start: int, parameters: dict = None) -> requests.Response:
        endpoint = "getitems"
        query_parameters = {
            "Format": "json",
            "Take": n,
            "Skip": i_start
        }
        if parameters:
            query_parameters.update(parameters)

        URL = f"{self.service_URL}/{endpoint}"
        response = self.get(URL, params=query_parameters)
        return response


class CrossrefWorksSession(requests.Session):
    BASE_URL = "https://api.crossref.org/"

    def __init__(self, app_name: str, app_version: str, app_URL: str, mailto: str) -> None:
        super().__init__()
        self.app_name = app_name
        self.app_version = app_version
        self.app_URL = app_URL
        self.mailto = mailto

    def get_user_agent_header(self) -> str:
        user_agent_header = None
        if self.app_name and self.app_version and self.mailto:
            user_agent_header = f"{self.app_name}/{self.app_version} ({self.app_URL}; mailto:{self.mailto})"
        return user_agent_header

    def get_clean_DOI(self, DOI: str) -> str:
        DOI = DOI.strip(" ").lower()
        if DOI[:4] == "doi:":
            # Drop leading "DOI: "
            DOI = DOI[4:].strip(" ")
        if "doi.org" in DOI:
            # Drop leading http://dx.doi.org/ or https://doi.org/
            DOI = re.sub(r"^.+doi.org/", "", DOI)
            
        url_safe_DOI = urllib.parse.quote(DOI)
        return url_safe_DOI

    def get_work_by_DOI(self, DOI: str) -> requests.Response:
        clean_DOI = self.get_clean_DOI(DOI)
        URL = f"{self.BASE_URL}/works/{clean_DOI}"

        headers = {}
        user_agent_header = self.get_user_agent_header()
        if user_agent_header:
            headers["User-Agent"] = user_agent_header

        response = self.get(URL, headers=headers)
        return response


def adjust_tempo(last_lap_timestamp: float, limit: str, interval: str) -> None:
    standard_limit = 50
    standard_interval = 1
    safety_margin = 0.1

    limit = int(limit or standard_limit)
    interval = int((interval or "").strip("s")) or standard_interval

    requests_per_second = 1 / (time.time() - last_lap_timestamp)
    requests_per_second_limit = limit / interval * (1 - safety_margin)
    if requests_per_second >= requests_per_second_limit:
        warnings.warn(f"Delaying requests. Requests per second too close to rate limit: ({round(requests_per_second, 2)} / {round(requests_per_second_limit, 2)})")
        time.sleep(interval / limit)


##########################
# Pull ETIS Publications #
##########################

ETIS_publication_session = EtisSession(service="publication")
ETIS_publication_parameters = {
    "PublicationStatus": 1,     # 1 - published, 0 - pending
    "PublishingYearMin": 2017,
    "PublishingYearMax": 2023
}
# 1.1. - Web of Science & Scopus scientific articles
# 1.2. - Other international scientific articles
# 1.3. - scientific articles in Estonian journals
classification_codes = ["1.1.", "1.2.", "1.3."]

items_per_request = 500
bad_response_threshold = 10
publications = list()

with tqdm.tqdm() as ETIS_progress_bar:
    n_bad_responses = 0
    _ = ETIS_progress_bar.set_description_str("ETIS requests")
    for classification_code in classification_codes:
        ETIS_publication_parameters["ClassificationCode"] = classification_code
        i = 0
        while n_bad_responses < bad_response_threshold:
            items_response = ETIS_publication_session.get_items(
                n=items_per_request,
                i_start=i,
                parameters=ETIS_publication_parameters)
            
            if not items_response:
                n_bad_responses += 1
                print(items_response)
                continue

            items = items_response.json()
            publications += items
            i += items_per_request
            _ = ETIS_progress_bar.update()

            if not items:
                break

publications_save_path = "./publications.json"
publications_json = json.dumps(publications, indent=2)

with open (publications_save_path, "w") as publications_save_file:
    publications_save_file.write(publications_json)


with open (publications_save_path) as publications_save_file:
    publications = json.loads(publications_save_file.read())


#####################
# Get CrossRef info #
#####################

# https://www.crossref.org/documentation/retrieve-metadata/rest-api/
# https://api.crossref.org/swagger-ui/index.html#/Works/get_works__doi_

# Try to load identifying information to get the "polite" API pool (more reliable than public pool)
# https://github.com/CrossRef/rest-api-doc#good-manners--more-reliable-service
ini_path = "./ini.yaml"
try:
    with open(ini_path) as ini_file:
        ini_str = ini_file.read()
        ini = yaml.safe_load(ini_str)
except FileNotFoundError:
    ini = {}

crossref_works_session = CrossrefWorksSession(
    app_name=ini.get("APP_NAME"),
    app_version=ini.get("APP_VERSION"),
    app_URL=ini.get("APP_URL"),
    mailto=ini.get("MAILTO")
)

n_bad_responses = 0
bad_response_threshold = 10

lap_timestamp = time.time()
for publication in tqdm.tqdm(publications, desc="CrossRef requests"):
    DOI = publication.get("Doi")
    publication["CrossrefInfo"] = {}

    if not DOI:
        continue

    response = crossref_works_session.get_work_by_DOI(DOI)

    if not response:
        if response.status_code == 404:
            continue
        n_bad_responses += 1
        if n_bad_responses >= bad_response_threshold:
            raise ConnectionError(f"Reached bad response threshold: {bad_response_threshold}")
        continue
    
    adjust_tempo(
        last_lap_timestamp=lap_timestamp,
        limit=response.headers.get("x-rate-limit-limit"),
        interval=response.headers.get("x-rate-limit-interval")
    )
    lap_timestamp = time.time()

    publication["CrossrefInfo"] = response.json().get("message") or {}


with open (publications_save_path, "w") as publications_save_file:
    publications_save_file.write(publications_json)

