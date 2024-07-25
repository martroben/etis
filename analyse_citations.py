# standard
import json
# external
import tqdm
import requests


#########################
# Classes and functions #
#########################

class EtisSession(requests.Session):

    def __init__(self, base_url: str, service: str) -> None:
        super().__init__()
        self.base_url = base_url.strip("/")
        self.service = service

    def get_count(self, parameters: dict = None) -> int:
        endpoint = "getcount"
        query_parameters = {
            "Format": "json"
        }
        if parameters:
            query_parameters.update(parameters)
        
        URL = f"{self.base_url}/{self.service}/{endpoint}"

        response = self.get(URL, params=query_parameters)
        return response.json()["Count"]

    def get_items(self, n: int, i_start: int, parameters: dict = None) -> list[dict]:
        endpoint = "getitems"
        query_parameters = {
            "Format": "json",
            "Take": n,
            "Skip": i_start
        }
        if parameters:
            query_parameters.update(parameters)

        URL = f"{self.base_url}/{self.service}/{endpoint}"

        response = self.get(URL, params=query_parameters)
        if not response:
            print(response)
        return response.json()



##########################
# Pull ETIS Publications #
##########################

base_url = "https://www.etis.ee:2346/api/"
ETIS_publication_session = EtisSession(base_url, service="publication")
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
publications = list()

with tqdm.tqdm() as ETIS_progress_bar:
    _ = ETIS_progress_bar.set_description_str("ETIS requests")
    for classification_code in classification_codes:
        ETIS_publication_parameters["ClassificationCode"] = classification_code
        i = 0
        while True:
            items = ETIS_publication_session.get_items(
                n=items_per_request,
                i_start=i,
                parameters=ETIS_publication_parameters)
            publications += items
            i += items_per_request
            _ = ETIS_progress_bar.update()

            if not items:
                break

publications_save_path = "./publications.json"
publications_json = json.dumps(publications)

with open (publications_save_path, "w") as publications_save_file:
    publications_save_file.write(publications_json)

publications_with_DOI = [pub for pub in publications if pub["Doi"]]


# CrossRef
# https://www.crossref.org/documentation/retrieve-metadata/rest-api/
# https://api.crossref.org/swagger-ui/index.html#/Works/get_works__doi_