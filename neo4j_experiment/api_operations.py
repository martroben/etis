
# external
import requests
import time
# standard
import urllib.parse


class PublicationSession(requests.Session):
    service = "publication"
    failed_requests = []

    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url

    def get_count(self) -> int:
        endpoint = "getcount"
        parameters = {"Format": "json"}
        url = urllib.parse.urljoin(
            self.base_url,
            "/".join([self.service, endpoint]))

        response = self.get(url, params=parameters)
        return response.json()["Count"]

    def get_items(self, n: int, i: int = 0) -> list[dict]:
        endpoint = "getitems"
        parameters = {
            "Format": "json",
            "Take": n,
            "Skip": i}
        url = urllib.parse.urljoin(
            self.base_url,
            "/".join([self.service, endpoint]))

        response = self.get(url, params=parameters)
        if not response:
            self.failed_requests += [{
                "time": time.time(),
                "url": url,
                "reason": f"{response.status_code}: {response.reason}"}]
        return response.json()
