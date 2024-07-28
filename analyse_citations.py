# standard
import datetime
import json
import os
import re
import time
import urllib
import warnings
# external
import plotly
import requests
import yaml
import tqdm


##########
# Inputs #
##########

ETIS_DATA_SAVE_PATH = "./ETIS_data.json"
CROSSREF_DATA_SAVE_PATH = "./crossref_data.json"
INI_PATH = "./ini.yaml"
PLOT_SAVE_PATH = "./sample_result.png"
SOURCE_REFERENCE = "https://github.com/martroben/citations_analyser"
PUBLISHING_YEAR_MIN = 2017
PUBLISHING_YEAR_MAX = 2023
ETIS_PUBLICATION_CLASSIFICATION_CODES = ["1.1.", "1.2.", "1.3."]
    # 1.1. - Web of Science & Scopus scientific articles
    # 1.2. - Other international scientific articles
    # 1.3. - scientific articles in Estonian journals


#########################
# Classes and functions #
#########################

class EtisSession(requests.Session):
    """
    Class for requesting info from ETIS API.
    """
    BASE_URL = "https://www.etis.ee:2346/api"

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service_URL = f'{self.BASE_URL}/{service}'

    def get_items(self, n: int, i_start: int, parameters: dict = None) -> requests.Response:
        """
        Get items from service that the session was initiated with.
        Start from item i and request n items.
        """
        endpoint = "getitems"
        query_parameters = {
            "Format": "json",
            "Take": n,
            "Skip": i_start
        }
        if parameters:
            query_parameters.update(parameters)

        URL = f'{self.service_URL}/{endpoint}'
        response = self.get(URL, params=query_parameters)
        return response


class CrossrefWorksSession(requests.Session):
    """
    Class for requesting info from Crossref API works (i.e. publications) route.
    """
    BASE_URL = "https://api.crossref.org/works"

    def __init__(self, app_name: str, app_version: str, app_URL: str, mailto: str) -> None:
        super().__init__()
        self.app_name = app_name
        self.app_version = app_version
        self.app_URL = app_URL
        self.mailto = mailto

    def get_user_agent_header(self) -> str:
        """
        Get the optional User-Agent header for the "polite" pool.
        """
        user_agent_header = None
        if self.app_name and self.app_version and self.mailto:
            user_agent_header = f'{self.app_name}/{self.app_version} ({self.app_URL}; mailto:{self.mailto})'
        return user_agent_header

    def get_work_by_DOI(self, DOI: str) -> requests.Response:
        """
        Takes the cleaned DOI as input.
        I.e. the https://doi.org/ part has to be removed and the DOI should be URL-encoded.
        """
        URL = f'{self.BASE_URL}/{DOI}'
        headers = {}
        user_agent_header = self.get_user_agent_header()
        if user_agent_header:
            headers["User-Agent"] = user_agent_header

        response = self.get(URL, headers=headers)
        return response


def clean_DOI(DOI: str) -> str:
    """
    Removes the leading doi.org URL or DOI:.
    URL-encodes the DOI.
    """
    DOI = DOI.strip(" ").lower()
    if not DOI:
        return DOI
    if DOI[:4] == "doi:":
        # Drop leading "DOI: "
        DOI = DOI[4:].strip(" ")
    if "doi.org" in DOI:
        # Drop leading http://dx.doi.org/ or https://doi.org/
        DOI = re.sub(r"^.+doi.org/\s*", "", DOI)
        
    URL_safe_DOI = urllib.parse.quote(DOI)
    return URL_safe_DOI


def adjust_rate(last_lap_timestamp: float, limit: str, interval: str) -> None:
    """
    Adds sleep to request cycles to adher to the rate limits.
    Takes the end timestamp of the last cycle and limit information from the request response headers.
    """
    # CrossRef API standard limits
    standard_limit = 50
    standard_interval = 1
    # Safety margin 0.1 triggers slowing down when request frequency is within 90% of rate limit
    safety_margin = 0.1

    limit = int(limit or standard_limit)
    interval = int((interval or "").strip("s")) or standard_interval

    requests_per_second = 1 / (time.monotonic() - last_lap_timestamp)
    requests_per_second_limit = limit / interval * (1 - safety_margin)
    if requests_per_second >= requests_per_second_limit:
        warnings.warn(f'Delaying requests. Requests per second too close to rate limit: ({round(requests_per_second, 2)} / {round(requests_per_second_limit, 2)})')
        time.sleep(interval / limit)


def get_xaxis_ticks(x_max: int, n_steps: int, add_plus_to_max_value: bool = True) -> dict:
    """
    Get the axis tick settings for a plotly figure.
    add_plus_to_max_value adds a + sign to the last x-axis tick value.
    This is useful if all values after some maximim threshold are aggregated.
    """
    tick_step = int(round(x_max / n_steps, -1))
    tick_values = [tick_step * i for i in range(n_steps)] + [x_max]
    tick_text = [str(value) for value in tick_values]
    if add_plus_to_max_value:
        tick_text = tick_text[:-1] + [f'{tick_text[-1]}+']
    xaxis_ticks = {
        "tickmode": "array",
        "tickvals": tick_values,
        "ticktext": tick_text
    }
    return xaxis_ticks


##########################
# Pull ETIS Publications #
##########################

ETIS_publication_session = EtisSession(service="publication")
ETIS_publication_parameters = {
    "PublicationStatus": 1,     # 1 - published, 0 - pending
    "PublishingYearMin": PUBLISHING_YEAR_MIN,
    "PublishingYearMax": PUBLISHING_YEAR_MAX
}

items_per_request = 500
# Throw after this threshold of bad responses
bad_response_threshold = 10
bad_responses = []
publications = []

with tqdm.tqdm() as ETIS_progress_bar:
    # Start counting bad responses from 0 for each classification code
    n_bad_responses = 0
    _ = ETIS_progress_bar.set_description_str("ETIS requests")
    for classification_code in ETIS_PUBLICATION_CLASSIFICATION_CODES:
        ETIS_publication_parameters["ClassificationCode"] = classification_code
        i = 0
        while True:
            response = ETIS_publication_session.get_items(
                n=items_per_request,
                i_start=i,
                parameters=ETIS_publication_parameters)
            
            if not response:
                bad_responses += [response]
                n_bad_responses += 1
                if n_bad_responses >= bad_response_threshold:
                    raise ConnectionError(f'Reached bad response threshold: {bad_response_threshold}')
                continue

            items = response.json()
            if not items:
                # Stop if no more items are returned
                break

            publications += items
            i += items_per_request
            _ = ETIS_progress_bar.update()


publications_json = json.dumps(publications, indent=2)

# Save data pulled from ETIS on disk
with open (ETIS_DATA_SAVE_PATH, "w") as ETIS_data_save_file:
    ETIS_data_save_file.write(publications_json)


#####################
# Get CrossRef info #
#####################

# # Uncomment to start with previously saved ETIS data
# with open(ETIS_DATA_SAVE_PATH) as ETIS_data_save_file:
#     publications = json.loads(ETIS_data_save_file.read())

# Load already processed CrossRef data if present
# Applicable if a previous run failed in the middle of the process
if os.path.exists(CROSSREF_DATA_SAVE_PATH):
    with open(CROSSREF_DATA_SAVE_PATH) as crossref_data_save_file:
        publications = json.loads(crossref_data_save_file.read())

# Try to load identifying information from an ini file to get the "polite" CrossRef API pool
try:
    with open(INI_PATH) as ini_file:
        ini_str = ini_file.read()
        ini = yaml.safe_load(ini_str)
except (FileNotFoundError, yaml.YAMLError):
    ini = {}

crossref_works_session = CrossrefWorksSession(
    app_name=ini.get("APP_NAME"),
    app_version=ini.get("APP_VERSION"),
    app_URL=ini.get("APP_URL"),
    mailto=ini.get("MAILTO")
)

# Throw after this threshold of bad responses
n_bad_responses = 0
bad_responses = []
bad_response_threshold = 10

lap_timestamp = time.monotonic()
for publication in tqdm.tqdm(publications, desc="CrossRef requests"):

    if "CrossrefInfo" in publication:
        # Don't re-request publications that already have Crossref info included
        # Applicable when retrying a failed run from data saved on disk
        continue

    DOI = clean_DOI(publication.get("Doi"))
    if not DOI:
        # Don't request publications where there is no DOI in the ETIS info
        continue

    try:
        response = crossref_works_session.get_work_by_DOI(DOI)
    except Exception as e:
        # Save process in case of an unexpected request error
        with open(CROSSREF_DATA_SAVE_PATH, "w") as crossref_data_save_file:
            crossref_data_save_file.write(json.dumps(publications, indent=2))
        raise e

    if not response:
        # CrossRef API returns 404 if there is no match for the input DOI
        if response.status_code == 404:
            publication["CrossrefInfo"] = {}
            continue
        n_bad_responses += 1
        bad_responses += [response]
        if n_bad_responses >= bad_response_threshold:
            raise ConnectionError(f'Reached bad response threshold: {bad_response_threshold}')
        continue
    
    # Add delay if the pace of requests is coming close to the API rate limit
    adjust_rate(
        last_lap_timestamp=lap_timestamp,
        limit=response.headers.get("x-rate-limit-limit"),
        interval=response.headers.get("x-rate-limit-interval")
    )
    lap_timestamp = time.monotonic()

    publication["CrossrefInfo"] = response.json().get("message") or {}


# Save data with added CrossRef info on disk
with open(CROSSREF_DATA_SAVE_PATH, "w") as crossref_data_save_file:
    crossref_data_save_file.write(json.dumps(publications, indent=2))


################
# Process data #
################

# # Uncomment to start with previously saved CrossRef data
# with open(CROSSREF_DATA_SAVE_PATH) as crossref_data_save_file:
#     publications = json.loads(crossref_data_save_file.read())

publications_without_DOI = [pub for pub in publications if not pub.get("Doi")]
publications_without_crossref = [pub for pub in publications if (pub.get("Doi") and not pub.get("CrossrefInfo"))]
publications_with_crossref = [pub for pub in publications if pub.get("CrossrefInfo")]

# Truncate citation counts above the limit and present them as a single bar in the histogram
truncation_limit = 100
# Dict with number of articles per citation count
counts = {}
for publication in publications_with_crossref:
    n_citations = publication["CrossrefInfo"]["is-referenced-by-count"]
    n_citations = n_citations if n_citations < truncation_limit else truncation_limit
    if n_citations not in counts:
        counts[n_citations] = 0
    counts[n_citations] += 1

# Add zeros for missing counts
for i in range(len(counts)):
    if i not in counts:
        counts[i] = 0

counts = dict(sorted(counts.items()))

# Dict with cumulative frequencies per citation count
cumulative_frequencies = {}
cumulative_sum = 0
for key, value in counts.items():
    cumulative_sum += value
    cumulative_frequencies[key] = cumulative_sum / sum(counts.values())


################
# Plot results #
################

count_plot = plotly.graph_objects.Bar(
    x=list(counts.keys()),
    y=list(counts.values())
)
cumulative_frequencies_plot = plotly.graph_objects.Scatter(
    x=list(cumulative_frequencies.keys()),
    y=list(cumulative_frequencies.values()),
    mode="lines",
    yaxis="y2"
)

# Annotations are notes on the plot
source_reference_annotation = {
    "x": -0.13,
    "y": -0.23,
    "xref": "paper",
    "yref": "paper",
    "text": f'Source: ETIS, CrossRef | {SOURCE_REFERENCE}',
    "showarrow": False,
    "xanchor": "left",
    "yanchor": "bottom"
}
overview_annotation = {
    "x": 0.95,
    "y": 0.66,
    "xref": "paper",
    "yref": "paper",
    "text": f'Total articles: {len(publications)}<br>Articles without DOI: {len(publications_without_DOI)}<br>Articles with DOI, but without CrossRef: {len(publications_without_crossref)}',
    "align": "right",
    "showarrow": False,
    "xanchor": "right",
    "yanchor": "top"
}

figure_title = f'Articles by citation count {PUBLISHING_YEAR_MIN}-{PUBLISHING_YEAR_MAX}'
figure_subtitle = f'date created: {datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d")}'

figure = plotly.graph_objects.Figure()
figure.update_layout(
    title=f'{figure_title}<br><sub>{figure_subtitle}</sub>',
    xaxis=get_xaxis_ticks(x_max=max(counts), n_steps=5),
    xaxis_title="citation count",
    yaxis_title="number of articles",
    yaxis2=dict(
        title="cumulative frequency",
        overlaying="y",
        side="right"
    ),
    showlegend=False,
    annotations=[
        source_reference_annotation,
        overview_annotation
    ]
)
figure.add_trace(count_plot)
figure.add_trace(cumulative_frequencies_plot)


#############
# Save plot #
#############

plotly.io.write_image(figure, PLOT_SAVE_PATH)
