# Citations analyser
App to pull publication data from the Estonian Research Information System (ETIS) API, add reference data from CrossRef API and plot the number of articles per citation count.

## Usage
### Pull script
```shell
git pull https://github.com/martroben/citations_analyser
cd citations_analyser
```

### Optional: install required libraries from `requirements.txt`:
```shell
pip install -r requirements.txt
```
Otherwise you have to install the required libraries manually.

### Optional: edit `sample_ini.yaml`
You can add your e-mail to the `MAILTO` field and rename `sample_ini.yaml` to `ini.yaml`. This will let you use the CrossRef API "polite" pool, which is more stable than the public pool.

See the CrossRef API [documentation on service pools](https://github.com/CrossRef/rest-api-doc#good-manners--more-reliable-service).

### Run the script
```shell
python3 analyse_citations.py
```
It probably takes several hours to run.

## API Documentation
### ETIS API
- https://avaandmed.eesti.ee/datasets/eesti-teadusinfosusteemi-avaandmed

### CrossRef API
- https://www.crossref.org/documentation/retrieve-metadata/rest-api/
- [https://api.crossref.org/swagger-ui/index.html#/Works/get_works__doi_](https://api.crossref.org/swagger-ui/index.html#/Works/get_works__doi_)