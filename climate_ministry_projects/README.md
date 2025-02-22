# Climate Ministry Projects
Ths is a script to pull project info from Estonian Research Information System (ETIS) [API](https://avaandmed.eesti.ee/datasets/eesti-teadusinfosusteemi-avaandmed) and analyse projects that are either partially or fully financed by the Ministry of Climate or organisations within it's administrative area.
- The list of organisations is in [get_data.py](/climate_ministry_projects/src/get_data.py) variable `ETIS_FINANCIER_GUIDS`.
- The total number of projects and total funds awarded to these projects (including funds from other financiers) are plotted by project start year.
- All of the funds awarded to a project are summarised and plotted under the project start year (even if the actual payments were done across several years).

## Usage
```shell
pip install -r climate_ministry_projects/requirements.txt
```
```shell
python -m climate_ministry_projects/src/get_data.py
python -m climate_ministry_projects/src/plot_data.py
```

The results are saved to `climate_ministry_projects/results/`
