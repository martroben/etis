# standard
import json
import os
import re
# external
import plotly   # Also requires kaleido for static image export


##########
# Inputs #
##########

RESULTS_DATA_DIRECTORY_PATH = "climate_ministry_projects/data/results/"
PLOT_SAVE_PATH = "climate_ministry_projects/data/results/climate_ministry_projects.png"


#############
# Functions #
#############

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


################
# Process data #
################

n_projects_by_year_raw = read_latest_file(RESULTS_DATA_DIRECTORY_PATH, "n_projects_by_year")
total_budget_eur_by_year_raw = read_latest_file(RESULTS_DATA_DIRECTORY_PATH, "total_budget_eur_by_year")

n_projects_by_year = {int(year): n_projects for year, n_projects in n_projects_by_year_raw.items()}
total_budget_eur_by_year = {int(year): total_budget_eur for year, total_budget_eur in total_budget_eur_by_year_raw.items()}
years = list(range(min(n_projects_by_year.keys()), max(n_projects_by_year.keys()) + 1))

n_projects = [n_projects_by_year.get(year, 0) for year in years]
total_budget_eur = [total_budget_eur_by_year.get(year, 0) / 1e6 for year in years]


############
# Plotting #
############

# Plot texts
plot_title = "projects with Ministry of Climate (+ admin area) among financiers"
x_axis_title = "project start year"
y_left_axis_title = "number of projects"
y_right_axis_title = "project funding (millions of EUR)"
annotation = "https://github.com/martroben/etis/tree/master/climate_ministry_projects"


figure = plotly.graph_objects.Figure()

# Bar plot for number of projects
figure.add_trace(
    plotly.graph_objects.Bar(
        x=years,
        y=n_projects,
        name=y_left_axis_title,
        yaxis="y1"
    )
)
# Line plot for total budget
figure.add_trace(
    plotly.graph_objects.Scatter(
        x=years,
        y=total_budget_eur,
        name=y_right_axis_title,
        yaxis="y2",
        mode="lines+markers"
    )
)

# Aligned y axis
y1_limit = round(max(n_projects) + 10, -1) / 10
y2_limit = round(max(total_budget_eur) + 1e3, -3) / 1e3
global_limit = max(y1_limit, y2_limit)
y1_tickvals = [i * 10 for i in range(int(global_limit) + 1) if i % 2 == 0]
y2_tickvals = [i for i in range(int(global_limit) + 1) if i % 2 == 0]

figure.update_layout(
    xaxis_title=x_axis_title,
    yaxis=dict(
        title=y_left_axis_title,
        side="left",
        range=[0, max(y1_tickvals)], 
        tickvals=y1_tickvals
    ),
    yaxis2=dict(
        title=y_right_axis_title,
        overlaying="y", 
        side="right",
        range=[0, max(y2_tickvals)],
        tickvals=y2_tickvals
    ),
    title=plot_title,
    legend=dict(
        x=0.1,
        y=1.1,
        orientation="h"
    )
)

# Repo link
figure.add_annotation(
    x=0.01, y=0.98,
    xref="paper", yref="paper",
    text=annotation,  
    showarrow=False,
    font=dict(size=12, color="darkgray"),
    xanchor="left", yanchor="top"
)


#############
# Save plot #
#############

figure.write_image(PLOT_SAVE_PATH)
