import json
import logging
import sys
import plotly
import plotly.subplots


##########
# Inputs #
##########

PROJECT_PUBLICATION_RELATIVE_TIMES_PATH = "data/results/project_publication_relative_times_20250129182037UTC.json"
PLOT_SAVE_PATH = "data/results/project_publication_relative_times.png"


#####################
# Environment setup #
#####################

# Logger
logger = logging.getLogger()
logger.setLevel("INFO")
logger.addHandler(logging.StreamHandler(sys.stdout))


#############
# Load data #
#############

with open(PROJECT_PUBLICATION_RELATIVE_TIMES_PATH, encoding="utf8") as read_file:
    data = json.loads(read_file.read())


#############
# Plot data #
#############
# Extract relevant data for the scatter plot and prune extreme values
data_for_plot = [
    {
        "EARLIEST_RELATIVE_TIME_DAYS": project["EARLIEST_RELATIVE_TIME_DAYS"],
        "FUNDING_EUR": project["FUNDING_EUR"],
        "INSTITUTION": project["INSTITUTION"]
    }
    for project in data
    if project["EARLIEST_RELATIVE_TIME_DAYS"] is not None
]

# Prune extreme values
def prune_data(data, key, prune_ratio=0.01):
    data.sort(key=lambda x: x[key])
    prune_count = int(len(data) * prune_ratio)
    return data[prune_count:-prune_count]

data_for_plot = prune_data(data_for_plot, "EARLIEST_RELATIVE_TIME_DAYS")
data_for_plot = prune_data(data_for_plot, "FUNDING_EUR")

# Create a scatter plot
figure = plotly.graph_objects.Figure()

# Add traces for each institution
institutions = set(item["INSTITUTION"] for item in data_for_plot)
for institution in institutions:
    institution_data = [item for item in data_for_plot if item["INSTITUTION"] == institution]
    figure.add_trace(plotly.graph_objects.Scatter(
        x=[item["EARLIEST_RELATIVE_TIME_DAYS"] for item in institution_data],
        y=[item["FUNDING_EUR"] for item in institution_data],
        mode='markers',
        name=institution
    ))

# Update layout
figure.update_layout(
    title="Scatter Plot of Earliest Relative Time Days vs Funding EUR",
    xaxis_title="Earliest Relative Time Days",
    yaxis_title="Funding EUR",
    legend_title="Institution",
    width=3000,
    height=1500,
    margin=dict(l=100, r=100, t=100, b=100),
    font=dict(size=18)
)

# Update marker colors and size
colors = plotly.colors.qualitative.Plotly
for i, trace in enumerate(figure.data):
    trace.marker.update(color=colors[i % len(colors)], size=12)

# Create a histogram for EARLIEST_RELATIVE_TIME_DAYS
histogram = plotly.graph_objects.Figure()
histogram.add_trace(plotly.graph_objects.Histogram(
    x=[item["EARLIEST_RELATIVE_TIME_DAYS"] for item in data_for_plot],
    nbinsx=100,  # Bin width two times smaller
    marker=dict(color='rgba(0, 0, 255, 0.7)')
))

# Update layout for histogram
histogram.update_layout(
    xaxis_title="Earliest Relative Time Days",
    yaxis_title="Count",
    width=3000,
    height=500,
    margin=dict(l=100, r=100, t=10, b=100),
    font=dict(size=18),
    yaxis=dict(showgrid=False, showticklabels=False),  # Remove y-axis gridlines and values
    xaxis=dict(showgrid=False)  # Remove x-axis gridlines
)

# Combine scatter plot and histogram into a single figure
combined_figure = plotly.subplots.make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    row_heights=[0.85, 0.15],  # Adjust row heights to bring figures closer
    vertical_spacing=0.02,  # Reduce vertical spacing
    subplot_titles=("", "")  # Remove figure captions
)

for trace in figure.data:
    combined_figure.add_trace(trace, row=1, col=1)

for trace in histogram.data:
    combined_figure.add_trace(trace, row=2, col=1)

# Update layout for combined figure
combined_figure.update_layout(
    height=2000,
    width=3000,
    showlegend=True,
    title_text="Combined Plot"
)

# Remove histogram legend from the colors legend
combined_figure.data[-1].showlegend = False

# Save the combined plot
plotly.io.write_image(combined_figure, PLOT_SAVE_PATH)


# TODO: university brand colours
# TODO: headings
# TODO: better colour for histogram
# TODO: remove gridlines
# TODO: bigger axis tick lables
# TODO: plot heading
# TODO: link to repo
