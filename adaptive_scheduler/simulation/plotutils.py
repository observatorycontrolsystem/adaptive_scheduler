import os

import numpy as np
from opensearchpy import OpenSearch

PLOTEXPORT_DIR = os.getenv('PLOTEXPORT_DIR', 'adaptive_scheduler/simulation/plot_output')
PLOTEXPORT_FORMATS = ['jpg', 'pdf']

OPENSEARCH_URL = os.getenv('OPENSEARCH_URL', 'https://logs.lco.global/')
OPENSEARCH_INDEX = os.getenv('OPENSEARCH_INDEX', 'scheduler-simulations')
opensearch_client = OpenSearch(OPENSEARCH_URL)

default_colors = ['deeppink',
                  'forestgreen',
                  'limegreen',
                  'mediumseagreen',
                  'mediumturquoise',
                  'royalblue',
                  'slateblue',
                  'darkorchid',
                  'indigo',
                  'navy']


def export_to_image(fname, fig):
    """Takes a matplotlib Figure object and saves the figure. If the output
    directory doesn't already exist, creates one for the user.

    Args:
        fname (str): The filename to save the file as.
        fig (matplotlib.pyplot.Figure): The figure to save, typically created by
            calling subplots().
    """
    try:
        os.mkdir(PLOTEXPORT_DIR)
        print(f'Directory "{PLOTEXPORT_DIR}" created')
    except FileExistsError:
        pass
    for imgformat in PLOTEXPORT_FORMATS:
        fpath = os.path.join(PLOTEXPORT_DIR, f'{fname}.{imgformat}')
        fig.savefig(fpath, format=imgformat)
        print(f'Plot exported to {fpath}')


def plot_barplot(ax, data, colors, labels, binnames, barwidth=0.04):
    """Generates a barplot for multiple datasets.

    Args:
        ax (matplotlib.pyplot.Axes): An Axes object to modify.
        data: A list of lists. Each sub-list contains the y-axis data for a dataset.
        colors: The list of colors to use for each dataset. Must contain enough colors
            to cover all datasets.
        labels: The list of labels to associate with each dataset. Must contain a label for each dataset.
        binnames: A list of names of the bins for marking the x-axis.
        barwidth (float): The width of each bar.
    """
    ticks = np.arange(len(data[0]))
    for i, datavalues in enumerate(data):
        ax.bar(ticks+barwidth*i, datavalues, barwidth, color=colors[i], label=labels[i], alpha=0.8)
    ax.set_xticks(ticks+barwidth*i/2, binnames)


def get_data_from_opensearch(query):
    """Searches OpenSearch for a matching query (wildcards allowed) and returns the source data.

    Args:
        query (str): The search query to look for.

    Returns:
        source_data (dict): A dictionary of the data returned from OpenSearch.
        None: Returns None if there are no results.
    """
    source_data = None
    query = {'query': {
        'wildcard': {'simulation_id.keyword': query}
        }
    }
    response = opensearch_client.search(query, OPENSEARCH_INDEX)
    try:
        result = response['hits']['hits'][0]
        source_data = result['_source']
        print(f'Got data for id: {source_data["simulation_id"]}')
    except IndexError:
        print(f'Found no results for {query}')
    return source_data
