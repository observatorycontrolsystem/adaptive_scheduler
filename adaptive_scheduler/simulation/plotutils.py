"""
Plotting utility functions
"""
import os
import logging
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
import opensearchpy
from opensearchpy import OpenSearch

PLOTEXPORT_DIR = os.getenv('PLOTEXPORT_DIR', 'adaptive_scheduler/simulation/plot_output')
PLOTEXPORT_FORMATS = ['jpg', 'pdf']

OPENSEARCH_URL = os.getenv('OPENSEARCH_URL', 'https://logs.lco.global/')
OPENSEARCH_INDEX = os.getenv('OPENSEARCH_INDEX', 'scheduler-simulations')
opensearch_client = OpenSearch(OPENSEARCH_URL)

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class Plot:
    def __init__(self, plotfunc, *sim_ids):
        """A wrapper class for plotting. The user specifies the plotting function to use
        and the simulation ID(s) or search keywords. The data is passed to the plotting
        function as a list of datasets, each set corresponding to an OpenSearch index.
        The plotting function is responsible for accessing the right data keys.

        Args:
            plotfunc: The plotting function to use.
            sim_ids: The simulation IDs to look for on OpenSearch.
        """
        self.plotfunc = plotfunc
        # expects plotting functions to be called 'plot_some_plot_name'
        self.name = plotfunc.__name__.replace('plot_', '')
        self.data = []
        for sim_id in sim_ids:
            self.data.append(get_opensearch_data(sim_id))

        self.fig, self.description = plotfunc(self.data)

    def save(self):
        timestamp = datetime.utcnow().isoformat(timespec='seconds')
        savename = f'{self.name}_{timestamp}'
        export_to_image(savename, self.fig)

    def show(self):
        plt.show()


def export_to_image(fname, fig):
    """Takes a matplotlib Figure object and saves the figure. If the output
    directory doesn't already exist, creates one for the user.

    Args:
        fname (str): The filename to save the file as.
        fig (matplotlib.pyplot.Figure): The figure to save.
    """
    try:
        os.mkdir(PLOTEXPORT_DIR)
        log.info(f'Directory "{PLOTEXPORT_DIR}" created')
    except FileExistsError:
        pass
    for imgformat in PLOTEXPORT_FORMATS:
        fpath = os.path.join(PLOTEXPORT_DIR, f'{fname}.{imgformat}')
        fig.savefig(fpath, format=imgformat)
        log.info(f'Plot exported to {fpath}')


def plot_barplot(ax, data, labels, binnames, barwidth):
    """Generates a barplot for multiple datasets.

    Args:
        ax (matplotlib.pyplot.Axes): An Axes object to modify.
        data: A list of lists. Each sub-list contains the y-axis data for a dataset.
        labels: The list of labels to associate with each dataset. Must contain a label for each dataset.
        binnames: A list of names of the bins for marking the x-axis.
        barwidth (float): The width of each bar.
    """
    ticks = np.arange(len(data[0]))
    for i, datavalues in enumerate(data):
        ax.bar(ticks+barwidth*i, datavalues, barwidth, label=labels[i], alpha=0.8)
    ax.set_xticks(ticks+barwidth*i/2, binnames)


def get_opensearch_data(query):
    """Gets a specific OpenSearch id and returns the source data. Tries to match the exact ID first,
    then moves on to a keyword search (wildcards allowed) if the first search fails. Returns the most
    recent index for the keyword search.

    Args:
        query (str): The index to look for.

    Returns:
        source_data (dict): A dictionary of the data returned from OpenSearch.
    """
    try:
        response = opensearch_client.get(OPENSEARCH_INDEX, query)
        source_data = response['_source']
        log.debug(f'Got data for id: {source_data["simulation_id"]}')
    except opensearchpy.exceptions.NotFoundError:
        log.info(f'Index matching id:{query} not found, trying keyword search')
        query = {
            'query': {
                'wildcard': {'simulation_id.keyword': query}
            },
            'sort': [
                {'record_time': {'order': 'desc'}}
            ]
        }
        response = opensearch_client.search(query, OPENSEARCH_INDEX)
        try:
            result = response['hits']['hits'][0]
            source_data = result['_source']
            log.debug(f'Got data for id: {source_data["simulation_id"]}')
        except IndexError:
            # give up
            raise opensearchpy.exceptions.NotFoundError(f'No data found for {query}')
    return source_data
