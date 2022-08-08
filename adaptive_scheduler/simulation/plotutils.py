"""
Plotting utility functions
"""
import os
import sys
import argparse
import readline
import logging
from copy import deepcopy
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
import opensearchpy
from opensearchpy import OpenSearch

DEFAULT_DIR = 'adaptive_scheduler/simulation/plot_output'

OPENSEARCH_URL = os.getenv('OPENSEARCH_URL', '')
OPENSEARCH_INDEX = os.getenv('SIMULATION_OPENSEARCH_INDEX', 'scheduler-simulations')
try:
    opensearch_client = OpenSearch(OPENSEARCH_URL)
except TypeError:
    print('Invalid OpenSearch endpoint. Please set `OPENSEARCH_URL` environment variable.')
    sys.exit(1)

# mask logging messages from OpenSearchPy
# they use the root logger, unfortunately
logging.getLogger().setLevel(logging.CRITICAL)

data_cache = {}


class AutoCompleter(object):
    def __init__(self, options):
        """Handles TAB autocomplete in the command line.

        Args:
            options [str]: A list of possible autocomplete options.
        """
        self.options = sorted(options)

    def complete(self, text, state):
        if state == 0:
            if text:
                self.matches = [s for s in self.options if s and text in s]
            else:
                self.matches = self.options[:]

        try:
            return self.matches[state]
        except IndexError:
            return None


def run_user_interface(plots):
    """Handles user interaction in the command line.

    Args:
        plots [Plot]: A list of Plot objects.
    """
    description = 'Plotting functions for scheduler simulator data visualization'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-s', '--save', help='save the plot(s) to a file', action='store_true')
    parser.add_argument('-f', '--format', help='the file format to save as', default='jpg')
    parser.add_argument('-o', '--outputdir', help='the output directory to save to', default=DEFAULT_DIR)
    args = parser.parse_args()
    global export_dir
    global export_format
    export_dir = args.outputdir
    export_format = args.format

    plot_dict = {plot.name: plot for plot in plots}
    plot_names = list(plot_dict.keys())
    spacing = max([len(name) for name in plot_names]) + 10
    print('\nAvailable plots:')
    print(f'\n{"Name":{spacing}}Description')
    print(f'{"====":{spacing}}===========')
    for plot in plots:
        print(f'{plot.name:{spacing}}{plot.description}')

    completer = AutoCompleter(plot_names)
    readline.set_completer(completer.complete)
    readline.parse_and_bind('tab: complete')
    while True:
        showplot = input('\nShow plot (default all): ').strip()
        if showplot == '' or showplot.lower() == 'all':
            for plot in plots:
                plot.generate()
                if args.save:
                    plot.save()
                plt.show()
            break
        else:
            try:
                plot = plot_dict[showplot]
                plt.close('all')
                plot.generate()
                if args.save:
                    plot.save()
                plot.fig.show()
                plt.show()
                break
            except KeyError as e:
                print(f'Plot name not found: {e}')


class Plot:
    def __init__(self, plotfunc, description, sim_ids, **kwargs):
        """A wrapper class for plotting. The user specifies the plotting function to use
        and the simulation ID(s) or search keywords. The data is passed to the plotting
        function as a list of datasets, each set corresponding to an OpenSearch index.
        The plotting function is responsible for accessing the right data keys. Data is cached
        within the same run but not between runs.

        Args:
            plotfunc (func): The plotting function to use.
            description (str): The description of the plot. Will be used as the plot title in matplotlib.
            sim_ids: The simulation IDs to look for on OpenSearch. Can be either a list or a single string.
            kwargs: Optional arguments to pass to the plotting function.
        """
        self.plotfunc = plotfunc
        self.description = description
        # expects plotting functions to be called 'plot_some_plot_name'
        self.name = plotfunc.__name__.replace('plot_', '')
        self.sim_ids = sim_ids if type(sim_ids) is list else [sim_ids]
        self.kwargs = kwargs

    def generate(self):
        self.data = []
        for sim_id in self.sim_ids:
            global data_cache
            try:
                self.data.append(data_cache[sim_id])
            except KeyError:
                data_cache[sim_id] = get_opensearch_data(sim_id)
                self.data.append(data_cache[sim_id])
        if len(self.data) == 1:
            self.data = self.data[0]

        self.fig = self.plotfunc(deepcopy(self.data), self.description, **self.kwargs)

    def save(self):
        timestamp = datetime.utcnow().isoformat(timespec='seconds')
        savename = f'{self.name}_{timestamp}'
        export_to_image(savename, self.fig)


def export_to_image(fname, fig):
    """Takes a matplotlib Figure object and saves the figure. If the output
    directory doesn't already exist, creates one for the user.

    Args:
        fname (str): The filename to save the file as.
        fig (matplotlib.pyplot.Figure): The figure to save.
    """
    global export_dir
    global export_format
    try:
        os.mkdir(export_dir)
        print(f'Directory "{export_dir}" created')
    except FileExistsError:
        pass
    fpath = os.path.join(export_dir, f'{fname}.{export_format}')
    fig.savefig(fpath, dpi=300, format=export_format)
    print(f'Plot exported to {fpath}')


def plot_multi_barplot(ax, data, labels, binnames, barwidth=0.04):
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
        ax.bar(ticks+barwidth*i, datavalues, barwidth, label=labels[i])
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
    except opensearchpy.exceptions.NotFoundError:
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
        except IndexError:
            # give up
            raise opensearchpy.exceptions.NotFoundError(f'No data found for {query}')
    return source_data
