"""
Plotting functions to use with the adaptive simulator plotting wrapper.
To write your own plotting functions, follow the format of the example functions.
The data passed in should be in list format.
"""
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.style as style

import adaptive_scheduler.simulation.plotutils as plotutils

# change default parameters for matplotlib here
style.use('tableau-colorblind10')
matplotlib.rcParams['figure.figsize'] = (20, 10)
matplotlib.rcParams['figure.titlesize'] = 20
matplotlib.rcParams['figure.subplot.wspace'] = 0.2  # horizontal spacing for subplots
matplotlib.rcParams['figure.subplot.hspace'] = 0.2  # vertical spacing for subplots
matplotlib.rcParams['figure.subplot.top'] = 0.9  # spacing between plot and title


def plot_airmass_difference_histogram(airmass_datasets, plot_title, normalize=False):
    """Plots the difference of airmass from ideal. If normalize is turned on, then it scores
    the airmasses with 0 being the worst (closest to bad airmass) and 1 being the best.

    Args:
        airmass_data [dict]: Should be a list of datasets, each dataset corresponding
            to a different airmass weighting coefficient. Assumes the first dataset passed
            is the control dataset (airmass optimization turned off).
        plot_title (str): The title of the plot.
        normalize (bool): Determines if the airmass score is normalized.

    Returns:
        fig (matplotlib.pyplot.Figure): The output figure object.
    """
    fig, ax = plt.subplots()
    fig.suptitle(plot_title)

    numbins = 10
    data = []
    labels = ['optimize by earliest']
    for dataset in airmass_datasets:
        airmass_data = dataset['airmass_metrics']['raw_airmass_data']
        airmass_coeff = dataset['airmass_weighting_coefficient']
        mp = np.array(airmass_data[0]['midpoint_airmasses'])
        a_min = np.array(airmass_data[1]['min_poss_airmasses'])
        a_max = np.array(airmass_data[2]['max_poss_airmasses'])
        if normalize:
            normed = 1 - (mp-a_min)/(a_max-a_min)
            data.append(normed[np.where((normed != 0) & (normed != 1))])
        else:
            data.append(mp-a_min)
        # the first dataset is the control dataset
        if dataset is not airmass_datasets[0]:
            labels.append(airmass_coeff)
    ax.hist(data, bins=numbins, label=labels)

    if normalize:
        ax.set_xlabel('Airmass Score (0 is worst, 1 is ideal)')
    else:
        ax.set_xlabel('Difference from Ideal Airmass (0 is ideal)')
    ax.set_ylabel('Number of Scheduled Requests')
    ax.legend(title='Airmass Coefficient')
    return fig


def plot_pct_scheduled_airmass_binned_priority(airmass_datasets, plot_title):
    """Plots the the percentage of requests scheduled for different airmass coefficients
    binned into priority levels.

    Args:
        airmass_data [dict]: Should be a list of datasets, each dataset corresponding
            to a different airmass weighting coefficient. Assumes the first dataset passed
            is the control dataset (airmass optimization turned off).
        plot_title (str): The title of the plot.

    Returns:
        fig (matplotlib.pyplot.Figure): The output figure object.
    """
    fig, ax = plt.subplots()
    fig.suptitle(plot_title)

    barwidth = 0.04
    bardata = []
    labels = ['optimize by earliest']
    # get the bin names from the first dataset, the bins should be consistent across datasets
    binnames = airmass_datasets[0]['percent_sched_by_priority'][0].keys()
    for dataset in airmass_datasets:
        priority_data = dataset['percent_sched_by_priority'][0]
        airmass_coeff = dataset['airmass_weighting_coefficient']
        bardata.append(list(priority_data.values()))
        # the first dataset is the control dataset
        if dataset is not airmass_datasets[0]:
            labels.append(airmass_coeff)
    plotutils.plot_barplot(ax, bardata, labels, binnames, barwidth)

    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent of Requests Scheduled')
    ax.set_ylim(0, 100)
    ax.legend(title='Airmass Coefficient')
    return fig
