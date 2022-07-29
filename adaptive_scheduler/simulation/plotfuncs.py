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
import adaptive_scheduler.simulation.metrics as metrics

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
    """Plots the percentage of requests scheduled for different airmass coefficients
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
    plotutils.plot_barplot(ax, bardata, labels, binnames)

    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent of Requests Scheduled')
    ax.set_ylim(0, 100)
    ax.legend(title='Airmass Coefficient')
    return fig


def plot_pct_time_scheduled_airmass_binned_priority(airmass_datasets, plot_title):
    """Plots the percentage of requested time scheduled for different airmass coefficients
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

    bardata = []
    labels = ['optimize by earliest']
    # get the bin names from the first dataset, the bins should be consistent across datasets
    binnames = airmass_datasets[0]['percent_sched_by_priority'][0].keys()
    for dataset in airmass_datasets:
        priority_data = dataset['percent_duration_by_priority'][0]
        airmass_coeff = dataset['airmass_weighting_coefficient']
        bardata.append(list(priority_data.values()))
        # the first dataset is the control dataset
        if dataset is not airmass_datasets[0]:
            labels.append(airmass_coeff)
    plotutils.plot_barplot(ax, bardata, labels, binnames)

    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent of Requested Time Scheduled')
    ax.set_ylim(0, 100)
    ax.legend(title='Airmass Coefficient')
    return fig


def plot_midpoint_airmass_histograms(airmass_datasets, plot_title):
    """Plots a distribution of midpoint airmasses for each different airmass coefficient.

    Args:
        airmass_data [dict]: Should be a list of datasets, each dataset corresponding
            to a different airmass weighting coefficient. Assumes the first dataset passed
            is the control dataset (airmass optimization turned off).
        plot_title (str): The title of the plot.

    Returns:
        fig (matplotlib.pyplot.Figure): The output figure object.
    """
    fig = plt.figure(figsize=(16, 16))
    fig.suptitle(plot_title)
    fig.subplots_adjust(wspace=0.3, hspace=0.3, top=0.92)
    for i, dataset in enumerate(airmass_datasets[1:]):
        ax = fig.add_subplot(3, 3, i+1)
        midpoint_airmasses = dataset['airmass_metrics']['raw_airmass_data'][0]['midpoint_airmasses']
        airmass_coeff = dataset['airmass_weighting_coefficient']
        ax.hist(midpoint_airmasses, bins=50)
        ax.set_title(f'Airmass Coefficient: {airmass_coeff}')
        ax.set_xlabel('Midpoint Airmass')
        ax.set_ylabel('Count')
        ax.set_xlim(1.0, 2.0)
        ax.set_ylim(0, 120)
    return fig


def plot_eff_priority_duration_scatter(datasets, plot_title):
    """Plots a scatterplot with effective priority on the y-axis and duration on the x-axis.

    Args:
        datasets [dict]: A list of datasets. Expects one dataset for priority range 10-30 and one dataset
            for priority scaled to 10-100.
        plot_title (str): The title of the plot.

    Returns:
        fig (matplotlib.pyplot.Figure): The output figure object.
    """
    fig, axs = plt.subplots(1, 2, figsize=(24, 8))
    fig.suptitle(plot_title)
    labels = ['Priority 10-30', 'Priority 10-100']
    # colors are from tableau-colorblind10
    colors = [('#006BA4', '#5F9ED1'), ('#C85200', '#FF800E')]
    for i, ax in enumerate(axs):
        data = datasets[i]
        prio_scheduled = np.array(data['raw_scheduled_priorities'])
        prio_unscheduled = np.array(data['raw_unscheduled_priorities'])
        dur_scheduled = np.array(data['raw_scheduled_durations'])/60
        dur_unscheduled = np.array(data['raw_unscheduled_durations'])/60
        ax.scatter(dur_scheduled, prio_scheduled*dur_scheduled,
                   label='scheduled', marker='x', color=colors[i][0])
        ax.scatter(dur_unscheduled, prio_unscheduled*dur_unscheduled,
                   label='unscheduled', marker='x', alpha=0.5, color=colors[i][1])
        ax.set_ylabel('Effective Priority (base priority x duration)')
        ax.set_xlabel('Duration [min]')
        ax.set_title(f'Optimize by Airmass, With Duration, {labels[i]}')
        ax.legend(title=labels[i])
    return fig
