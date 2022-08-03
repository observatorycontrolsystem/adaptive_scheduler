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
from adaptive_scheduler.simulation.metrics import bin_data
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
    """Plots a barplot of the percentage of requests scheduled for different airmass coefficients
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
    plotutils.plot_multi_barplot(ax, bardata, labels, binnames)

    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent of Requests Scheduled')
    ax.set_ylim(0, 100)
    ax.legend(title='Airmass Coefficient')
    return fig


def plot_percent_sched_requests_bin_by_priority(eff_pri_datasets, plot_title):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(25, 12))
    fig.suptitle(plot_title)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    bardata1 = []
    labels = ['with duration', 'no duration', 'with duration scaled 100', 'no duration scaled 100']
    for dataset in eff_pri_datasets:
        bardata1.append(list(dataset['percent_duration_by_priority'][0].values()))
        
    priorities = ['low priority(10-19)', 'mid priority(20-29)', 'high priority(30)']
    plotutils.plot_multi_barplot(ax1, bardata1, labels, priorities)
    ax1.set_xlabel('Priority')
    ax1.set_ylabel('Scheduled Time/Total Request Time (%)')
    ax1.set_title('Percent of requested time scheduled')
    ax1.legend(title='Effective Priority Algorithms')
    bardata2 = []
    for dataset in eff_pri_datasets:
        bardata2.append(list(dataset['percent_sched_by_priority'][0].values()))
    priorities = ['low priority', 'mid priority', 'high priority']
    plotutils.plot_multi_barplot(ax2, bardata2, labels, priorities)
    ax2.set_xlabel('Priority')
    ax2.set_ylabel('Scheduled Requests/Total Requests (%)')
    ax2.set_title('Percent of requests Scheduled')
    ax2.legend(title='Effective Priority Algorithms')
    plt.show()
    
    
def plot_sched_priority_duration_dotplot(eff_pri_datasets, plot_title):
    def rand_jitter(arr):
        stdev = .01 * (max(arr) - min(arr))
        return arr + np.random.randn(len(arr)) * stdev
    
    markers = ["o" , "," ,"v" , "^" , "<", ">"]
    colors = ['r','b','c','m', 'y', 'k']
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(28, 12))
    fig.suptitle(plot_title)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    labels = ['with duration', 'no duration', 'with duration scaled 100', 'no duration scaled 100']
    for i, data in enumerate(eff_pri_datasets):
        id = data['simulation_id']
        if id in ['airmass-0.1-w-duration-w-scaling','airmass-0.1-no-duration-w-scaling']:
            data['raw_scheduled_priorities'] = [(p+35)/4.5 for p in data['raw_scheduled_priorities']]   
        data['raw_scheduled_durations'] = [d/60 for d in data['raw_scheduled_durations']]                                
        ax1.scatter(rand_jitter(data['raw_scheduled_priorities']), rand_jitter(data['raw_scheduled_durations']), 
                   marker = markers[i],c = colors[i], s=10, label=labels[i], alpha=0.3) 
    ax1.set_ylim(top=100)
    ax1.set_xlabel('Priority')
    ax1.set_ylabel('Request Duration (minutes)')
    ax1.set_title('Scheduled Reservations distribution')
    ax1.legend(title='Effective Priority Algorithms')
    for i, data in enumerate(eff_pri_datasets):
        id = data['simulation_id']
        if id in ['airmass-0.1-w-duration-w-scaling', 'airmass-0.1-no-duration-w-scaling']:
            data['raw_unscheduled_priorities'] = [(p+35)/4.5 for p in data['raw_unscheduled_priorities']]
        data['raw_unscheduled_durations'] = [d/60 for d in data['raw_unscheduled_durations']]
        ax2.scatter(rand_jitter(data['raw_unscheduled_priorities']), rand_jitter(data['raw_unscheduled_durations']),
                   c=colors[i], marker=markers[i],s=10, label=labels[i], alpha=0.3)
    ax2.set_ylim(top=100)
    ax2.set_xlabel('Priority')
    ax2.set_ylabel('Request Duration (minutes)')
    ax2.set_title('Unscheduled Reservations distribution')
    ax2.legend(title='Effective Priority Algorithms')
    plt.show(block = False)
    plt.show()


def plot_heat_map_priority_duration(eff_pri_datasets, plot_title):
    fig, axs= plt.subplots(2, 2, figsize=(13, 12))
    fig.suptitle(plot_title)
    fig.subplots_adjust(wspace=0.01, hspace=0.01, top=0.9)
    ax_list = [axs[0,0],axs[0,1],axs[1,0], axs[1,1]]
    labels = ['with duration', 'no duration', 'with duration scaled 100', 'no duration scaled 100']
    for i, data in enumerate(eff_pri_datasets):
        id = data['simulation_id']
        if id in ['airmass-0.1-w-duration-w-scaling', 'airmass-0.1-no-duration-w-scaling']:
            data['raw_scheduled_priorities'] = [(p+35)/4.5 for p in data['raw_scheduled_priorities']]
            data['raw_unscheduled_priorities'] = [(p+35)/4.5 for p in data['raw_unscheduled_priorities']]
        sched_priorities = data['raw_scheduled_priorities']
        sched_durations = data['raw_scheduled_durations']
        unsched_priorities = data['raw_unscheduled_priorities']
        unsched_durations = data['raw_unscheduled_durations']
        level_1_bins = bin_data(sched_priorities, sched_durations, bin_size=4, bin_range=(10,30),aggregator=None)
        level_2_bins = {
            bin_key: bin_data(bin_values, bin_size=300, bin_range=(0, 1499)) | bin_data(bin_values, bin_size=3000, bin_range=(1500, 4000))
            for bin_key, bin_values in level_1_bins.items()
        } 
        level_1_bins_unsched = bin_data(unsched_priorities, unsched_durations, bin_size=4, bin_range=(10,30),aggregator=None)
        level_2_bins_unsched = {
            bin_key: bin_data(bin_values, bin_size=300, bin_range=(0, 1499)) | bin_data(bin_values, bin_size=3000, bin_range=(1500, 4000))
            for bin_key, bin_values in level_1_bins_unsched.items()
        }   
        heat_map_elements = []
        heat_map_elements_unsched = []
        for values in level_2_bins.values():
            heat_map_elements.append(list(values.values()))
        for values in level_2_bins_unsched.values():
            heat_map_elements_unsched.append(list(values.values()))  
        priority_bins = list(level_2_bins.keys())
        duration_bins = ['0-5','5-10','10-15', '15-20', '20-25', '25&up']
        heat_map_elements = np.array(heat_map_elements)
        heat_map_elements_unsched = np.array(heat_map_elements_unsched)
        axis = ax_list[i]
        cmap=plt.get_cmap('coolwarm')
        cmap2 = plt.get_cmap('gray')
        heatplot = axis.imshow(heat_map_elements,cmap=cmap)
        axis.set_ylabel('Priority')
        axis.set_xlabel('Duration (minutes)')
        axis.set_xticks(np.arange(len(duration_bins)), labels=duration_bins)
        axis.set_yticks(np.arange(len(priority_bins)), labels=priority_bins)
        plt.setp(axis.get_xticklabels(), rotation=45, ha="right",
            rotation_mode="anchor")
        for j in range(len(priority_bins)):
            for k in range(len(duration_bins)):
                value = heat_map_elements[j, k]
                text1 = axis.text(k, j, f'{heat_map_elements[j, k]}|{ heat_map_elements_unsched[j, k]}',
                            ha="center", va="center", fontsize='large', fontweight='semibold', color=cmap2(0.001/value))
        axis.set_title(f'{labels[i]} (sched|unsched)', fontweight='semibold')
    fig.tight_layout()
    plt.show()
    
    
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
    plotutils.plot_multi_barplot(ax, bardata, labels, binnames)

    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent of Requested Time Scheduled')
    ax.set_ylim(0, 100)
    ax.legend(title='Airmass Coefficient')
    return fig


def plot_pct_scheduled_airmass_lineplot(airmass_datasets, plot_title):
    """Plots a line chart with percent of requests scheduled on the y-axis and airmass
    coefficient on the x-axis. The priority bins are highlighted in different colors.

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

    prio_names = list(airmass_datasets[0]['percent_sched_by_priority'][0].keys())
    airmass_coeffs = []
    pct_scheduled = []
    # exclude the control dataset
    for dataset in airmass_datasets[1:]:
        data_by_priority = dataset['percent_sched_by_priority'][0]
        airmass_coeffs.append(dataset['airmass_weighting_coefficient'])
        pct_scheduled.append(list(data_by_priority.values()))
    data_by_airmass = np.array(pct_scheduled).transpose()
    for i, data in enumerate(data_by_airmass):
        ax.plot(airmass_coeffs, data, label=prio_names[i])
    ax.set_xlabel('Airmass Coefficient')
    ax.set_ylabel('Percent of Requests Scheduled')
    ax.set_ylim(0, 100)
    ax.legend(title='Priority')
    return fig


def plot_pct_time_scheduled_airmass_lineplot(airmass_datasets, plot_title):
    """Plots a line chart with percent of requested time scheduled on the y-axis and airmass
    coefficient on the x-axis. The priority bins are highlighted in different colors.

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

    prio_names = list(airmass_datasets[0]['percent_duration_by_priority'][0].keys())
    prio_names.append('all')
    airmass_coeffs = []
    pct_scheduled = []
    # exclude the control dataset
    for dataset in airmass_datasets[1:]:
        sched_by_priority = np.array(list(dataset['scheduled_seconds_by_priority'][0].values()))
        total_by_priority = np.array(list(dataset['total_seconds_by_priority'][0].values()))
        airmass_coeffs.append(dataset['airmass_weighting_coefficient'])
        pct_by_priority = sched_by_priority/total_by_priority * 100
        pct_cumulative = np.sum(sched_by_priority)/np.sum(total_by_priority) * 100
        pct_scheduled.append(np.append(pct_by_priority, pct_cumulative))
    data_by_airmass = np.array(pct_scheduled).transpose()
    for i, data in enumerate(data_by_airmass):
        ax.plot(airmass_coeffs, data, label=prio_names[i])
    ax.set_xlabel('Airmass Coefficient')
    ax.set_ylabel('Percent of Requested Time Scheduled')
    ax.set_ylim(0, 100)
    ax.legend(title='Priority')
    return fig


def plot_pct_time_scheduled_out_of_available(airmass_datasets, plot_title):
    """Plots a line chart with percent of requested time scheduled out of all availabel time
    on the y-axis and airmass coefficient on the x-axis. The priority bins are highlighted
    in different colors.

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

    prio_names = list(airmass_datasets[0]['percent_duration_by_priority'][0].keys())
    prio_names.append('all')
    airmass_coeffs = []
    pct_scheduled = []
    # exclude the control dataset
    for dataset in airmass_datasets[1:]:
        sched_by_priority = np.array(list(dataset['scheduled_seconds_by_priority'][0].values()))
        available_time = dataset['total_available_seconds']
        airmass_coeffs.append(dataset['airmass_weighting_coefficient'])
        pct_by_priority = sched_by_priority/available_time * 100
        pct_cumulative = np.sum(sched_by_priority)/available_time * 100
        pct_scheduled.append(np.append(pct_by_priority, pct_cumulative))
    data_by_airmass = np.array(pct_scheduled).transpose()
    for i, data in enumerate(data_by_airmass):
        ax.plot(airmass_coeffs, data, label=prio_names[i])
    ax.set_xlabel('Airmass Coefficient')
    ax.set_ylabel('Percent of Requested Time Scheduled')
    ax.set_ylim(0, 100)
    ax.legend(title='Priority')
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


def plot_duration_by_window_duration_scatter(data, plot_title):
    """Plots a scatterplot with observation duration on the y-axis and maximum window length per
    observation on the x-axis.

    Args:
        data (dict): The dataset for this metric. Expects one dataset.
        plot_title (str): The title of the plot.

    Returns:
        fig (matplotlib.pyploy.Figure): The output Figure object.
    """
    fig, ax = plt.subplots()
    fig.suptitle(plot_title)
    data = data[0]
    sec_to_min = 1/60
    window_dur = np.array(data['raw_window_durations']) * sec_to_min
    sched_dur = np.array(data['raw_scheduled_durations']) * sec_to_min
    ax.scatter(window_dur, sched_dur, s=4)
    ax.set_ylabel('Request Duration [min]')
    ax.set_xlabel('Longest Possible Window Duration [min]')

    return fig


def plot_input_duration_binned_priority(dataset, plot_title):
    fig, ax = plt.subplots()
    fig.suptitle(plot_title)
    bardata = []
    input_durations = dataset[0]['raw_scheduled_durations'] + dataset[0]['raw_unscheduled_durations']
    input_priorities = dataset[0]['raw_scheduled_priorities'] + dataset[0]['raw_unscheduled_priorities']
    input_bins = metrics.bin_data(input_priorities, input_durations, bin_size=10, bin_range=(10,30),aggregator=None)
    duration_bins = {
        bin_key: metrics.bin_data(bin_values, bin_size=300, bin_range=(0, 1499)) | metrics.bin_data(bin_values, bin_size=10000, bin_range=(1500, 10000))
        for bin_key, bin_values in input_bins.items()
    } 
    labels = ['10-19', '20-29', '30']
    for values in duration_bins.values():
        bardata.append(list(values.values()))
    binnames = ['0-5','5-10','10-15', '15-20', '20-25', '25&up']
    plotutils.plot_multi_barplot(ax, bardata, labels, binnames, barwidth=0.1)
    ax.set_xlabel('Duration (minutes)')
    ax.set_ylabel('Input reservation counts')
    ax.set_ylim(0, 300)
    ax.legend(title='Priority')
    return fig


def plot_subplots_input_duration(dataset, plot_title):
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(28,10))
    fig.suptitle(plot_title)
    sched_durations = dataset[0]['raw_scheduled_durations']
    unsched_durations = dataset[0]['raw_unscheduled_durations']
    sched_priorities = dataset[0]['raw_scheduled_priorities']
    unsched_priorities = dataset[0]['raw_unscheduled_priorities']

    input_durations = sched_durations + unsched_durations
    input_priorities = sched_priorities + unsched_priorities
    input_bins = metrics.bin_data(input_priorities, input_durations, bin_size=10, bin_range=(10,30),aggregator=None)
    sched_bins = metrics.bin_data(sched_priorities, sched_durations, bin_size=10, bin_range=(10,30),aggregator=None)
    unsched_bins = metrics.bin_data(unsched_priorities, unsched_durations, bin_size=10, bin_range=(10,30),aggregator=None)
    labels = ['10-19', '20-29', '30']
    axis = [ax1, ax2, ax3]
    for i, values in enumerate(sched_bins.values()):
        bars = ['Scheduled', 'Unscheduled']
        # axis[i].hist(values, bins = np.arange(0, 4000, 120))
        axis[i].hist([values,list(unsched_bins.values())[i]], bins = np.arange(0, 4000, 120), 
                      stacked = True, label = bars)
        # axis[i].hist(list(unsched_bins.values())[i], bins = np.arange(0, 4000, 120))
        axis[i].set_xlabel('Duration (seconds)')
        axis[i].set_ylabel('Input reservation counts')
        axis[i].set_ylim(0, 300)
        axis[i].set_title(f'{labels[i]} Priority binned by duration')
        axis[i].legend()
    return fig
