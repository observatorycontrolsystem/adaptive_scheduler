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


def plot_percent_sched_requests_bin_by_priority(eff_pri_datasets, plot_title):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(25, 12))
    fig.suptitle(plot_title)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    bardata1 = []
    labels1 = []
    for dataset in eff_pri_datasets:
        bardata1.append(list(dataset['percent_duration_by_priority'][0].values()))
        labels1.append(dataset['simulation_id'][21:-3])
    priorities = ['low priority', 'mid priority', 'high priority']
    plotutils.plot_barplot(ax1, bardata1, labels1, priorities)
    ax1.set_xlabel('Priority')
    ax1.set_ylabel('Percent Scheduled Time')
    ax1.set_title('Percent Duration Scheduled')
    ax1.legend()
    bardata2 = []
    labels2 = []
    for dataset in eff_pri_datasets:
        bardata2.append(list(dataset['percent_sched_by_priority'][0].values()))
        labels2.append(dataset['simulation_id'][21:])
    priorities = ['low priority', 'mid priority', 'high priority']
    plotutils.plot_barplot(ax2, bardata2, labels2, priorities)
    ax2.set_xlabel('Priority')
    ax2.set_ylabel('Percent Scheduled Count')
    ax2.set_title('Percent Number Scheduled')
    ax2.legend()
    plt.show()
    

def rand_jitter(arr):
    stdev = .01 * (max(arr) - min(arr))
    return arr + np.random.randn(len(arr)) * stdev
    
def plot_sched_priority_duration_dotplot(eff_pri_datasets, plot_title):
    markers = ["o" , "," ,"v" , "^" , "<", ">"]
    colors = ['r','b','c','m', 'y', 'k']
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(28, 12))
    fig.suptitle(plot_title)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    for i, data in enumerate(eff_pri_datasets):
        id = data['simulation_id'][21:]
        if id in ['with-duration-scaled-100-v3', 'no-duration-scaled-100-v3']:
            data['raw_scheduled_priorities'] = [(p+35)/4.5 for p in data['raw_scheduled_priorities']]
        ax1.scatter(rand_jitter(data['raw_scheduled_priorities']), rand_jitter(data['raw_scheduled_durations']), 
                   marker = markers[i],c = colors[i], s = 10, label = f'scheduled requests {id}',alpha = 0.3) 
    ax1.set_ylim(top=11000)
    ax1.set_xlabel('Priority')
    ax1.set_ylabel('Request Duration')
    ax1.legend()
    for i, data in enumerate(eff_pri_datasets):
        id = data['simulation_id'][21:]
        if id in ['with-duration-scaled-100-v3', 'no-duration-scaled-100-v3']:
            data['raw_unscheduled_priorities'] = [(p+35)/4.5 for p in data['raw_unscheduled_priorities']]
        ax2.scatter(rand_jitter(data['raw_unscheduled_priorities']), rand_jitter(data['raw_unscheduled_durations']),
                   c =colors[i], marker=markers[i],s=10, label = f'unscheduled requests {id}', alpha = 0.3)
    ax2.set_ylim(top=11000)
    ax2.set_xlabel('Priority')
    ax2.set_ylabel('Request Duration')
    ax2.legend()
    plt.show(block = False)
    plt.show()


def plot_heat_map_priority_duration(eff_pri_datasets, plot_title):
    fig, axs= plt.subplots(2, 2, figsize=(13, 12))
    fig.suptitle(plot_title)
    fig.subplots_adjust(wspace=0.01, hspace=0.01, top=0.9)
    ax_list = [axs[0,0],axs[0,1],axs[1,0], axs[1,1]]
    for i, data in enumerate(eff_pri_datasets):
        id = data['simulation_id'][21:]
        if id in ['with-duration-scaled-100-v3', 'no-duration-scaled-100-v3']:
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
        for i in range(len(priority_bins)):
            for j in range(len(duration_bins)):
                value = heat_map_elements[i, j]
                text1 = axis.text(j, i, f'{heat_map_elements[i, j]}|{ heat_map_elements_unsched[i, j]}',
                            ha="center", va="center", fontsize='large', fontweight='semibold', color=cmap2(0.001/value))
        axis.set_title(f'{id} (sched|unsched)', fontweight='semibold')
    fig.tight_layout()
    plt.show()