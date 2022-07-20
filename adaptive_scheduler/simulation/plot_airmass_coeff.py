from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt

import adaptive_scheduler.simulation.plotutils as plotutils
from adaptive_scheduler.simulation.plotutils import opensearch_client, default_colors

AIRMASS_TEST_VALUES = [0, 0.01, 0.05, 0.1, 1.0, 10, 100, 1000, 1000000]

control_id = '1m0-simulation-real-airmass-control-1_2022-07-18T23:59:44.770684'
control = opensearch_client.get('scheduler-simulations', control_id)
labels = ['prioritize early']
labels.extend(AIRMASS_TEST_VALUES)
timestamp = datetime.utcnow().isoformat(timespec='seconds')


def get_airmass_data_from_opensearch(coeff):
    query = f'1m0-simulation-real-airmass-coeff-{coeff}-1'
    source_data = plotutils.get_data_from_opensearch(query)
    airmass_coeff = source_data['airmass_weighting_coefficient']
    airmass_data = source_data['airmass_metrics']['raw_airmass_data']
    return airmass_data, airmass_coeff


def plot_normed_airmass_histogram():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle('1m0 Network Normalized Airmass Distributions', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()

    control_airmass_data = control['_source']['airmass_metrics']['raw_airmass_data']
    control_mp = np.array(control_airmass_data[0]['midpoint_airmasses'])
    control_min = np.array(control_airmass_data[1]['min_poss_airmasses'])
    control_max = np.array(control_airmass_data[2]['max_poss_airmasses'])
    normed = [1-(control_mp-control_min)/(control_max-control_min)]

    for value in AIRMASS_TEST_VALUES:
        airmass_data, airmass_coeff = get_airmass_data_from_opensearch(value)
        mp = np.array(airmass_data[0]['midpoint_airmasses'])
        min_ = np.array(airmass_data[1]['min_poss_airmasses'])
        max_ = np.array(airmass_data[2]['max_poss_airmasses'])
        normed.append(1-(mp-min_)/(max_-min_))
    ax.hist(normed, bins=10, label=labels, color=default_colors, alpha=0.8)
    ax.set_xlabel('Airmass Score (0 is worst, 1 is closest to ideal)')
    ax.set_ylabel('Count')
    ax.legend()
    plotutils.export_to_image(f'1m0_normed_airmass_hist_{timestamp}', fig)
    plt.show()


def plot_midpoint_airmass_histogram():
    fig = plt.figure(figsize=(16, 16))
    fig.suptitle('1m0 Network Midpoint Airmass Distributions', fontsize=20)
    fig.subplots_adjust(wspace=0.3, hspace=0.3, top=0.92)
    for i, value in enumerate(AIRMASS_TEST_VALUES):
        ax = fig.add_subplot(3, 3, i+1)
        airmass_data, airmass_coeff = get_airmass_data_from_opensearch(value)
        midpoint_airmasses = airmass_data[0]['midpoint_airmasses']
        ax.hist(midpoint_airmasses, bins=50)
        ax.set_title(f'Airmass Coefficient: {airmass_coeff}')
        ax.set_xlabel('Midpoint Airmass')
        ax.set_ylabel('Count')
        ax.set_xlim(1.0, 2.0)
        ax.set_ylim(0, 120)
    plotutils.export_to_image(f'1m0_midpoint_airmass_hist_{timestamp}', fig)
    plt.show()


def get_priority_data_from_opensearch(coeff):
    query = f'1m0-simulation-real-airmass-coeff-{coeff}-1'
    source_data = plotutils.get_data_from_opensearch(query)
    pct_scheduled = source_data['percent_sched_by_priority'][0]
    pct_duration = source_data['percent_duration_by_priority'][0]
    return pct_scheduled, pct_duration


def plot_pct_scheduled_bins():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle('1m0 Network Percentage of Requests Scheduled', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()

    bardata = []
    control_prio_data = control['_source']['percent_sched_by_priority'][0]
    priorities = list(control_prio_data.keys())
    percentages = list(control_prio_data.values())
    bardata.append(percentages)

    for value in AIRMASS_TEST_VALUES:
        priority_data, _ = get_priority_data_from_opensearch(value)
        bardata.append(list(priority_data.values()))

    plotutils.plot_barplot(ax, bardata, default_colors, labels, priorities)
    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent of Requests Scheduled')
    ax.legend()
    plotutils.export_to_image(f'1m0_pct_count_scheduled_{timestamp}', fig)
    plt.show()


def plot_pct_time_scheduled_bins():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle('1m0 Network Percentage Requested Time Scheduled', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()

    bardata = []
    control_prio_data = control['_source']['percent_duration_by_priority'][0]
    priorities = list(control_prio_data.keys())
    percentages = list(control_prio_data.values())
    bardata.append(percentages)

    for value in AIRMASS_TEST_VALUES:
        _, priority_data = get_priority_data_from_opensearch(value)
        bardata.append(list(priority_data.values()))

    plotutils.plot_barplot(ax, bardata, default_colors, labels, priorities)
    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent Time Scheduled')
    ax.legend()
    plotutils.export_to_image(f'1m0_pct_time_scheduled_{timestamp}', fig)
    plt.show()


if __name__ == '__main__':
    plot_midpoint_airmass_histogram()
    plot_normed_airmass_histogram()
    plot_pct_scheduled_bins()
    plot_pct_time_scheduled_bins()
