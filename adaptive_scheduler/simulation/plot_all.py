"""
Plotting functions for an airmass optimization experiment.
"""
import argparse
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.style as style

import adaptive_scheduler.simulation.plotutils as plotutils
from adaptive_scheduler.simulation.plotutils import opensearch_client

AIRMASS_TEST_VALUES = [0, 0.01, 0.05, 0.1, 1.0, 10, 100, 1000, 1000000]
EFF_PRI_SCALING_TEST_LABELS = ['airmass', 'airmass-with-duration-scaled-100',
                               'airmass-no-duration', 'airmass-no-duration-scaled-100']

control_id = '1m0-simulation-real-airmass-control-1_2022-07-18T23:59:44.770684'
control = opensearch_client.get('scheduler-simulations', control_id)
labels = ['prioritize early']
labels.extend(AIRMASS_TEST_VALUES)
timestamp = datetime.utcnow().isoformat(timespec='seconds')
style.use('tableau-colorblind10')


def get_airmass_data_from_opensearch(coeff):
    query = f'1m0-simulation-real-airmass-coeff-{coeff}-1'
    source_data = plotutils.get_data_from_opensearch(query)
    airmass_coeff = source_data['airmass_weighting_coefficient']
    airmass_data = source_data['airmass_metrics']['raw_airmass_data']
    return airmass_data, airmass_coeff


def plot_normed_airmass_histogram():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle('1m0 Network Normalized Airmass Distributions for Different Airmass Coefficients', fontsize=20)
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
    ax.hist(normed, bins=10, label=labels, alpha=0.8)
    ax.set_xlabel('Airmass Score (0 is worst, 1 is closest to ideal)')
    ax.set_ylabel('Count')
    ax.legend()
    if not displayonly:
        plotutils.export_to_image(f'1m0_normed_airmass_hist_{timestamp}', fig)
    plt.show()


def plot_midpoint_airmass_histogram():
    fig = plt.figure(figsize=(16, 16))
    fig.suptitle('1m0 Network Midpoint Airmass Distributions for Different Airmass Coefficients', fontsize=20)
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
    if not displayonly:
        plotutils.export_to_image(f'1m0_midpoint_airmass_hist_{timestamp}', fig)
    plt.show()


def get_priority_data_from_opensearch(coeff):
    query = f'1m0-simulation-real-airmass-coeff-{coeff}-1'
    source_data = plotutils.get_data_from_opensearch(query)
    pct_scheduled = source_data['percent_sched_by_priority'][0]
    pct_duration = source_data['percent_duration_by_priority'][0]
    return pct_scheduled, pct_duration


def plot_pct_count_airmass_prio_bins():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle('1m0 Percent of Requests Scheduled by Priority Class for Different Airmass Coefficients', fontsize=20)
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

    plotutils.plot_barplot(ax, bardata, labels, priorities)
    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent of Requests Scheduled')
    ax.set_ylim(0, 100)
    ax.legend()
    if not displayonly:
        plotutils.export_to_image(f'1m0_pct_count_scheduled_airmass_{timestamp}', fig)
    plt.show()


def plot_pct_time_airmass_prio_bins():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle('1m0 Percent of Requested Time Scheduled by Priority Class for Different Airmass Coefficients', fontsize=20)
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

    plotutils.plot_barplot(ax, bardata, labels, priorities)
    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent of Requested Time Scheduled')
    ax.set_ylim(0, 100)
    ax.legend()
    if not displayonly:
        plotutils.export_to_image(f'1m0_pct_time_scheduled_airmass_{timestamp}', fig)
    plt.show()


def plot_pct_time_scaling_prio_bins():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle('1m0 Percent of Requested Time Scheduled by Priority Class for Different Scaling Options', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()

    bardata = []
    for label in EFF_PRI_SCALING_TEST_LABELS:
        priority_data = plotutils.get_data_from_opensearch(f'1m0-optimize-{label}')['percent_duration_by_priority']
        bardata.append(list(priority_data[0].values()))

    priorities = ['low priority', 'mid priority', 'high priority']
    plotutils.plot_barplot(ax, bardata, EFF_PRI_SCALING_TEST_LABELS, priorities)
    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent of Requested Time Scheduled')
    ax.set_ylim(0, 100)
    ax.legend()
    if not displayonly:
        plotutils.export_to_image(f'1m0_pct_time_scheduled_scaling_{timestamp}', fig)
    plt.show()


def plot_pct_count_scaling_prio_bins():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle('1m0 Percent of Requests Scheduled by Priority Class for Different Scaling Options', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()

    bardata = []
    for label in EFF_PRI_SCALING_TEST_LABELS:
        priority_data = plotutils.get_data_from_opensearch(f'1m0-optimize-{label}')['percent_sched_by_priority']
        bardata.append(list(priority_data[0].values()))

    priorities = ['low', 'medium', 'high']
    plotutils.plot_barplot(ax, bardata, EFF_PRI_SCALING_TEST_LABELS, priorities)
    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent of Requests Scheduled')
    ax.set_ylim(0, 100)
    ax.legend()
    if not displayonly:
        plotutils.export_to_image(f'1m0_pct_count_scheduled_scaling_{timestamp}', fig)
    plt.show()


def plot_pct_total_sched_prio_bins():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle('1m0 Percent of Requests Scheduled out of All Requests by Priority Class', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()

    bardata = []
    for label in EFF_PRI_SCALING_TEST_LABELS:
        scheduled_count = plotutils.get_data_from_opensearch(f'1m0-optimize-{label}')['scheduled_req_by_priority'][0]
        total_count = plotutils.get_data_from_opensearch(f'1m0-optimize-{label}')['total_request_count']
        scheduled_count = {bin_name: 100*np.array(values)/total_count for bin_name, values in scheduled_count.items()}
        bardata.append(scheduled_count.values())

    priorities = ['low', 'medium', 'high']
    plotutils.plot_barplot(ax, bardata, EFF_PRI_SCALING_TEST_LABELS, priorities)
    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent of Requests Scheduled out of All Requests')
    ax.set_ylim(0, 100)
    ax.legend()
    if not displayonly:
        plotutils.export_to_image(f'1m0_pct_count_total_scaling_{timestamp}', fig)
    plt.show()


def plot_pct_total_prio_bins():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle('1m0 Percent of Requests by Priority Class (both scheduled and unscheduled)', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()

    bardata = []
    scheduled_count = plotutils.get_data_from_opensearch('1m0-optimize-airmass')['total_req_by_priority'][0]
    total_count = plotutils.get_data_from_opensearch('1m0-optimize-airmass')['total_request_count']
    scheduled_count = {bin_name: 100*np.array(values)/total_count for bin_name, values in scheduled_count.items()}
    bardata.append(scheduled_count.values())

    priorities = ['low', 'medium', 'high']
    plotutils.plot_barplot(ax, bardata, EFF_PRI_SCALING_TEST_LABELS, priorities)
    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent of Requests out of All Requests')
    ax.set_ylim(0, 100)
    ax.legend()
    if not displayonly:
        plotutils.export_to_image(f'1m0_pct_count_total_scaling_{timestamp}', fig)
    plt.show()


def plot_duration_histogram():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle('1m0 Distribution of Scheduled Request Durations with/without Duration Scaling', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()
    with_duration_data = opensearch_client.get('scheduler-simulations',
                                               '1m0-optimize-airmass-with-duration_2022-07-21T21:48:02.586407')
    no_duration_data = opensearch_client.get('scheduler-simulations',
                                             '1m0-optimize-airmass-no-duration_2022-07-21T21:52:46.316207')
    duration_data = [with_duration_data['_source']['raw_scheduled_durations']]
    duration_data.append(no_duration_data['_source']['raw_scheduled_durations'])
    labels = ['eff. prio scaled by duration', 'eff. prio not scaled by duration']
    ax.hist(duration_data, bins=50, label=labels)
    ax.set_xlabel('Duration [s]')
    ax.set_ylabel('Counts')
    ax.set_title('Optimize by Airmass')
    ax.legend()
    if not displayonly:
        plotutils.export_to_image(f'1m0_duration_hist_{timestamp}', fig)
    plt.show()


def plot_eff_prio_duration_scatter():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle('1m0 Scatterplot of Effective Priority and Duration', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()
    tagnames = ['with-duration-v2', 'with-duration-scaled-100-v2']
    labels = ['priority 10-30', 'priority 10-100']
    for i, tag in enumerate(tagnames):
        data = plotutils.get_data_from_opensearch(f'1m0-optimize-airmass-{tag}')
        prio_scheduled = np.array(data['raw_scheduled_priorities'])
        prio_unscheduled = np.array(data['raw_unscheduled_priorities'])
        dur_scheduled = np.array(data['raw_scheduled_durations'])
        dur_unscheduled = np.array(data['raw_unscheduled_durations'])
        ax.scatter(dur_scheduled, prio_scheduled*dur_scheduled, label=f'{labels[i]}, scheduled', marker='+')
        ax.scatter(dur_unscheduled, prio_unscheduled*dur_unscheduled, label=f'{labels[i]}, unscheduled', marker='x')
    ax.set_ylabel('Effective Priority (base priority x duration)')
    ax.set_xlabel('Duration [s]')
    ax.legend()
    if not displayonly:
        plotutils.export_to_image(f'1m0_eff_prio_duration_scatter_{timestamp}', fig)
    plt.show()


if __name__ == '__main__':
    plots = {
        'normed_airmass_hist': {'func': plot_normed_airmass_histogram,
                                'desc': 'Airmass distribution, normalized so that 0 is worst airmass and 1 is best'},
        'midpoint_airmass_hist': {'func': plot_midpoint_airmass_histogram,
                                  'desc': 'Midpoint airmass distributions for different airmass weighting coefficients'},
        'pct_sched_airmass_bin_priority': {'func': plot_pct_count_airmass_prio_bins,
                                           'desc': 'Percent of requests scheduled binned by priority level'
                                                   'for different airmass coefficients'},
        'pct_time_airmass_bin_priority': {'func': plot_pct_time_airmass_prio_bins,
                                          'desc': 'Percent of time requested scheduled binned by priority level'
                                                  ' for different airmass coefficients'},
        'pct_sched_scaling_bin_priority': {'func': plot_pct_count_scaling_prio_bins,
                                           'desc': 'Percent of requests scheduled binned by priority level'
                                                   ' for different scaling strategies'},
        'pct_time_scaling_bin_priority': {'func': plot_pct_time_scaling_prio_bins,
                                          'desc': 'Percent of time requested scheduled binned by priority level'
                                                  ' for different scaling strategies'},
        'pct_total_sched_scaling_bin_priority': {'func': plot_pct_total_sched_prio_bins,
                                                 'desc': 'Percent of requests scheduled with respect to all requests, '
                                                 'binned by priority level for different scaling strategies'},
        'pct_total_scaling_bin_priority': {'func': plot_pct_total_prio_bins,
                                           'desc': 'The percent of requests occupied at each priority level'},
        'duration_hist': {'func': plot_duration_histogram,
                          'desc': 'Scheduled request duration distribution.'},
        'eff_prio_duration_scatter': {'func': plot_eff_prio_duration_scatter,
                                      'desc': 'Scatterplot with (prio x duration) on y-axis and duration on x-axis'},
    }

    description = 'Plotting functions for airmass optimization experiment.'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('plot_name', type=str.lower, nargs='*',
                        help="The name of the plot(s) to display. `all` can be passed to show all.")
    parser.add_argument('-l', '--list', help='Show plot info. `-l all` to show all available plots.', action='store_true')
    parser.add_argument('-d', '--displayonly', help='Display the plots without exporting them.', action='store_true')
    args = parser.parse_args()
    global displayonly
    displayonly = args.displayonly

    if args.list:
        spacing = max([len(name) for name in plots.keys()]) + 4
        print(f'{"NAME":{spacing}}DESCRIPTION')
        print(f'{"====":{spacing}}===========')
        for name, details in plots.items():
            print(f'{name:{spacing}}{details["desc"]}')
    else:
        plots_to_show = list(plots.keys()) if args.plot_name == ['all'] else args.plot_name
        for plot_name in plots_to_show:
            plots[plot_name]['func']()