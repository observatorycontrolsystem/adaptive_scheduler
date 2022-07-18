import numpy as np
import matplotlib.pyplot as plt
from opensearchpy import OpenSearch

OPENSEARCH_URL = 'https://logs.lco.global/'
AIRMASS_TEST_VALUES = [0, 0.01, 0.05, 0.1, 1, 10, 100, 1000, 1000000]
USE_1m_ONLY = True

client = OpenSearch(OPENSEARCH_URL)
control_id = ('test-real-airmass-coeff-default-1-1m0_2022-07-18T16:56:27.411946' if USE_1m_ONLY
              else 'simulation-real-prefer-earliest-1_2022-07-15T23:56:48.471472')
control = client.get('scheduler-simulations', control_id)
colors = ['deeppink',
          'forestgreen',
          'limegreen',
          'mediumseagreen',
          'mediumturquoise',
          'royalblue',
          'slateblue',
          'darkorchid',
          'indigo',
          'navy']
labels = ['earliest']
labels.extend(AIRMASS_TEST_VALUES)
search_suffix = '1m0' if USE_1m_ONLY else ''


def get_airmass_data_from_opensearch(coeff):
    query = {'query': {
        'wildcard': {'simulation_id.keyword': f'*-real-airmass-coeff-{coeff}-1-{search_suffix}'}
        }
    }
    response = client.search(query, 'scheduler-simulations')
    try:
        result = response['hits']['hits'][0]
    except IndexError:
        print(f'Found no results for {coeff}')
    source_data = result['_source']
    print(f'Got data for {source_data["simulation_id"]}')
    airmass_coeff = source_data['airmass_weighting_coefficient']
    airmass_data = source_data['airmass_metrics']['raw_airmass_data']
    return airmass_data, airmass_coeff


def plot_normed_airmass_histogram():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle(f'{search_suffix} Normalized Airmass Distributions (midpoint/ideal)', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()

    control_airmass_data = control['_source']['airmass_metrics']['raw_airmass_data']
    normed = [np.divide(np.array(control_airmass_data[0]['midpoint_airmasses']),
                        np.array(control_airmass_data[1]['ideal_airmasses']))]

    for value in AIRMASS_TEST_VALUES:
        airmass_data, airmass_coeff = get_airmass_data_from_opensearch(value)
        midpoint_airmasses = np.array(airmass_data[0]['midpoint_airmasses'])
        ideal_airmasses = np.array(airmass_data[1]['ideal_airmasses'])
        normed.append(np.divide(midpoint_airmasses, ideal_airmasses))
    ax.hist(normed, bins=30, range=(1, 1.2), label=labels, color=colors, alpha=0.8)
    ax.set_xlabel('Airmass Ratio (midpoint/ideal)')
    ax.set_ylabel('Count')
    fig.legend()
    plt.show()


def plot_midpoint_airmass_histogram():
    fig = plt.figure(figsize=(16, 16))
    fig.suptitle(f'{search_suffix} Midpoint Airmass Distributions', fontsize=20)
    fig.subplots_adjust(wspace=0.3, hspace=0.3, top=0.92)
    for i, value in enumerate(AIRMASS_TEST_VALUES):
        ax = fig.add_subplot(3, 3, i+1)
        airmass_data, airmass_coeff = get_airmass_data_from_opensearch(value)
        midpoint_airmasses = airmass_data[0]['midpoint_airmasses']
        ax.hist(midpoint_airmasses, bins=50)
        ax.set_title(f'Airmass Coefficient: {airmass_coeff}')
        ax.set_xlabel('Midpoint Airmass')
        ax.set_ylabel('Count')
    plt.show()


def plot_barplot(ax, data, colors, labels, binnames):
    # data is a list of lists
    ticks = np.arange(len(data[0]))
    barwidth = 0.05
    for i, datavalues in enumerate(data):
        ax.bar(ticks+barwidth*i, datavalues, barwidth, color=colors[i], label=labels[i], alpha=0.8)
    ax.set_xticks(ticks+barwidth*i/2, binnames)


def get_priority_data_from_opensearch(coeff):
    query = {'query': {
        'wildcard': {'simulation_id.keyword': f'*-real-airmass-coeff-{coeff}-1-{search_suffix}'}
        }
    }
    response = client.search(query, 'scheduler-simulations')
    try:
        result = response['hits']['hits'][0]
    except IndexError:
        print(f'Found no results for {coeff}')
    source_data = result['_source']
    print(f'Got data for {source_data["simulation_id"]}')
    airmass_coeff = source_data['airmass_weighting_coefficient']
    pct_scheduled = source_data['percent_sched_by_priority'][0]
    pct_duration = source_data['percent_duration_by_priority'][0]
    return pct_scheduled, pct_duration, airmass_coeff


def plot_pct_scheduled_bins():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle(f'{search_suffix} Percentage of Requests Scheduled', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()

    bardata = []
    control_prio_data = control['_source']['percent_sched_by_priority'][0]
    priorities = list(control_prio_data.keys())
    percentages = list(control_prio_data.values())
    bardata.append(percentages)

    for value in AIRMASS_TEST_VALUES:
        priority_data, _, _ = get_priority_data_from_opensearch(value)
        bardata.append(list(priority_data.values()))

    plot_barplot(ax, bardata, colors, labels, priorities)
    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent Count')
    fig.legend()
    plt.show()


def plot_pct_duration_bins():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle(f'{search_suffix} Percentage Duration of Requests Scheduled', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()

    bardata = []
    control_prio_data = control['_source']['percent_duration_by_priority'][0]
    priorities = list(control_prio_data.keys())
    percentages = list(control_prio_data.values())
    bardata.append(percentages)

    for value in AIRMASS_TEST_VALUES:
        _, priority_data, _ = get_priority_data_from_opensearch(value)
        bardata.append(list(priority_data.values()))

    plot_barplot(ax, bardata, colors, labels, priorities)
    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent Duration')
    fig.legend()
    plt.show()


if __name__ == '__main__':
    plot_midpoint_airmass_histogram()
    plot_normed_airmass_histogram()
    plot_pct_scheduled_bins()
    plot_pct_duration_bins()
