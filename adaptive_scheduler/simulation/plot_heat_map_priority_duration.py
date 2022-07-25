from xml.dom.pulldom import default_bufsize
import numpy as np
import matplotlib.pyplot as plt
from opensearchpy import OpenSearch
from plotutils import get_data_from_opensearch
from adaptive_scheduler.simulation.metrics import bin_data
VARIABLE = ['no-duration-v2','no-duration-scaled-100-v2']
            # 'with-duration-scaled-100','no-duration','no-duration-scaled-100',]

markers = ["o" , "," ,"v" , "^" , "<", ">"]
colors = ['r','b','c','m', 'y', 'k']
def rand_jitter(arr):
    stdev = .01 * (max(arr) - min(arr))
    return arr + np.random.randn(len(arr)) * stdev
    
def plot_sched_priority_duration_dotplot():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(28, 12))
    fig.suptitle(f'1m0 Distribution of Priority and Duration With Airmass Optimization', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    for i, id in enumerate(VARIABLE):
        data = get_data_from_opensearch(f'1m0-optimize-airmass-{id}')
        if id in ['no-duration-scaled-100-v2', 'with-duration-scaled-100-v2']:
            data['raw_scheduled_priorities'] = [(p+35)/4.5 for p in data['raw_scheduled_priorities']]
        # print(id, len(data['raw_scheduled_priorities']), len(data['raw_unscheduled_priorities']))
        # ax1.scatter(rand_jitter(data['raw_scheduled_priorities']), rand_jitter(data['raw_scheduled_durations']), 
        #            marker = markers[i],c = colors[i], s = 10, label = f'scheduled requests {id}',alpha = 0.3)

    
    ax1.set_ylim(top=11000)
    ax1.set_xlabel('Priority')
    ax1.set_ylabel('Request Duration')
    ax1.legend()
    for i, id in enumerate(VARIABLE):
        data = get_data_from_opensearch(f'1m0-optimize-airmass-{id}')
        if id in ['no-duration-scaled-100-v2', 'with-duration-scaled-100-v2']:
            data['raw_unscheduled_priorities'] = [(p+35)/4.5 for p in data['raw_unscheduled_priorities']]
        # ax2.scatter(rand_jitter(data['raw_unscheduled_priorities']), rand_jitter(data['raw_unscheduled_durations']),
        #            c =colors[i], marker=markers[i],s=10, label = f'unscheduled requests {id}', alpha = 0.3)
    ax2.set_ylim(top=11000)
    ax2.set_xlabel('Priority')
    ax2.set_ylabel('Request Duration')
    ax2.legend()
    plt.show(block = False)
    plt.show()


def plot_heat_map_priority_duration():
    fig = plt.subplot()
    fig.suptitle()
    fig.subplots_adjust()
    for i, id in enumerate(VARIABLE):
        data = get_data_from_opensearch(f'1m0-optimize-airmass-{id}')
        if id in ['no-duration-scaled-100-v2', 'with-duration-scaled-100-v2']:
            data['raw_unscheduled_priorities'] = [(p+35)/4.5 for p in data['raw_unscheduled_priorities']]
            sched_priorities = data['raw_scheduled_priorities']
            sched_durations = data['raw_scheduled_durations']
            unsched_priorities = data['raw_unscheduled_priorities']
            unsched_durations = data['raw_unscheduled_durations']
            level_1_bins = bin_data(sched_priorities, sched_durations, bin_size=5, aggregator=None)
            print(level_1_bins)
        

if __name__ == '__main__':
   plot_heat_map_priority_duration()