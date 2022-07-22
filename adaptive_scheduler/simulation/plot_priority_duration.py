from xml.dom.pulldom import default_bufsize
import numpy as np
import matplotlib.pyplot as plt
from opensearchpy import OpenSearch
from plotutils import get_data_from_opensearch
VARIABLE = ['with-duration','no-duration',]
            # 'with-duration-scaled-100','no-duration','no-duration-scaled-100',]

markers = ["o" , "," ,"v" , "^" , "<", ">"]
colors = ['r','g','b','c','m', 'y', 'k']
def rand_jitter(arr):
    stdev = .01 * (max(arr) - min(arr))
    return arr + np.random.randn(len(arr)) * stdev
    
def plot_sched_priority_duration_dotplot():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle(f'1m0 Scheduled requests Distribution of Priority and Duration With Airmass Optimization', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()
    for i, id in enumerate(VARIABLE):
        data = get_data_from_opensearch(f'1m0-optimize-airmass-{id}')
        ax.scatter(rand_jitter(data['raw_scheduled_priorities']), rand_jitter(data['raw_scheduled_durations']), 
                   marker = markers[i],c = colors[i], s = 10, label = f'scheduled requests {id}',alpha = 0.5)
        # ax.scatter(rand_jitter(data['raw_unscheduled_priorities']), rand_jitter(data['raw_unscheduled_durations']), c = default_colors[5*i+2], marker='^',s=15, label = f'unscheduled requests {id}', alpha = 0.7)
    ax.set_xlabel('Priority')
    ax.set_ylabel('Request Duration')
    ax.legend()
    plt.show(block=False)
    plt.show()

def plot_unsched_priority_duration_dotplot():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle(f'1m0 Unscheduled requests Distribution of Priority and Duration With Airmass Optimization', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()
    for i, id in enumerate(VARIABLE):
        data = get_data_from_opensearch(f'1m0-optimize-airmass-{id}')
        # ax.scatter(rand_jitter(data['raw_scheduled_priorities']), rand_jitter(data['raw_scheduled_durations']), 
        #            marker = markers[i],c = colors[i], s = 10, label = f'scheduled requests {id}',alpha = 0.5)
        ax.scatter(rand_jitter(data['raw_unscheduled_priorities']), rand_jitter(data['raw_unscheduled_durations']),
                   c =colors[i], marker=markers[i],s=10, label = f'unscheduled requests {id}', alpha = 0.7)
    ax.set_xlabel('Priority')
    ax.set_ylabel('Request Duration')
    ax.legend()
    plt.show(block=False)
    plt.show()

if __name__ == '__main__':
    
    plot_sched_priority_duration_dotplot()
    plot_unsched_priority_duration_dotplot()