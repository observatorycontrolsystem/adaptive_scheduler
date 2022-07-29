import numpy as np
import matplotlib.pyplot as plt
from opensearchpy import OpenSearch
from plotutils import get_data_from_opensearch, plot_barplot

EFF_PRI_CALC= ['airmass','airmass-with-duration-scaled-100','airmass-no-duration','airmass-no-duration-scaled-100',]
  
    
def plot_percent_sched_duration_bin_by_priority():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle(f'1m0 Network Percent Request Time Scheduled binned by Priority', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()
    bardata = []
    for id in EFF_PRI_CALC:
        priority_data = get_data_from_opensearch(f'1m0-optimize-{id}')['percent_duration_by_priority']
        
        bardata.append(list(priority_data[0].values()))

    priorities = ['low priority', 'mid priority', 'high priority']
    plot_barplot(ax, bardata, EFF_PRI_CALC, priorities)
    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent Scheduled Time')
    fig.legend()
    plt.show()
     
     
def plot_percent_sched_numbers_bin_by_priority():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle(f'1m0 Network Percent Request Number Scheduled binned by Priority', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()
    bardata = []
    for id in EFF_PRI_CALC:
        priority_data = get_data_from_opensearch(f'1m0-optimize-{id}')['percent_sched_by_priority']
        
        bardata.append(list(priority_data[0].values()))

    priorities = ['low priority', 'mid priority', 'high priority']
    plot_barplot(ax, bardata, EFF_PRI_CALC, priorities)
    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent Scheduled Count')
    fig.legend()
    plt.show()
     
    
if __name__ == '__main__':
    
    plot_percent_sched_duration_bin_by_priority()
    plot_percent_sched_numbers_bin_by_priority()