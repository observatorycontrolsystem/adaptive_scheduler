import sched
from xml.dom.pulldom import default_bufsize
import numpy as np
import matplotlib.pyplot as plt
from opensearchpy import OpenSearch
from adaptive_scheduler.simulation.plotutils import get_data_from_opensearch
from adaptive_scheduler.simulation.metrics import bin_data
import seaborn as sns
from colorspacious import cspace_converter
VARIABLE = [ 'with-duration-v3',
            'no-duration-v3',
            'with-duration-scaled-100-v3',
            'no-duration-scaled-100-v3',]

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
        if id in ['with-duration-scaled-100-v3', 'no-duration-scaled-100-v3']:
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
        if id in ['with-duration-scaled-100-v3', 'no-duration-scaled-100-v3']:
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
    fig, axs= plt.subplots(2, 2, figsize=(13, 12))
    fig.suptitle(f'1m0 Network Requests Heatmap With Airmass Optimization', fontsize=20)
    fig.subplots_adjust(wspace=0.01, hspace=0.01, top=0.9)
    ax_list = [axs[0,0],axs[0,1],axs[1,0], axs[1,1]]
    for i, id in enumerate(VARIABLE):
        data = get_data_from_opensearch(f'1m0-optimize-airmass-{id}')
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
        print(level_2_bins)
        level_1_bins_unsched = bin_data(unsched_priorities, unsched_durations, bin_size=4, bin_range=(10,30),aggregator=None)
        level_2_bins_unsched = {
            bin_key: bin_data(bin_values, bin_size=300, bin_range=(0, 1499)) | bin_data(bin_values, bin_size=3000, bin_range=(1500, 4000))
            for bin_key, bin_values in level_1_bins_unsched.items()
        }   
        heat_map_elements = []
        heat_map_elements_unsched = []
        for values in level_2_bins.values():
            # new_value= np.sum(list(values.values())[-5:])
            # temp_list = ['3000-3249', '3250-3499', '3500-3749', '3750-3999', '4000']
            # for key in temp_list:
            #     del values[key]
            # values['3000&above'] = new_value
            heat_map_elements.append(list(values.values()))
        for values in level_2_bins_unsched.values():
            # new_value= np.sum(list(values.values())[-5:])
            # temp_list = ['3000-3249', '3250-3499', '3500-3749', '3750-3999', '4000']
            # for key in temp_list:
            #     del values[key]
            # values['3000&above'] = new_value
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
        

if __name__ == '__main__':
   plot_heat_map_priority_duration()