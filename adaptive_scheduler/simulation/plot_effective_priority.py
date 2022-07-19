import numpy as np
import matplotlib.pyplot as plt
from opensearchpy import OpenSearch
from plotutils import get_data_from_opensearch, plot_barplot, default_colors

EFF_PRI_CALC= ['base-only', 'base-scaled-60', 'base-scaled-3600', 'base-duration']
  
    
def plot_percent_duration_bin():
    fig = plt.figure(figsize=(20, 10))
    fig.suptitle(f'Scheduled requests seconds over total request seconds for different Effective Priority algorithms', fontsize=20)
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.9)
    ax = fig.add_subplot()
    bardata = []
    for id in EFF_PRI_CALC:
        priority_data = get_data_from_opensearch(f'*-effective-priority-{id}-1m0')['percent_duration_by_priority']
        priorities = list(priority_data[0].keys())
        bardata.append(list(priority_data[0].values()))

    plot_barplot(ax, bardata, default_colors, EFF_PRI_CALC, priorities)
    ax.set_xlabel('Priority')
    ax.set_ylabel('Percent Duration')
    fig.legend()
    plt.show()
     
    
if __name__ == '__main__':
    plot_percent_duration_bin()