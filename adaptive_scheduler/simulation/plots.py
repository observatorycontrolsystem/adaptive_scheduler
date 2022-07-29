"""
The interface for producing plots. To create plots, add plots to the list of plots,
modifying the parameters to Plot as necessary.
"""
import matplotlib.pyplot as plt

import adaptive_scheduler.simulation.plotfuncs as plotfuncs
import adaptive_scheduler.simulation.plotutils as plotutils
from adaptive_scheduler.simulation.plotutils import Plot

airmass_experiment_ids = [
    '1m0-simulation-real-airmass-control-1_2022-07-18T23:59:44.770684',
    '1m0-simulation-real-airmass-coeff-0-1',
    '1m0-simulation-real-airmass-coeff-0.01-1',
    '1m0-simulation-real-airmass-coeff-0.05-1',
    '1m0-simulation-real-airmass-coeff-0.1-1',
    '1m0-simulation-real-airmass-coeff-1.0-1',
    '1m0-simulation-real-airmass-coeff-10-1',
    '1m0-simulation-real-airmass-coeff-100-1',
    '1m0-simulation-real-airmass-coeff-1000-1',
    '1m0-simulation-real-airmass-coeff-1000000-1',
]

plots = [
    Plot(plotfuncs.plot_normed_airmass_histogram, *airmass_experiment_ids),
]

if __name__ == '__main__':
    spacing = max([len(plot.name) for plot in plots]) + 10
    print('Available plots:')
    print(f'{"Name":{spacing}}Description')
    print(f'{"====":{spacing}}===========')

    for plot in plots:
        print(f'{plot.name:{spacing}}{plot.description}')
    showplot = input('Show plot (default all): ')
    if showplot == '':
        for plot in plots:
            plt.show()
    else:
        plt.close('all')
        plot.fig.show()
        plt.show()
