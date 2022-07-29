"""
The interface for producing plots. To create plots, add plots to the list of plots,
modifying the parameters to Plot as necessary.
"""
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
    Plot(plotfuncs.plot_airmass_difference_histogram,
         '1m Network Airmass Score Distribution for Scheduled Requests',
         *airmass_experiment_ids),
    Plot(plotfuncs.plot_pct_scheduled_airmass_binned_priority,
         '1m Network Airmass Experiment Percent of Requests Scheduled',
         *airmass_experiment_ids),
]

if __name__ == '__main__':
    plotutils.run_user_interface(plots)
