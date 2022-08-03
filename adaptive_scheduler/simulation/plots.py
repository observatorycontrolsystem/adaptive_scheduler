"""
The interface for producing plots. To create plots, add plots to the list of plots,
modifying the parameters to Plot as necessary.
"""
import adaptive_scheduler.simulation.plotfuncs as plotfuncs
import adaptive_scheduler.simulation.plotutils as plotutils
from adaptive_scheduler.simulation.plotutils import Plot

airmass_experiment_ids = [
    'no-airmass-w-duration-no-scaling',
    'airmass-0.01-w-duration-no-scaling',
    'airmass-0.05-w-duration-no-scaling',
    'airmass-0.1-w-duration-no-scaling',
    'airmass-1.0-w-duration-no-scaling',
    'airmass-10-w-duration-no-scaling',
    'airmass-100-w-duration-no-scaling',
    'airmass-1000-w-duration-no-scaling',
    'airmass-1000000-w-duration-no-scaling',
]

plots = [
     Plot(plotfuncs.plot_airmass_difference_histogram,
          '1m Network Airmass Difference Distribution for Scheduled Requests',
          *airmass_experiment_ids),
     Plot(plotfuncs.plot_pct_scheduled_airmass_binned_priority,
          '1m Network Airmass Experiment Percent of Requests Scheduled per Priority Class',
          *airmass_experiment_ids),
     Plot(plotfuncs.plot_pct_scheduled_airmass_lineplot,
          '1m Network Airmass Experiment Percent of Requests Scheduled per Priority Class',
          *airmass_experiment_ids),
     Plot(plotfuncs.plot_pct_time_scheduled_airmass_binned_priority,
          '1m Network Airmass Experiment Percent of Requested Time Scheduled per Priority Class',
          *airmass_experiment_ids),
     Plot(plotfuncs.plot_pct_time_scheduled_airmass_lineplot,
          '1m Network Airmass Experiment Percent of Requested Time Scheduled per Priority Class',
          *airmass_experiment_ids),
     Plot(plotfuncs.plot_pct_time_scheduled_out_of_available,
          '1m Network Airmass Experiment Percent of Requested Time Scheduled out of Available Time',
          *airmass_experiment_ids),
     Plot(plotfuncs.plot_midpoint_airmass_histograms,
          '1m Network Airmass Experiment Midpoint Airmass Distributions',
          *airmass_experiment_ids),
     Plot(plotfuncs.plot_eff_priority_duration_scatter,
          '1m Network Scatterplot of Effective Priority and Duration',
          '1m0-optimize-airmass-with-duration-v2',
          '1m0-optimize-airmass-with-duration-scaled-100-v2'),
     Plot(plotfuncs.plot_duration_by_window_duration_scatter,
          '1m Network Scatterplot of Duration and Window Duration',
          'window-duration'),
     Plot(plotfuncs.plot_input_duration_binned_priority,
          '1m Network Histogram of Input Reservation duration binned by priority',
          '1m0-optimize-airmass-no-duration-v3'),
]

if __name__ == '__main__':
    plotutils.run_user_interface(plots)
