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

effective_priority_experiment_ids = [
    'airmass-0.1-w-duration-no-scaling',
    'airmass-0.1-no-duration-no-scaling',
    'airmass-0.1-w-duration-w-scaling',
    'airmass-0.1-no-duration-w-scaling',
]


plots = [
     Plot(plotfuncs.plot_airmass_difference_histogram,
          '1m Network Airmass Difference Distribution for Scheduled Requests',
          airmass_experiment_ids),
     Plot(plotfuncs.plot_pct_scheduled_airmass_binned_priority,
          '1m Network Airmass Experiment Percent of Requests Scheduled per Priority Class',
          airmass_experiment_ids),
     Plot(plotfuncs.plot_pct_scheduled_airmass_lineplot,
          '1m Network Airmass Experiment Percent of Requests Scheduled per Priority Class',
          airmass_experiment_ids),
     Plot(plotfuncs.plot_pct_time_scheduled_airmass_binned_priority,
          '1m Network Airmass Experiment Percent of Requested Time Scheduled per Priority Class',
          airmass_experiment_ids),
     Plot(plotfuncs.plot_pct_time_scheduled_airmass_lineplot,
          '1m Network Airmass Experiment Percent of Requested Time Scheduled per Priority Class',
          airmass_experiment_ids),
     Plot(plotfuncs.plot_pct_time_scheduled_out_of_available,
          '1m Network Airmass Experiment Percent of Requested Time Scheduled out of Available Time',
          airmass_experiment_ids),
     Plot(plotfuncs.plot_midpoint_airmass_histograms,
          '1m Network Airmass Experiment Midpoint Airmass Distributions',
          airmass_experiment_ids),
     Plot(plotfuncs.plot_eff_priority_duration_scatter,
          '1m Network Scatterplot of Effective Priority and Duration',
          ['airmass-0.1-w-duration-no-scaling', 'airmass-0.1-w-duration-w-scaling']),
     Plot(plotfuncs.plot_duration_by_window_duration_scatter,
          '1m Network Scatterplot of Duration and Window Duration',
          'window-duration'),
     Plot(plotfuncs.plot_subplots_input_duration,
          '1m Network Scheduled/Unscheduled Requests Length Distribution',
          'no-airmass-w-duration-no-scaling'),
     Plot(plotfuncs.plot_percent_sched_requests_bin_by_priority,
          '1m Network Scheduler Metrics Binned by Priority',
          effective_priority_experiment_ids),
     Plot(plotfuncs.plot_sched_priority_duration_dotplot,
          '1m Distribution of Priority and Duration With Airmass Optimization',
          effective_priority_experiment_ids),
     Plot(plotfuncs.plot_heat_map_priority_duration,
          '1m Network Requests Heatmap With Airmass Optimization (sched|unsched)',
          effective_priority_experiment_ids),
]


if __name__ == '__main__':
    plotutils.run_user_interface(plots)
