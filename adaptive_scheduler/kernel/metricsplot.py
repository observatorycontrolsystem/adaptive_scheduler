#!/usr/bin/env python

'''
MetricsPlot

Author: Sotiria Lampoudi (slampoud@gmail.com)
May 2012
'''

import matplotlib
matplotlib.use('Agg')
from pylab import *

class MetricsPlot(object):
    
    def plot_vector(self, v, description, filename):
        x = []
        y = []
        ymax = 0
        for s, f, val in v:
            x.append(s)
            y.append(val)
            x.append(f)
            y.append(val)
            if val > ymax:
                ymax = val
        axis([x[0]-1, x[-1]+1, 0, ymax+1])
        xlabel('time')
        ylabel('whatever')
        title(description)
        plot(x,y)
        savefig(filename)
