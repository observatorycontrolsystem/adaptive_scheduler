from matplotlib.ticker import MultipleLocator
from matplotlib import pyplot as plt
from matplotlib.ticker import MultipleLocator, FormatStrFormatter

import sys

in_fh = open(sys.argv[1], 'r')

n_reqs = []
run_time = []
for line in in_fh:
    words = line.split()
    n_reqs.append(words[0])
    run_time.append(words[1])

#plt.yticks(np.arange(min(x), max(x)+1, 1.0))

plt.plot(n_reqs, run_time, marker='+', linestyle='None', color='r')
ml_minor = MultipleLocator(5)
ml_major = MultipleLocator(20)
plt.axes().xaxis.set_minor_locator(ml_minor)
plt.axes().xaxis.set_major_locator(ml_major)
plt.axes().xaxis.set_major_formatter(FormatStrFormatter('%d'))

plt.axes().yaxis.set_minor_locator(ml_minor)
plt.axes().yaxis.set_major_locator(ml_major)
plt.axes().yaxis.set_major_formatter(FormatStrFormatter('%d'))




plt.xlabel('N requests')
plt.ylabel('Run time (s)')
plt.title('Scheduler run time scaling')
plt.grid(True)
plt.savefig("scheduler_run_time.png")
plt.show()
