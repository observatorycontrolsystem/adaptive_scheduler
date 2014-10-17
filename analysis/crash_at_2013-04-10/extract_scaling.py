#!/usr/bin/env python

import sys

in_fh = open(sys.argv[1], 'r')

n_urs = run_time = 0
values = []
for i, line in enumerate(in_fh):
    words = line.split()

    # Total run time of main()
    if i % 2:
        date, time, run_time = words[0], words[1], words[-2]
        print n_urs, run_time
        values.append((int(n_urs), float(run_time), date, time))
        #values.append((int(n_urs), float(run_time)))
    # n User Requests
    else:
        n_urs = words[5]

values.sort()

for x, y, date, time in values:
    print x, y, date, time
