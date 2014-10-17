# slampoud@cs.ucsb.edu 3/1/2011

from operator import attrgetter, methodcaller

class ObservationEndpoint:
  def __init__(self, time, is_start, seqid):
        self.time     = time
        self.is_start = is_start
        self.seqid    = seqid

  def __repr__(self):
      return repr( (self.time, self.is_start, self.seqid) )


# populate two lists of ObservationEndpoint objects with starts and ends

seq1 = []
seq2 = []
seq1.append(ObservationEndpoint(1, True, 0)) # start
seq1.append(ObservationEndpoint(2, False, 0)) # end

seq2.append(ObservationEndpoint(1, True, 1)) # start
seq2.append(ObservationEndpoint(1.5, False, 1)) # end
seq2.append(ObservationEndpoint(1.6, True, 1)) # start
seq2.append(ObservationEndpoint(3, False, 1)) # end

# concatenate sequences and sort by time
# tie breaking: end and start w/ same time => end before start
#               2 ends or 2 starts w/ same time => order doesn't matter
# note: I don't know how to do double criterion sorting with methodcaller, which 
# would be the preferable way, so I'm using attrgetter


def find_overlaps(seq1, seq2):
    bothseq = sorted(seq1+seq2, key=attrgetter('time', 'is_start'))

    overlaps = []
    flags    = [0,0]
    for oe in bothseq:
       if oe.is_start:
          flags[oe.seqid] += 1 # raise the flag
          if flags[(oe.seqid+1)%2]: # detect start of an overlap
             overlaps.append(ObservationEndpoint(oe.time, True, 0)) # add start
       else:
          flags[oe.seqid]-=1 # lower the flag
          if flags[(oe.seqid+1)%2]: # detect end of an overlap
             overlaps.append(ObservationEndpoint(oe.time, False, 0)) # add end

    return overlaps


print "Method 1"
overlaps = find_overlaps(seq1, seq2)
print overlaps


def windows_intersect(start1, end1, start2, end2):
    if end2 <= start1 or start2 >= end1:
        return False

    return  start1 <= end2 and end1 >= start2



def find_overlaps2(seq1, seq2):
    overlaps = ()

    return overlaps


seq3 = (
         (1, 2),
       )

seq4 = (
         (1, 1.5),
         (1.6, 3),
       )


print "\nAlternative method"
overlaps = find_overlaps2(seq3, seq4)
print overlaps


# Eric's code for overlapping dark and up intervals
def intervals_to_obs_endpoints(intervals):
    endpoints = []
    for n, i in enumerate(intervals):
        start, end = i[0], i[1]
        start_ep = ObservationEndpoint(time=start, is_start=True,  seqid=n)
        end_ep   = ObservationEndpoint(time=end,   is_start=False, seqid=n)

        endpoints.append(start_ep)
        endpoints.append(end_ep)

    return endpoints


def chunker(seq, chunksize):
    return (seq[pos:pos + chunksize] for pos in xrange(0, len(seq), chunksize))


def obs_endpoints_to_intervals(eps):
    intervals = []
    for e_start, e_end in chunker(eps, 2):
        interval = (e_start.time, e_end.time)
        intervals.append(interval)

    return intervals


def is_dark_and_up(dark_ints, up_ints):
    dark_eps = intervals_to_obs_endpoints(dark_ints)
    up_eps   = intervals_to_obs_endpoints(up_ints)

    overlap_eps = find_overlaps(dark_eps, up_eps)

    overlap_ints = obs_endpoints_to_intervals(overlap_eps)

    return overlap_ints

# Example use:
import datetime

up_ints = [(datetime.datetime(2010, 10, 1, 1, 56, 49, 16863),
            datetime.datetime(2010, 10, 1, 10, 57, 27, 464638)),
           (datetime.datetime(2010, 10, 2, 1, 52, 53, 186013),
            datetime.datetime(2010, 10, 2, 10, 53, 31, 627179))]

dark_ints = [(datetime.datetime(2010, 10, 1, 4, 13, 38, 879895),
              datetime.datetime(2010, 10, 1, 16, 16, 7, 100207)),
             (datetime.datetime(2010, 10, 2, 4, 12, 44, 417371),
              datetime.datetime(2010, 10, 2, 16, 16, 23, 159988))]

dark_and_up_ints = is_dark_and_up(dark_ints, up_ints)
print "Dark and up:"
print dark_and_up_ints
