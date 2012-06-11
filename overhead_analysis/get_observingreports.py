#!/usr/bin/env python

'''
get_observingreports.py - Extract observing reports, along with metadata about
where the observation took place.

XML observing reports are created whenever an observation takes place on a telescope.
However, historical reports don't include information about site/obs/telescope, or
instrument, which makes analysis by resource difficult.

This code traverses the Django model to retroactively connect ObservationHistorys
(which contain the observingreport) to ScheduledBlocks (which contain the telescope
and, via the Observation, the instrument information).

A single Observation may be scheduled in multiple places. We assume that when a
block is cloned and resubmitted for observing, it is submitted to the same resource
(telescope-instrument pair).

Author: Eric Saunders
June 2012
'''

from pond.models import ScheduledBlock, ObservationHistory

scheduled_blocks = ScheduledBlock.objects.all()
histories        = ObservationHistory.objects.all()


def collate_origin_info_from_scheduled_blocks():
    print "Extracting location and instrument information..."
    collated = {}
    for sb in scheduled_blocks:
        extracted = {}

        extracted['telescope']   = sb.telescope
        extracted['observatory'] = sb.observatory
        extracted['site']        = sb.site

        extracted['instrument']  = None
        if len(sb.obs.all()) > 0:
            extracted['instrument']  = sb.obs.all()[0].inst_name

        extracted['obs_ids']     = []

        for sbo in sb.scheduledblock_observation_set.all():
            extracted['obs_ids'].append(
                                         {
                                            'obs_id'  : sbo.obs_id,
                                            'history' : None,
                                         }
                                       )

        location = "%s.%s.%s" % (sb.telescope, sb.observatory, sb.site)
        extracted['location'] = location

        collated[sb.id] = extracted

    return collated


def associate_locations_with_histories(collated):
    print "Associating histories..."
    n_histories_matched   = 0
    n_histories_not_found = 0
    for sb_info in collated.values():
        for obs_id_dict in sb_info['obs_ids']:
            try:
                history = histories.get(id=obs_id_dict['obs_id'])
                obs_id_dict['history'] = history
                n_histories_matched += 1
            except ObservationHistory.DoesNotExist as e:
                #print "No history found for obs_id %d." % obs_id_dict['obs_id']
                n_histories_not_found += 1


    # Calculate some summary information
    total_obs = 0
    for sb in scheduled_blocks:
        total_obs += len(sb.obs.all())

    n_orphan_histories = len(histories) - n_histories_matched

    print "Found %d scheduled blocks." % len(scheduled_blocks)
    print "Found %d histories in the DB." % len(histories)
    print "Found %d total observations." % total_obs
    print "Successfully matched %d histories." % n_histories_matched
    print "%d observations have no corresponding history." % n_histories_not_found
    print "There are %d histories which don't appear to have blocks." % n_orphan_histories

    return collated


def write_collated_histories_to_file(collated, file_to_write):
    out_fh = open(file_to_write, 'w')
    out_fh.write("#sb_id obs_id Location      Instrument   ObservingReport\n")
    for sb_id in sorted(collated):
        extracted = collated[sb_id]

        if not collated[sb_id]['instrument']:
            print "Didn't find an instrument - skipping ID %s" % sb_id
            continue

        for obs_id_dict in collated[sb_id]['obs_ids']:
            obs_id = obs_id_dict['obs_id']
            if not obs_id_dict['history']:
                print "Didn't find a history - skipping obs_id %s (sb_id %s)" % (obs_id,
                                                                                 sb_id)
                continue

            obs_report = obs_id_dict['history'].event.replace('\n','')
            formatted_data_line = "%-6s %-6s %-5s %-12s %s\n" % (
                                                             sb_id,
                                                             obs_id,
                                                             extracted['location'],
                                                             extracted['instrument'],
                                                             obs_report
                                                           )
            out_fh.write(formatted_data_line)

    out_fh.close()

    return


if __name__ == '__main__':
    file_to_write = 'observing_histories.dat'

    collated = collate_origin_info_from_scheduled_blocks()
    associate_locations_with_histories(collated)
    write_collated_histories_to_file(collated, file_to_write)
