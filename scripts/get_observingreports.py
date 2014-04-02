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


def increment_dict(dictionary, key):
    if key in dictionary:
        dictionary[key] += 1
    else:
        dictionary[key]  = 1


def count_histories():
    hist_dict = {}

    n_histories = 0
    n_sb        = 0
    n_obs       = 0
    for sb in scheduled_blocks:
        n_sb += 1
        for obs in sb.obs.all():
            n_obs += 1
            hist_set = obs.observationhistory_set
            n_histories += len(hist_set.all())

            for history in hist_set.all():
                if history.id in hist_dict:
                 #   print "Appending!"
                 #   print "Inside SB %d, obs_id %d, history_id %d" % (sb.id, obs.id,
                 #                                                    history.id)
                    hist_dict[history.id].append(history.obs_id)
                 #   return
                else:
                 #   if history.id == 1:
                 #       print "New!"
                 #       print "Inside SB %d, obs_id %d, history_id %d" % (sb.id, obs.id,
                 #                                                        history.id)
                    hist_dict[history.id] = [history.obs_id]

    print "Found %d scheduled blocks" % n_sb
    print "Found %d observations" % n_obs
    print "Found %d histories" % n_histories

    return hist_dict


def easier_do():
    hist_dict = {}

    n_histories = 0
    n_sb        = 0
    n_obs       = 0
    for sb in scheduled_blocks:
        n_sb += 1
        for obs in sb.obs.all():
            n_obs += 1
            hist_set = obs.observationhistory_set
            n_histories += len(hist_set.all())

            for history in hist_set.all():
                hist_dict[history.id] = {
                                          'sb_id'  : sb.id,
                                          'obs_id' : history.obs_id
                                        }

    print "Found %d scheduled blocks" % n_sb
    print "Found %d observations" % n_obs
    print "Found %d histories" % n_histories

    out_fh = open('observing_histories.dat', 'w')
    for hist_id in sorted(hist_dict):
        info_dict = hist_dict[hist_id]

        sb = ScheduledBlock.objects.get(id=info_dict['sb_id'])
        instrument = None
        if len(sb.obs.all()) > 0:
            instrument = sb.obs.all()[0].inst_name.lower()

        # Track unique location-instrument combinations for sanity checking
        loc_instr = "%s.%s.%s.%s" % (
                                      sb.telescope,
                                      sb.observatory,
                                      sb.site,
                                      str(instrument)
                                    )

        history = ObservationHistory.objects.get(id=hist_id)
        obs_report = history.event.replace('\n','')
        formatted_data_line = "%-6s %-6s %-15s %s\n" % (
                                                     hist_id,
                                                     sb.id,
                                                     loc_instr,
                                                     obs_report
                                                   )
        out_fh.write(formatted_data_line)
    out_fh.close()

    return



def write_collated_histories_to_file_serial2(extracted, histories, out_fh):

    if not extracted['instrument']:
        print "Didn't find an instrument - skipping ID %s" % extracted['sb_id']
        return

    if len(histories) == 0:
        print "Didn't find any histories - skipping obs_id %s (sb_id %s)" % (
                                                            obs_id_dict['obs_id'],
                                                            extracted['sb_id']
                                                                      )
        return

    for history in histories:
        obs_report = history.event.replace('\n','')
        formatted_data_line = "%-6s %-6s %-5s %-12s %s\n" % (
                                                     extracted['sb_id'],
                                                     obs_id_dict['obs_id'],
                                                     extracted['location'],
                                                     extracted['instrument'],
                                                     obs_report
                                                   )
        out_fh.write(formatted_data_line)

    return




def collate_origin_info_from_scheduled_blocks():
    print "Extracting location and instrument information..."
    unique_loc_instr = {}
    collated = {}
    for sb in scheduled_blocks:
        extracted = {}

        extracted['sb_id']       = sb.id
        extracted['telescope']   = sb.telescope
        extracted['observatory'] = sb.observatory
        extracted['site']        = sb.site

        extracted['instrument']  = None
        if len(sb.obs.all()) > 0:
            extracted['instrument']  = sb.obs.all()[0].inst_name

        # Track unique location-instrument combinations for sanity checking
        loc_instr = "%s.%s.%s.%s" % (
                                      extracted['telescope'],
                                      extracted['observatory'],
                                      extracted['site'],
                                      str(extracted['instrument'])
                                    )
        increment_dict(unique_loc_instr, loc_instr.lower())


        extracted['obs_ids'] = []

        for sbo in sb.scheduledblock_observation_set.all():
            extracted['obs_ids'].append(
                                         {
                                            'obs_id'  : sbo.obs_id,
                                         }
                                       )

        location = "%s.%s.%s" % (sb.telescope, sb.observatory, sb.site)
        extracted['location'] = location

        collated[sb.id] = extracted


    # Print the set of location-instruments we found
    print "location.instrument  n_blocks"
    for name, count in sorted(unique_loc_instr.iteritems()):
        print "%-20s %d" % (name, count)

    return collated


def associate_locations_with_histories(collated, file_to_write):
    print "Associating histories..."
    out_fh = open(file_to_write, 'w')
    out_fh.write("#sb_id obs_id Location      Instrument   ObservingReport\n")

    n_histories_matched   = 0
    n_histories_not_found = 0
    for sb_id in sorted(collated):
        sb_info = collated[sb_id]
        for obs_id_dict in sb_info['obs_ids']:
            try:
                matching_histories = histories.filter(obs__id=obs_id_dict['obs_id'])
                n_histories_matched += len(matching_histories)
            except ObservationHistory.DoesNotExist as e:
                #print "No history found for obs_id %d." % obs_id_dict['obs_id']
                n_histories_not_found += 1

            write_collated_histories_to_file_serial(sb_info, obs_id_dict,
                                                    matching_histories,
                                                    out_fh)

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

    out_fh.close()

    return collated


def write_collated_histories_to_file_serial(extracted, obs_id_dict, histories, out_fh):

    if not extracted['instrument']:
        print "Didn't find an instrument - skipping ID %s" % extracted['sb_id']
        return

    if len(histories) == 0:
        print "Didn't find any histories - skipping obs_id %s (sb_id %s)" % (
                                                            obs_id_dict['obs_id'],
                                                            extracted['sb_id']
                                                                      )
        return

    for history in histories:
        obs_report = history.event.replace('\n','')
        formatted_data_line = "%-6s %-6s %-5s %-12s %s\n" % (
                                                     extracted['sb_id'],
                                                     obs_id_dict['obs_id'],
                                                     extracted['location'],
                                                     extracted['instrument'],
                                                     obs_report
                                                   )
        out_fh.write(formatted_data_line)

    return



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
            if len(obs_id_dict['histories']) == 0:
                print "Didn't find any histories - skipping obs_id %s (sb_id %s)" % (
                                                                               obs_id,
                                                                                sb_id
                                                                              )
                continue

            for history in obs_id_dict['histories']:
                obs_report = history.event.replace('\n','')
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
    associate_locations_with_histories(collated, file_to_write)
    #write_collated_histories_to_file(collated, file_to_write)
