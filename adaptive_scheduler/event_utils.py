#!/usr/bin/env python

'''
event_utils.py - Useful event emitters for common tests

description

Author: Eric Saunders
December 2013
'''

def report_scheduling_outcome(to_schedule, scheduled_reservations):
    # Collate the scheduled and unscheduled reservations
    to_schedule_res = []
    for comp_res in to_schedule:
        to_schedule_res.extend(comp_res.reservation_list)


    not_scheduled_res = set(to_schedule_res) - set(scheduled_reservations)

    # Emit the messages
    for res in scheduled_reservations:
        tag = 'WasScheduled'
        msg = 'This Request (request number=%s) was scheduled' % res.request.request_number
        res.user_request.emit_user_feedback(msg, tag)

    for res in not_scheduled_res:
        tag = 'WasNotScheduled'
        msg = 'This Request (request number=%s) was not scheduled (it clashed)' % res.request.request_number
        res.user_request.emit_user_feedback(msg, tag)

    return


def split_urs_by_rise_set_filtering(all_urs, visible_urs):
    non_visible_urs = set(all_urs) - set(visible_urs)

    return visible_urs, non_visible_urs


def report_visibility_outcome(all_urs, visible_urs):
    visible_urs, non_visible_urs = split_urs_by_rise_set_filtering(all_urs, visible_urs)

    for ur in visible_urs:
        tag = 'IsVisible'
        msg = 'UR %s is visible' % ur.tracking_number
        ur.emit_user_feedback(msg, tag)

    for ur in non_visible_urs:
        tag = 'IsNotVisible'
        msg = 'UR %s is not visible - dropping from consideration' % ur.tracking_number
        ur.emit_user_feedback(msg, tag)

    return
