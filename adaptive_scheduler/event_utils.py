#!/usr/bin/env python
'''
event_utils.py - Useful event emitters for common tests

description

Author: Eric Saunders
December 2013
'''

from adaptive_scheduler.models import RequestGroup


def report_scheduling_outcome(to_schedule, scheduled_reservations):
    # Collate the scheduled and unscheduled reservations
    to_schedule_res = []
    for comp_res in to_schedule:
        to_schedule_res.extend(comp_res.reservation_list)

    not_scheduled_res = set(to_schedule_res) - set(scheduled_reservations)

    # Emit the messages
    for res in scheduled_reservations:
        tag = 'WasScheduled'
        msg = 'This Request (request id=%d) was scheduled' % res.request.id
        RequestGroup.emit_request_group_feedback(res.request_group_id, msg, tag)

    for res in not_scheduled_res:
        tag = 'WasNotScheduled'
        msg = 'This Request (request id=%d) was not scheduled (it clashed)' % res.request.id
        RequestGroup.emit_request_group_feedback(res.request_group_id, msg, tag)

    return


def split_rgs_by_rise_set_filtering(all_rgs, visible_rgs):
    non_visible_rgs = set(all_rgs) - set(visible_rgs)

    return visible_rgs, non_visible_rgs


def report_visibility_outcome(all_rgs, visible_rgs):
    visible_rgs, non_visible_rgs = split_rgs_by_rise_set_filtering(all_rgs, visible_rgs)

    for rg in visible_rgs:
        tag = 'IsVisible'
        msg = 'RG %d is visible' % rg.id
        rg.emit_rg_feedback(msg, tag)

    for rg in non_visible_rgs:
        tag = 'IsNotVisible'
        msg = 'RG %d is not visible - dropping from consideration' % rg.id
        rg.emit_rg_feedback(msg, tag)

    return
