#!/usr/bin/env python
'''
network_state_json.py - Prototype json format for network resource state

This shows a rough data format for capturing network resource state (what can be
scheduled) for the scheduler. Obviously, there are many more fields which should be
present, but this provides the skeleton.

 Hierarchy:
 Network
     Site
         Weather
         Observatory
             Telescope
                 Instrument
                     Filter
                     Molecule

Author: Eric Saunders
April 2012
'''


import json

# 1) Equipment objects
status_1 = {
            'state'  : 'down',
            'reason' : 'weather',
           }

molecule_1 = {
               'name' : 'expose_n',
             }

filter_1   = {
               'name'   : 'BSSL-UX-020',
               'status' : status_1,
             }

instrument_1 = {
                 'name'      : 'KB12',
                 'filters'   : [filter_1],
                 'molecules' : [molecule_1],
                 'status'    : status_1,
               }

telescope_1 = {
                'name'        : '1m0a.doma.bpl',
                'instruments' : [instrument_1],
                'status'      : status_1,
              }

observatory_1 = {
                  'name'       : 'doma.bpl',
                  'telescopes' : [telescope_1],
                  'status'     : status_1,
                }


# 2) Weather objects
datum_1 = {
            'rh_avg' : 28.6,
          }

weather = {
            'datums' : [datum_1],
            'status' : status_1,
          }


# Aggregate states into site object, then sites into network
site_1 = {
           'name'          : 'bpl',
           'observatories' : [observatory_1],
           'weather'       : weather,
         }

network = {
            'timestamp' :  '2011-04-06 00:00:00',
            'sites'     : [site_1],
          }

serialised = json.dumps(network)
print serialised

