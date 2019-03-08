from adaptive_scheduler.pond import ObservationScheduleInterface, build_block, PondBlock, Block
from adaptive_scheduler.configdb_connections import ConfigDBInterface
from adaptive_scheduler.model2 import (RequestGroup, Request, Configuration, Window, Windows, Proposal, Target,
                                       SiderealTarget, Constraints, ConfigurationFactory)
from adaptive_scheduler.kernel.reservation_v3 import Reservation_v3
from adaptive_scheduler.utils import datetime_to_normalised_epoch

from datetime import datetime

# Script for submitting block directly into the pond. Modify the resource and reservation duration and start
# to change when and for how long a block is. If you use tracking/request numbers that are not in valhalla,
# valhalla will crash when it tries to do its is_dirty check.

resource = '1m0a.doma.cpt'
semester_start = datetime(2017, 12, 1)
reservation = Reservation_v3(30, 24*60*60, {})
reservation.scheduled_start = datetime_to_normalised_epoch(datetime(2018,2,28,19,30), semester_start)
reservation.scheduled_resource = resource

constraints = Constraints(max_airmass=3.0)
window = Window({'start': datetime(2018,2,28,15),
                 'end': datetime(2018,3,5)},
                resource=resource)
windows = Windows()
windows.append(window)

target = SiderealTarget(name='sirius', ra=101.2871542, dec=-16.7161167, proper_motion_ra=-546.01, proper_motion_dec=-1223.07)

molecules = [ConfigurationFactory().build({
    'type': 'EXPOSE',
    'exposure_count': 60,
    'bin_x': 1,
    'bin_y': 1,
    'instrument_name': '1M0-SCICAM-SINISTRO',
    'filter': 'w',
    'exposure_time': 60,
    'priority':1,
    'ag_mode': 'OFF',
}),]

request = Request(target, molecules, windows, constraints, 9000000012, state='PENDING', duration=5*3600)

proposal = Proposal({'id': 'LCOSchedulerTest',
                     'tag': 'LCOScheduler Test',
                     'tac_priority': 20,
                     'pi': 'me'})

user_request = RequestGroup('single', [request, ], proposal, 9000000012, 'NORMAL', 1.0, 'test normal',
                            datetime(2049,1, 1), 'me')

configdb_interface = ConfigDBInterface(configdb_url='http://configdbdev.lco.gtn')
block = build_block(reservation, request, user_request, semester_start, configdb_interface)

PondBlock.save_blocks(blocks=[block.create_pond_block(),], port=12345, host='ponddev.lco.gtn')
