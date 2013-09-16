# Logger configuration file
# Note that the recommendation at
# http://docs.python.org/howto/logging.html#library-config
# is that logging levels are the responsibility of the client.

import logging
from adaptive_scheduler.log import UserRequestHandler, UserRequestLogger

log = logging.getLogger('adaptive_scheduler')
log.setLevel(logging.INFO)
log.propagate=False

sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)

formatter = logging.Formatter(fmt='%(asctime)s.%(msecs).03d %(levelname)7s: %(module)15s: %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
sh.setFormatter(formatter)
log.addHandler(sh)


multi_ur_log = logging.getLogger('ur_logger')
multi_ur_log.setLevel(logging.DEBUG)
multi_ur_log.propagate=False

uh = UserRequestHandler(tracking_number='0000000001', logdir='logs')
uh.setLevel(logging.DEBUG)

uh.setFormatter(formatter)
multi_ur_log.addHandler(uh)

