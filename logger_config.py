# Logger configuration file
# Note that the recommendation at
# http://docs.python.org/howto/logging.html#library-config
# is that logging levels are the responsibility of the client.

import logging
from adaptive_scheduler.log import RequestGroupHandler, RequestGroupLogger
from lcogt_logging import LCOGTFormatter

log = logging.getLogger('adaptive_scheduler')
log.setLevel(logging.INFO)
log.propagate = False

sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)

formatter = LCOGTFormatter()

sh.setFormatter(formatter)
log.addHandler(sh)

multi_rg_log = logging.getLogger('rg_logger')
multi_rg_log.setLevel(logging.DEBUG)
multi_rg_log.propagate = False

uh = RequestGroupHandler(request_group_id=1, logdir='logs')
uh.setLevel(logging.DEBUG)

uh.setFormatter(formatter)
multi_rg_log.addHandler(uh)
