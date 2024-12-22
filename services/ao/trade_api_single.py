import logging

logging.basicConfig(
    level=logging.INFO
)
logger = logging.getLogger(__name__)


import sys
if '/home/brumen/work/' not in sys.path:
    sys.path.append('/home/brumen/work/')

from rm.services.ao.trade_api import main

# start the server
main()
