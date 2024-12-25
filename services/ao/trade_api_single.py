import logging
from rm.services.ao.trade_api import main

logging.basicConfig(
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# start the server
main()
