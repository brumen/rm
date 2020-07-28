# portfolio worker relating to Air options.

import datetime
import logging

from typing import List

from rm.delta_dict       import DeltaDict
from ao.air_option       import AirOptionMock
from rm.portfolio_worker import PortfolioWorker
from rm.socket_msg       import NanoSocketMixin

logger = logging.getLogger(__name__)
logger.setLevel('INFO')  # log at info level


def revalue_ao_portfolio(portfolio, mkt_date : datetime.date) -> DeltaDict:
    """ Revalues the AirOptions portfolio given.

    :param portfolio: trade portfolio to use.
    :param mkt_date: market date
    :returns:
    """

    logger.debug('Computing portfolio {0}'.format(str(portfolio)))
    portfolio_delta = DeltaDict({})

    for _, orig\
         , dest\
         , option_start_date\
         , option_end_date\
         , option_ret_start_date\
         , option_ret_end_date\
         , outbound_date_start\
         , outbound_date_end\
         , inbound_date_start\
         , inbound_date_end\
         , K\
         , carrier\
         , adults\
         , cabinclass in portfolio:

        # computing pv01 - delta
        portfolio_delta += AirOptionMock( mkt_date
                                , orig
                                , dest
                                , outbound_date_start   = outbound_date_start
                                , outbound_date_end     = outbound_date_end
                                , inbound_date_start    = inbound_date_start
                                , inbound_date_end      = inbound_date_end
                                , K                     = K
                                , carrier               = carrier
                                , adults                = adults
                                , cabinclass            = cabinclass ).PV01()

    return portfolio_delta


def start_workers(mkt_date : datetime.date, worker_ports : List[int]) -> List[PortfolioWorker] :
    """ Sets the workers and starts their .start function.

    :param mkt_date: market date
    :param worker_ports: number of ports where these workers are listening to.
    :returns: workers listening to required ports.
    """

    workers = []
    for worker_idx, worker_port in enumerate(worker_ports):
        curr_worker = PortfolioWorker( NanoSocketMixin.create_socket(port=worker_port, pub_sub='pair,recv')  # TODO: CHECK HERE, THIS IS PROBABLY WRONG.
                                     , revalue_portfolio = lambda portfolio : revalue_ao_portfolio(portfolio, mkt_date = mkt_date)
                                     , worker_name       = 'Worker{0}'.format(worker_idx) )
        curr_worker.start()
        workers.append(curr_worker)

    return workers
