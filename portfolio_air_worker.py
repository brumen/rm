# portoflio worker relating to Air options.

import datetime
import logging

from rm.delta_dict       import DeltaDict
from ao.air_option       import AirOptionMock
from rm.portfolio_worker import PortfolioWorker


logger = logging.getLogger(__name__)
logger.setLevel('INFO')  # log at info level


class PortfolioAirWorker(PortfolioWorker):
    """ Worker specifically relating to Air Options.
    """

    @staticmethod
    def revalue_portfolio(portfolio, mkt_date : datetime.date) -> DeltaDict:
        """ Revalues the portfolio given.

        :param portfolio: trade portfolio to use.
        :param mkt_date: market date
        :return:
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
