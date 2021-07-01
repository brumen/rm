""" AO controller by keeping the trade information in a dictionary.
"""

# computing the delta publisher

import datetime
import logging

from typing import Optional, Dict, Union, Tuple

from ao.flight            import AOTrade
from ao.air_option_derive import AirOptionFlightsExplicit, AOTradeException
from ao.delta_dict import DeltaDict

from rm.controller_ao_by_trade import ControllerAOByTrade


logging.basicConfig(filename='/tmp/controller_ao_by_trade_delta.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ControllerAOByTradeDelta(ControllerAOByTrade):
    """ Controller for AirOptions.

    report on ao_results_by_trade topic on kafka
    """

    @staticmethod
    def _trade_result_agg_single(trade_pv_1 : Optional[DeltaDict], trade_pv_2 : Optional[DeltaDict]) -> Union[DeltaDict, None]:
        """ Aggregation function for trade_1 and trade_2, where trade_pv_1 and trade_pv_2 are dictionaries

        Merging of the dicts, DeltaDicts are of type [int, float]

        :param trade_pv_1: dictionary of position aggregates for the existing trades.
        :param trade_pv_2: dictionary of position for the second trade, like {trade_2: PV(trade_2)}
        :returns:
        """

        if trade_pv_1 is None:
            if trade_pv_2 is None:
                return DeltaDict({})  # empty dict

            _, trade_2_delta = trade_pv_2  # ignoring the trade_id_2
            return trade_2_delta

        if trade_pv_2 is None:  # trade_pv_1 is not None
            return trade_pv_1

        # merge two dicts
        _, trade_2_delta = trade_pv_2  # ignoring the trade_id_2
        return trade_pv_1 + trade_2_delta

    @staticmethod
    def _value_trade_id(mkt_date_trade_id : Tuple[datetime.date, Tuple[int, str]], db_session = None) -> Tuple[int, Dict[int, float]]:
        """ Returns the PV of the trade with trade_id.

        :param mkt_date_trade_id: market date and trade id as a tuple (useful for spark calculations)
        :param db_session: sql alchemy session.
        :returns: tuple of trade_id, and PV of the referenced trade.
        """

        mkt_date, (trade_id, trade_direction) = mkt_date_trade_id

        trade_value = ControllerAOByTradeDelta._value_trade((mkt_date, ControllerAOByTrade._retrieve_tradeao(trade_id, db_session)))

        return trade_id, trade_value if trade_direction == 'c' else - trade_value

    @staticmethod
    def _value_trade(mkt_date_trade: Tuple[datetime.date, Optional[AOTrade]]) -> Optional[DeltaDict]:
        """ Returns the PV01 of a air option trade with specific trade id.

        :param mkt_date_trade: tuple of market date and AOTrade.
        :returns: delta of the trade specified.
        """

        mkt_date, ao_trade = mkt_date_trade

        if ao_trade is None:
            return DeltaDict({})

        # ao_trade is not None, price.
        try:
            return AirOptionFlightsExplicit( mkt_date, ao_trade.flights, ao_trade.strike).PV01()

        except AOTradeException:  # fails in AOTrade
            logger.error(f'Trade {ao_trade.position_id} could not be found in the database.')
            return None

        except Exception as e:
            logger.error(f'Trade {ao_trade.position_id} could not be priced. Reason: {str(e)}')
            return None


# example
def main():
    ao_by_trade = ControllerAOByTradeDelta(topic_to_publish_to='ao_results_by_trade')
    ao_by_trade.start()

# main()
