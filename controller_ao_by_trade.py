""" AO controller by keeping the trade information in a dictionary.
"""

# concrete implementation of the controller, used

import datetime
import logging

from typing import Optional, Dict, Union, Tuple

from rm.controller_ao2 import ControllerAO

logging.basicConfig(filename='/tmp/controller.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ControllerAOByTrade(ControllerAO):
    """ Controller for AirOptions.

    report on ao_results_by_trade topic on kafka
    """

    @staticmethod
    def _trade_result_agg_single(trade_pv_1 : Optional[Dict[int, float]], trade_pv_2 : Optional[Tuple[int, float]]) -> Union[Dict[int, float], None]:
        """ Aggregation function for trade_1 and trade_2, where trade_pv_1 and trade_pv_2 are dictionaries

        Merging of the dicts.

        :param trade_pv_1: dictionary of position aggregates for the existing trades.
        :param trade_pv_2: dictionary of position for the second trade, like {trade_2: PV(trade_2)}
        :returns:
        """

        if trade_pv_1 is None:
            if trade_pv_2 is None:
                return {}  # empty dict

            trade_2_id, trade_2_pv = trade_pv_2
            return {trade_2_id: trade_2_pv}

        if trade_pv_2 is None:  # trade_pv_1 is not None
            return trade_pv_1

        # aggregation of two dictionaries
        trade_2_id, trade_2_pv = trade_pv_2
        if trade_2_id not in trade_pv_1:
            trade_pv_1[trade_2_id] = trade_2_pv
        else:
            trade_pv_1[trade_2_id] += trade_2_pv

        return trade_pv_1

    @staticmethod
    def _value_trade_id(mkt_date_trade_id : Tuple[datetime.date, Tuple[int, str]], db_session = None) -> Tuple[int, float]:
        """ Returns the PV of the trade with trade_id.

        :param mkt_date_trade_id: market date and trade id as a tuple (useful for spark calculations)
        :param db_session: sql alchemy session.
        :returns: tuple of trade_id, and PV of the referenced trade.
        """
        mkt_date, (trade_id, trade_direction) = mkt_date_trade_id

        trade_value = ControllerAO._value_trade((mkt_date, ControllerAO._retrieve_tradeao(trade_id, db_session)))

        return (trade_id, trade_value) if trade_direction == 'c' else (trade_id, - trade_value)


# example
def main():
    ao_by_trade = ControllerAOByTrade(topic_to_publish_to='ao_results_by_trade')
    ao_by_trade.start()

# main()
