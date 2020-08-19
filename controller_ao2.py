# concrete implementation of the controller.

from typing  import List
from pyspark import SparkContext

from rm.controller2 import Controller


class ControllerAO2(Controller):
    """ Specification for the workers.
    """

    def __init__(self):
        """
        """

        super().__init__(self._value_portfolio_fct)

    def _get_total_current_portfolio(self) -> List:
        return ['TRADE1'] * 100

    def _value_portfolio_fct(self, new_trades):
        """ Defines the portfolio_function from trades -> results.

        :return:
        """

        print("HHH {0}".format(len(new_trades)))
        return len(new_trades)


class ControllerAO(ControllerAO2):
    """ Controller for AirOptions.
    """

    def __init__(self):
        super().__init__()
        self.sc = SparkContext()  # TODO: THIS SHOULD BE PROPERLY DEFINED

    def _value_trade(self, trade):
        return 1.

    def _value_portfolio_fct(self, new_trades):
        """ Defines the portfolio_function from trades -> results.

        :return:
        """

        return self.sc.range(new_trades).filter(self._value_trade)  # TODO: THIS IS BULLSHIT, BUT AT LEAST SOMETHING


# from rm.market_ticker import MarketUpdater
# from rm.positions_updater import PositionUpdaterKafka


# class ControllerWithInputs(Controller):
#
#     def __init__(self, server : str, port : int = '9092'):
#         self.position_updater = PositionUpdaterKafka(server, port)
#         self.market_updater   = MarketUpdater(server, port)
#
#         super().__init__(VALUE_PORTFOLIO_FCT)
#         # TODO:
