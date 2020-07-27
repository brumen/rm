# produces market events

from rm.in_out_updater import InOutUpdater
from rm.input_classes  import SocketInputSource


class MarketTicker(SocketInputSource):

    def _update_value(self):
        receive_from_source = super()._update_value()
        return receive_from_source  # TODO: MAYBE THIS IS NOT NECESSARY
        # while True:
        #     self._pub_socket.send(dumps({'timestamp': datetime.datetime.now(), 'UA-176': 100.}))
        #     time.sleep(sleep_time)


class MarketToPositions2(InOutUpdater):

    def __init__(self, mkt_socket, position_socket):  # TODO: THIS IS WRONG.

        self.mkt_input       = self.input(mkt_socket)
        self.position_output = self.output(position_socket)

    def transform(self):
        """ Transforms the inputs -> outputs.
        """

        # TODO: DO SOMETHING USEFUL HERE
        # fictious transformation
        self.position_output << ('trade1', 'trade2')
