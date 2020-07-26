# produces market events

import time
import datetime

from json   import dumps
from typing import Set

from rm.positions_updater import PositionUpdater
from rm.in_out_updater    import InOutupdater


class MarketTicker(PositionUpdater):
    """ Produces market events that have changed and publishes them to a nonmsg queue.
    """

    def start(self, sleep_time = .3) -> None:
        """ Starts the market ticker.

        :return:
        """

        while True:
            self._pub_socket.send(dumps({'timestamp': datetime.datetime.now(), 'UA-176': 100.}))
            time.sleep(sleep_time)


class MarketToPositions(PositionUpdater):

    def __init__(self
                 , recv_mkt_socket     : Socket
                 , pub_positions_socket: Socket ):

        self._pub_positions_socket = pub_positions_socket
        self._recv_mkt_socket      = recv_mkt_socket

        # list of updated trade positions
        self.__updated_trades = set()  # empty set, no new positions yet


    def _mkt_event_to_position(self, mkt_event) -> Set[str]:
        """ Produces the positions that have changed as a result of mkt_event.

        :returns: set of positions that have changed as a result of this event.
        """

        return {'pos1'}


    def __update_trades(self, mkt_event):
        """ Update trades according to the market event.
        """

        self.__updated_trades = self.__updated_trades.union(self._mkt_event_to_position(mkt_event))

    def __update_processed_trades(self, processed_trades : Set[str]):
        """ Deducts the trades that were updated.
        """

        self.__updated_trades = self.__updated_trades.difference(processed_trades)

    def __listen_to_mkt_updates(self, LLL):

        while True:
            new_market_event = self._recv_mkt_socket.recv()

    def start(self, sleep_time = .3):
        """ Listens on a socket and updates the positions

        """


class MarketToPositions2(InOutupdater):

    def __init__(self, mkt_socket, position_socket):

        self.mkt_input       = self.input(mkt_socket)
        self.position_output = self.output(position_socket)

    def transform(self):
        """ Transforms the inputs -> outputs.
        """

        # TODO: DO SOMETHING USEFUL HERE
        # fictious transformation
        self.position_output << ('trade1', 'trade2')
