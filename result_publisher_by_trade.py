import logging
import tkinter as tk
import numpy   as np
import pandas  as pd
import requests

from typing      import Dict
from time        import sleep
from threading   import Thread
from json        import loads
from kafka       import KafkaConsumer
from pandastable import Table

logging.basicConfig(filename = '/tmp/rm_results_by_trade.log', level = logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ResultPublisherBase:
    """ Base class for publishing results.
    """

    def __init__(self):

        # values of the portfolio
        self.curr_value = {}  # current results value of the portfolio

        # Tk canvas for display
        self._root = tk.Tk()
        self._frame = tk.Frame(self._root)
        self._frame.pack()
        self._results_table = Table(self._frame, showtoolbar=True, showstatusbar=True)
        self._results_table.show()

    def _get_results(self):
        """ Gets the results from Kafka.

        :returns: None, just updates curr_value, new_value, and trades
        """

        raise NotImplemented(f'Implement the function that updates self.curr_value')

    def update_results(self):
        """ Updates the pandas table w/ the results.
        """

        if isinstance(self.curr_value, np.ndarray):
            self._results_table.model.df = pd.DataFrame(self.curr_value)
            self._results_table.redraw()

        self._root.after(1, self.update_results)

    def start(self):
        # TODO: MAYBE FIX THREADING HERE!!!

        # thread for reading results
        thread_updating = Thread(target=self._get_results)
        thread_updating.start()

        self._root.after(1, self.update_results)
        self._root.mainloop()  # looping the tk canvas


class ResultPublisherKafka(ResultPublisherBase):
    """ Results are obtained from Kafka.
    """

    def __init__( self
                  , server_port_topic = ('localhost', 9092, 'ao_results_by_trade')
                  , ):

        super().__init__()

        server, port, topic = server_port_topic

        self._subscriber = KafkaConsumer(topic, bootstrap_servers=f'{server}:{port}')

    def _get_results(self):
        """ Gets the results from Kafka.

        :returns: None, just updates curr_value, new_value, and trades
        """

        for msg in self._subscriber:
            logger.debug(f'Processing trades. {self._trades_curr_working} in the current queue, {self._trades_new_working} in the new queue.')
            field, value = loads(msg.value)  # value is json encoded

            if field == 'curr_market':
                self.curr_value = np.array([]) if value is None else np.array(list(value.items()))


class ResultPublisherRester(ResultPublisherBase):
    """ Results are obtained from rester and displayed, somewhat processed.
    """

    def __init__( self
                  , rester_addr = 'http://localhost:5001/mkt/get_market_2'
                  , sleep_time  = 5
                  , ):
        """ Display the portfolio results from the rester.

        :param rester_addr: address from which the results are read.
        :param sleep_time: sleep time in seconds between refreshes.
        """

        super().__init__()

        self._rester_addr = rester_addr
        self._sleep_time  = sleep_time

    def _get_results(self) -> None:
        """ Gets the results from rester and update self.curr_value

        :returns: updates curr_value which is then displayed.
        """

        while True:
            logger.info('Obtaining new result batch.')
            self.curr_value = self.decode_results(requests.get(self._rester_addr).json())

            sleep(self._sleep_time)

    @staticmethod
    def decode_results(results : Dict[str, float]) -> np.ndarray:
        """ Processing the results.

        """
        results_d = results.get('PV01', {})

        return np.array(list(results_d.items()))


def main():
    rp = ResultPublisherRester()
    rp.start()


main()
