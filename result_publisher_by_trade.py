import logging
import tkinter as tk
import numpy   as np
import pandas  as pd

from threading   import Thread
from json        import loads
from kafka       import KafkaConsumer
from pandastable import Table

logging.basicConfig(filename = '/tmp/rm_results_by_trade.log', level = logging.INFO)
logger = logging.getLogger(__name__)


class ResultPublisherByTrade:

    def __init__(self
                , server = 'localhost'
                , port   = 9092
                , topic  = 'ao_results_by_trade' ):

        logger.debug('Initializing ResultPublisherByTrade.')
        self._server = server
        self._port   = port
        self._topic  = topic
        self._subscriber = KafkaConsumer(topic, bootstrap_servers=f'{server}:{port}')

        # values of the portfolio
        self.curr_value = {}  # current value
        self.new_value  = {}  # value evaluated on the new market
        self._trades_curr_working = 0
        self._trades_new_working  = 0

        # Tk canvas for display
        self._root = tk.Tk()
        self._frame = tk.Frame(self._root)
        self._frame.pack()
        self._results_table = Table(self._frame, showtoolbar=True, showstatusbar=True)
        self._results_table.show()

    def _get_results_ao(self):
        """ Gets the results from Kafka.

        :returns: None, just updates curr_value, new_value, and trades
        """

        for msg in self._subscriber:
            logger.debug(f'Processing trades. {self._trades_curr_working} in the current queue, {self._trades_new_working} in the new queue.')
            field, value = loads(msg.value)  # value is json encoded
            if field == 'curr_market':
                if value is None:
                    self.curr_value = np.array([])
                else:
                    self.curr_value = np.array(list(value.items()))
            elif field == 'new_market':
                if value is None:
                    self.new_value = np.array([])
                else:
                    self.new_value = np.array(list(value.items()))
            elif field == 'curr_trades':
                self._trades_curr_working = value
            elif field == 'new_trades':
                self._trades_new_working = value

    def update_results(self):
        """ Updates the pandas table w/ the results.
        """

        if isinstance(self.curr_value, np.ndarray):
            self._results_table.model.df = pd.DataFrame(self.curr_value)
            self._results_table.redraw()
        self._root.after(1, self.update_results)

    def start(self):
        # thread for reading results
        thread_updating = Thread(target=self._get_results_ao)
        thread_updating.start()

        self._root.after(1, self.update_results)
        self._root.mainloop()  # looping the tk canvas


class ResultPublisher(ResultPublisherByTrade):
    """ Publisher of aggregated results.
    """

    def __init__(self
                , server = 'localhost'
                , port   = 9092
                , topic  = 'ao_results' ):

        super().__init__(server=server, port=port, topic=topic)
        self._results_table.showIndex()

    def _get_results_ao(self):
        """ Gets the results from Kafka and attributes them to self.curr_value etc.

        :returns: None, just updates curr_value, new_value, and trades
        """

        for msg in self._subscriber:
            logger.debug(f'Processing trades. {self._trades_curr_working} in the current queue, {self._trades_new_working} in the new queue.')
            field, value = loads(msg.value)  # value is json encoded
            if field == 'curr_market':
                if value is None:
                    self.curr_value = {}
                else:
                    self.curr_value = value['PV01']

            elif field == 'new_market':
                if value is None:
                    self.new_value = {}
                else:
                    self.new_value = value['PV01']

            elif field == 'curr_trades':
                self._trades_curr_working = value

            elif field == 'new_trades':
                self._trades_new_working = value

    def update_results(self):
        """ Updates the pandas table w/ the results.
        """

        if isinstance(self.curr_value, dict):
            self._results_table.model.df = pd.DataFrame.from_dict(self.curr_value, orient='index')
            self._results_table.redraw()
        self._root.after(1, self.update_results)


def main():
    rp = ResultPublisher(topic='ao_results')
    rp.start()


# main()
