""" Displaying the results in a table.
"""

import logging
import requests
import tkinter as tk
import numpy   as np
import pandas  as pd

from typing      import Dict, Optional, Any
from time        import sleep
from threading   import Thread
from json        import loads
from kafka       import KafkaConsumer
from pandastable import Table

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

        raise NotImplementedError(
            f'Implement the function that updates {self.curr_value}'
        )

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

    def __init__(
            self,
            server_port_topic=('localhost', 9092, 'air_options.ao.results'),
            metric: str = 'PV',
    ):
        """ Kafka receiver.

        :param server_port_topic: server, port and topic where to read data.
        :param metric: metric which should be extracted from the message,
             currently only 'PV', 'PV01'.
        """

        super().__init__()

        server, port, topic = server_port_topic
        self._server_port_topic = server_port_topic
        self.metric = metric

        self._subscriber = KafkaConsumer(
            topic,
            bootstrap_servers=f'{server}:{port}'
        )

        # for processing
        self._current_value = None
        self._prev_value = None

    def _get_results(self):
        """ Gets the results from Kafka.

        :returns: None, just updates curr_value, new_value, and trades
        """

        for msg in self._subscriber:
            logger.debug(f'Processing trades from {self._server_port_topic}.')
            logger.info(f'Got message: {msg.value}')

            # updating state
            self._prev_value = self._current_value
            self._current_value = loads(msg.value)

            self.curr_value = self._process_result(
                self._current_value,
                self._prev_value,
            )

    def _process_result(
            self,
            current_result: Optional[Dict[str, Any]],
            prev_result: Optional[Dict[str, Any]],
    ):
        raise NotImplementedError(
            'Need to implement the _process_result method'
        )


class ResultPublisherKafkaPV(ResultPublisherKafka):

    def _process_result(
            self,
            current_result: Optional[Dict[str, Dict[str, float]]],
            prev_result: Optional[Dict[str, Dict[str, float]]],
    ):
        """ Processing the PV result.

        :param result_dict: dictionary of results, the keys are PV, PV01,
           the computed requests. Value is a
           dictionary of flight names, and values of that flight.
        """

        if current_result is None:
            return np.array([])

        results = current_result[self.metric]
        sort_results = np.array(sorted(results.items(),
                                       key=lambda trade: int(trade[0])
                                       )
                                )

        return sort_results


class ResultPublisherKafkaPV_Useless(ResultPublisherKafka):

    def _get_results(self):
        super()._get_results()
        # self._subscriber.seek_to_end()

    def _process_result(
            self,
            curr_result: Optional[Dict[str, Dict[str, float]]],
            prev_result: Optional[Dict[str, Dict[str, float]]],
    ):
        """ Processing the PV result.

        :param result_dict: dictionary of results, the keys are PV,
           PV01, the computed requests. Value is a
           dictionary of flight names, and values of that flight.
        """

        if curr_result is None:
            return np.array([[]])

        proper_results = curr_result.get(self.metric)
        if proper_results is None:
            return prev_result  # nothing new to display

        # sort the results:
        itemized_l = []
        for trade_id_date, trade_val in proper_results.items():
            itemized_l.append((trade_id_date.split('|')[0], trade_val))

        logger.info(f"Published list has {len(itemized_l)} trades")
        return np.array(sorted(itemized_l,
                               key=lambda trade_id_date: int(trade_id_date[0]))
                        )


class ResultPublisherKafkaPV01_Useless(ResultPublisherKafka):

    def _get_results(self):
        super()._get_results()
        # self._subscriber.seek_to_end()

    def _process_result(
            self,
            curr_result: Optional[Dict[str, Dict[str, float]]],
            prev_result: Optional[Dict[str, Dict[str, float]]],
    ):
        """ Processing the PV result.

        :param result_dict: dictionary of results, the keys are PV,
           PV01, the computed requests. Value is a
           dictionary of flight names, and values of that flight.
        """

        if curr_result is None:
            return np.array([[]])

        proper_results = curr_result.get(self.metric)
        if proper_results is None:
            return prev_result

        # sort the results:
        itemized_l = []
        for trade_id_date, trade_val in proper_results.items():
            itemized_l.append((trade_id_date.split('|')[0], trade_val))

        logger.info(f"Published list has {len(itemized_l)} trades")
        return np.array(sorted(itemized_l, key=lambda trade_id: trade_id))


class ResultPublisherKafkaPV_Useless2(ResultPublisherKafka):

    def _process_result(
            self,
            curr_result: Optional[Dict[str, Dict[str, float]]],
            prev_result: Optional[Dict[str, Dict[str, float]]],
    ):
        """ Processing the PV result.

        :param result_dict: dictionary of results, the keys are PV,
           PV01, the computed requests. Value is a
           dictionary of flight names, and values of that flight.
        """

        if curr_result is None or prev_result is None:
            return np.array([[]])

        curr_pv = curr_result['PV']
        prev_pv = prev_result['PV']

        # sort the results:
        curr_trade_pv = []
        for trade_id_date, trade_val in curr_pv.items():
            curr_trade_pv.append((trade_id_date.split('|')[0], trade_val))

        prev_trade_pv = []
        for trade_id_date, trade_val in prev_pv.items():
            prev_trade_pv.append((trade_id_date.split('|')[0], trade_val))

        curr_trade_pv_sorted = sorted(
            curr_trade_pv,
            key=lambda trade_id_date: trade_id_date[0]
        )
        prev_trade_pv_sorted = sorted(
            prev_trade_pv,
            key=lambda trade_id_date: trade_id_date[0]
        )

        all_pv = []
        for curr_pv_elt, prev_pv_elt in zip(
                curr_trade_pv_sorted,
                prev_trade_pv_sorted
        ):
            all_pv.append(
                (curr_pv_elt[0],
                 curr_pv_elt[1],
                 curr_pv_elt[1] - prev_pv_elt[1]
                 )
            )

        return np.array(all_pv)


class ResultPublisherKafkaPV01(ResultPublisherKafka):

    def _process_result(self,
                        result_dict: Optional[Dict[str, Dict[str, float]]],
                        prev_result: Optional[Dict[str, Dict[str, float]]],
                        ):
        """ Processing the PV result.

        :param result_dict: dictionary of results, the keys are PV,
           PV01, the computed requests. Values is
           a dictionary of PV01s with respect to that flight.
        """

        return np.array([]) if result_dict is None \
            else np.array(list(result_dict['PV01'].items()))


class ResultPublisherRester(ResultPublisherBase):
    """ Results are obtained from rester and displayed, somewhat processed.
    """

    def __init__(
            self,
            rester_addr='http://localhost:5001/mkt/get_market_2',
            sleep_time=5,
    ):
        """ Display the portfolio results from the rester.

        :param rester_addr: address from which the results are read.
        :param sleep_time: sleep time in seconds between refreshes.
        """

        super().__init__()

        self._rester_addr = rester_addr
        self._sleep_time = sleep_time

    def _get_results(self) -> None:
        """ Gets the results from rester and update self.curr_value

        :returns: updates curr_value which is then displayed.
        """

        while True:
            logger.info('Obtaining new result batch.')

            try:
                results = requests.get(self._rester_addr)

            except ConnectionError as ce:
                logger.warning(
                    f'Could not connect to {self._rester_addr}: {ce}'
                )
                results_final = {}

            except Exception as e:
                logger.warning(f'Weird error: {e}')
                results_final = {}

            finally:  # no exception
                # we got the results, convert from json
                results_final = results.json()

            self.curr_value = self.decode_results(results_final)

            sleep(self._sleep_time)

    @staticmethod
    def decode_results(results: Dict[str, float]) -> np.ndarray:
        """ Processing the results.

        :param results: results to be decoded
        """

        if results is None:
            return np.array([])

        return np.array(list(results.get('PV', {}).items()))


class ResultPublisherLETF(ResultPublisherKafka):

    def _process_result(
            self,
            current_result: Optional[Dict[str, float]],
            prev_result: Optional[Dict[str, float]],
    ):
        """ Processing the PV result.

        :param result_dict: dictionary of results, the keys are PV, PV01,
           the computed requests. Value is a
           dictionary of flight names, and values of that flight.
        """

        return np.array([]) if current_result is None \
            else np.array(list(sorted(current_result[self.metric].items(),
                                      key=lambda x: int(x[0])))
                          )


# rp = ResultPublisherKafkaPV(metric='PV01')
# rp.start()
