# class implements the in-out updater

from threading import Thread
from time      import sleep
from uuid      import uuid4
from queue     import Queue
from typing    import Union, List, Tuple, Dict, Optional
from json      import loads
from kafka     import KafkaConsumer, TopicPartition

from rm.controller_ao2 import ControllerAO, get_trade


class InOutUpdaterException(Exception):
    pass


class KafkaInput:
    """ Uses Kafka stream as an input.
    """

    def __init__(self
                 , name : str
                 , server      = 'localhost'
                 , port        = 9092
                 , topic_input = 'air_options.ao.option_positions' ):
        self.name         = name
        self._server      = server
        self._port        = port
        self._topic_input = topic_input

        self._subscriber = KafkaConsumer(bootstrap_servers=f'{server}:{port}')
        self._subscriber.assign([TopicPartition(topic=topic_input, partition=0)])
        self._subscriber.seek_to_beginning()

    def __next__(self):
        """ Generator for the subscriber topic.
        """
        return next(self._subscriber)

    def __iter__(self):
        return self


class FlightMemorizer(KafkaInput):
    """ Obtains the flights needed, and prices memorized.
    """

    def __init__( self
                  , name : str
                  , server       : str = 'localhost'
                  , port         : int = 9092
                  , topic_input  : str = 'air_options.ao.option_positions'
                  , queue_list   : Optional[List[Queue]] = None ):
        """

        :param queue_list: list of queues where new positions should be published.
        """

        super().__init__(name, server=server, port=port, topic_input=topic_input)

        self.flights      = set([])  # list of all flights associated w/ the trade.
        self.queue_list   = queue_list

    def _all_trades(self):
        """ Accessory function for running it as thread. Updates the self.flights variable.
        """

        for msg in self:

            msg_payload = self._decode_msg(msg)
            if msg_payload is None:
                continue

            # we have a proper message, continue.
            event_type  = self._event(msg_payload)
            trade_id    = msg_payload['after']['position_id']  # adding this position id
            flights     = self._get_flights(trade_id)

            if event_type in ( 'c', 'u', ):  # create event, update
                self.flights.union(flights)
                position = (trade_id, 'c')  # 'c' for create, 'd' for delete
                for q in self.queue_list:
                    q.put(position)

            else:  # unknown type of event, raise RuntTimeError
                raise RuntimeError(f'Unknown event type: {event_type}')

            self.new_flights = True

    def _get_flights(self, trade_id : int) -> List[Tuple[str, str, str, float]]:
        """ Get flights for the trade id given.

        :param trade_id: trade id
        :returns: list of all flights associated w/ the trade,
                  element of the list is a tuple of (origin, destination, carrier, flight price)
        """
        trade_ao = get_trade(trade_id)
        flights = trade_ao.flights  # list of flights

        flight_prices = []
        for flight in trade_ao.flights:
            all_flight_prices = flight.prices
            if not all_flight_prices:  # empty list
                # TODO: FIX THIS
                latest_price = 200.
            else:
                latest_price = all_flight_prices[-1].price

            flight_prices.append((flight.orig, flight.dest, flight.carrier, latest_price)) # TODO: FIX HERE

        return flight_prices

    def _event(self, msg_payload : Dict):
        """ Extract the event from the message.

        :param msg:
        :returns: 'c' for create, 'd' for delete.
        """

        return msg_payload['op']  # either c - create, d - delete, u - update

    def _decode_msg(self, msg):
        """ Decode the message.
        """

        if msg.value is None:  # TODO: This here is wrong, should be a message.
            return None

        msg_decoded = loads(msg.value.decode())

        return msg_decoded['payload']

    def run(self):
        Thread(target=self._all_trades, daemon=True).start()



class KafkaInputOutput(KafkaInput):
    """ Takes the inputs, transforms them, and publishes to output.
    """

    def __init__( self
                  , name : str
                  , server       = 'localhost'
                  , port         = 9092
                  , topic_input  = 'ao_results_by_trade'
                  , topic_output = 'ao_results_ouput'
                  , ):

        super().__init__(name, server=server, port=port, topic_input=topic_input)
        self._topic_output = topic_output

        self._publisher = KafkaProducer(bootstrap_servers=f'{server_name}:{port}')

    def __next__(self):
        msg = super().__next__()
        self._publisher.send(self._topic_output, value=self._transform_msg(msg))

    def _transform_msg(self, msg):
        return msg
