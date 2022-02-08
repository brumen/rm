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
