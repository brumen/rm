# class for encoding and decoding messages.

import datetime
import json
import logging


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class EncodeDecodeMixin:
    """ Mixin class for json message encoding and decoding.
    """

    @staticmethod
    def decode_message(msg_from_worker : str):
        """ Decodes the message from the worker.

        :param msg_from_worker: message from worker.
        :returns: json representation of the message.
        """

        return json.loads(msg_from_worker.decode('utf-8'))

    @staticmethod
    def _datetime_converter(date_ : datetime.date) -> str:
        """ Converter of datetime.date objects for json.

        :param date_: date which needs to be converted to string, json format
        :returns: date in the string format.
        """

        if isinstance(date_, datetime.date):
            return date_.__str__()

        raise NotImplementedError('Type handling for {0} not implemented'.format(type(date_)))

    @staticmethod
    def encode_msg(msg) -> str:
        """ Encoding of messages

        :param msg: message to be json encoded.
        :returns: string representing the encoding of the message
        """

        return json.dumps(msg, default=EncodeDecodeMixin._datetime_converter)  # to convert datetime objects
