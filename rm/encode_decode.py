#

import datetime
import sys
import json
import logging
sys.path.append('/home/brumen/work/rm/ao/')

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class EncodeDecodeMixin:
    """ Mixin class for json message encoding and decoding.
    """

    def _decode_message(self, msg_from_worker):
        """ Decodes the message from the worker.

        :param msg_from_worker: message from worker.
        :returns:
        """

        return json.loads(msg_from_worker.decode('utf-8'))

    def _datetime_converter(self, date_obj):
        """ Converter of datetime.date objects for json.

        :param date_obj:
        :return:
        """

        if isinstance(date_obj, datetime.date):
            return date_obj.__str__()

    def _encode_msg(self, msg):
        """ Encoding of messages

        :param msg: message to be json encoded.
        :returns:
        """

        return json.dumps(msg, default=self._datetime_converter)  # to convert datetime objects
