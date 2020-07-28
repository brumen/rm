# class implements the in-out updater

from threading import Thread
from time      import sleep
from uuid      import uuid4
from queue     import Queue

from rm.socket_msg    import NanoSocketMixin, NNGSocketMixin
from rm.encode_decode import EncodeDecodeMixin


class InOutUpdaterException(Exception):
    pass


class InputClass(EncodeDecodeMixin):

    def __init__(self, name : str, queue_length = None, sleep_time = .0001 ):
        """ Initiates the input class.

        :param name: name of the class, this is usually internally set.
        :param queue_length: length of the queue that is kept.
        :param sleep_time: amount of sleep the process does between looking for new values.
        """

        self.__name        = name
        self._queue_length = queue_length
        self._sleep_time   = sleep_time
        self.__value       = None

        # internal variables
        self.__value_has_changed = True

    @property
    def value(self):
        return self.__value

    @value.setter
    def value(self, new_value):
        if not self._queue_length:
            self.has_changed = True
            self.__value = new_value
        else:
            if not self.__value:  # queue is not initiated
                self.__value = Queue(maxsize=self._queue_length)

            self.__value.put(new_value)  # add new value to the queue

    @property
    def has_changed(self):
        return self.__value_has_changed

    @has_changed.setter
    def has_changed(self, new_value):
        self.__value_has_changed = new_value

    def __call__(self):
        return self.value

    def _update_value(self):
        raise NotImplementedError('method _update_value not implemented.')

    def run(self):
        Thread(target=self._update_value).start()


# TODO: REMOVE THIS LATER< JUST FOR TESTING
class RandomInputSource(InputClass):
    """ Alternates between two values of the input class.

    """

    VALUES = (1, 2)

    def _update_value(self):
        while True:
            self.value = self.VALUES[0]
            sleep(self._sleep_time)
            self.value = self.VALUES[1]
            sleep(self._sleep_time)


# TODO: REMOVE THIS, JUST FOR TESTING
class RandomInputSource2(RandomInputSource):

    VALUES = (3, 4)


class SocketInputSource(InputClass):

    def __init__(self, name : str, source, queue_length = None, sleep_time = .0001):
        """

        :param name: name of the input source.
        :param source: socket source
        :param queue_length:
        :param sleep_time:
        """

        super().__init__(name, queue_length=queue_length, sleep_time=sleep_time)
        self._source = source

    @classmethod
    def from_source(cls, port : int , host : str = '127.0.0.1', queue_length= None, sleep_time = .0001):
        """ A simpler way to construct the class.

        :param port: port where the socket connects
        :param host: host of the source.
        :param queue_length: length of the queue, if None, no queue.
        :param sleep_time: time to sleep between updates.
        """

        return cls( str(uuid4())
                  # , NanoSocketMixin.create_socket(port, pub_sub='pub', host=host)
                  , NNGSocketMixin.create_socket(port, pub_sub = 'pub', host = host)
                  , queue_length = queue_length
                  , sleep_time   =sleep_time )

    def _update_value(self):
        return self._source.recv()


class InputFactory:
    """ Factory method for the input sources.
    """

    @staticmethod
    def input(input_type, *args, **kwargs):
        if input_type == 'BaseInput':
            return InputClass(*args, **kwargs)

        # TODO: REMOVE THIS LATER, JUST FOR TESTING
        if input_type == 'RandomInput':
            return RandomInputSource(*args, **kwargs)

        # TODO: REMOVE THIS LATER
        if input_type == 'RandomInput2':
            return RandomInputSource2(*args, **kwargs)

        if input_type == 'SocketInput':
            return SocketInputSource(*args, **kwargs)
