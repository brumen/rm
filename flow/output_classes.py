# output classes for communication.
from uuid      import uuid4

from rm.sockets.socket_msg import NNGSocketMixin
from rm.sockets.encode_decode import EncodeDecodeMixin


class OutputClassException(Exception):
    pass


class OutputClass(EncodeDecodeMixin):
    """ Publishes values to the source.
    """

    def __init__(self, name : str, value = None ):
        """

        :param name: name of the output class.
        :param value: value that the output class is initiated with, default None.
        """

        self._name     = name
        self._value    = value

        # internal states
        self.__value_has_changed = False   # indicator if the value is changed

    @property
    def value(self):
        return self._value

    @property
    def has_changed(self):
        return self.__value_has_changed

    @has_changed.setter
    def has_changed(self, new_value):
        self.__value_has_changed = new_value

    def __lshift__(self, value):
        """ Executes the transform function and publishes the value

        :param value: value to assign to the output class.
        """

        self._value = value
        self.has_changed = True

    def __call__(self):
        return self.value

    def _update_value(self):
        if self.has_changed:
            return self.value

    def __or__(self, input_class):
        """ Chaining of the output to the input with | symbol (or class method).
        """

        # TODO: HERE IMPLEMENT
        pass


class SocketOutputSource(OutputClass):
    """ Output source where output is a socket.
    """

    def __init__(self, name : str, source, sleep_time = .0001):
        """

        :param name: name of the input source.
        :param source: socket source
        :param sleep_time:
        """

        super().__init__(name)
        self._source = source
        self._sleep_time = sleep_time

    @classmethod
    def from_source(cls, port : int, host : str = '127.0.0.1', sleep_time = .0001):
        """ A simpler way to construct the class.

        :param port: port where the socket connects
        :param host: host of the source.
        :param sleep_time: time to sleep between updates.
        """

        return cls( str(uuid4())
                  #, NanoSocketMixin.create_socket(port, pub_sub='sub', host=host)
                  , NNGSocketMixin.create_socket(port, pub_sub='pub', host=host)
                  , sleep_time   =sleep_time )

    def _update_value(self):
        if self.has_changed:
            return self._source.send(self.value)


class OutputFactory:

    @staticmethod
    def output(input_type, name, *args, **kwargs):
        if input_type == 'BaseOutput':
            return OutputClass(name, *args, **kwargs)

        if input_type == 'RandomInput':
            return SocketOutputSource(name, *args, **kwargs)
