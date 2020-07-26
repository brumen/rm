# class implements the in-out updater

from uuid      import uuid4
from threading import Thread

from rm.nanomsg       import NanoSocketMixin
from rm.encode_decode import EncodeDecodeMixin


class InOutUpdaterException(Exception):
    pass


class InputClass(EncodeDecodeMixin):

    def __init__(self, name, source, queue_length = None, sleep_time = .0001 ):
        """

        """

        self.__name = name
        self.__source = source
        self.__queue_length = queue_length
        self.__sleep_time = sleep_time

        # internal variables
        self.__value = None

    @classmethod
    def from_source(cls, name, source_name, queue_lenght= None, sleep_time = .0001):
        """ A simpler way to construct the class.

        """

        port = 5567  # TODO: THIS NEEDS TO BE FIXED.
        return cls(name, NanoSocketMixin.create_socket(port, pub_sub = 'pub'))

    def __call__(self):

        if not queue_length:
            return self.__value

        # TODO: ADD THE QUEUE FUNCTIONALITY

    def __update_value(self):

        while True:
            self.__value = self._decode_msg(self.__source.recv())  # this is blocking

    def run(self):
        Thread(target=self._update_value).start()


class OutputClassException(Exception):
    pass


class OutputClass(EncodeDecodeMixin):
    """ Publishes values to the source.
    """

    def __init__(self, name : str, source):
        self.__name = name
        self.__source = source

    @classmethod
    def from_source(cls, name : str, port : int ):
        return cls(name, NanoSockets.create_socket(port))

    def __lshift__(self, value):
        """ Publishes the value

        """

        self.__source.send(self._encode_msg(value))

    def __call__(self):
        raise OutputClassException('Cant obtain a value of the output')


class InOutUpdater:

    def input(self, source, queue_length = None):
        """ Implements the input of the updater. If queue_length is different than None,
            implement the queue.
        """

        if not queue_length: #
            input_1 = InputClass( uuid4(), source, queue_length = queue_length )
            input_1.run()

            return input_1

        # implement here the queue version of the input


    def output(self, source):
        return OutputClass(uuid4(), source)

    def transform(self):
        raise NotImplementedError('transform method not implemented')


    def run(self):
        """ Runs the inputs and outputs.
        """

        # Implement separate threads
        pass
