# class implements the in-out updater

from time      import sleep
from uuid      import uuid4
from threading import Thread
from typing    import Callable

from rm.socket_msg    import NanoSocketMixin
from rm.encode_decode import EncodeDecodeMixin


class InOutUpdaterException(Exception):
    pass


class InputClass(EncodeDecodeMixin):

    def __init__(self, name, value, queue_length = None, sleep_time = .0001 ):
        """ Initiates the input class.

        :param name: name of the class, this is usually internally set.
        :param source: source where this is listening to.
        :param queue_length: length of the queue that is kept.
        :param sleep_time: amount of sleep the process does between looking for new values.
        """

        self.__name         = name
        self.__queue_length = queue_length
        self.__sleep_time   = sleep_time
        self.__value = value

        # internal variables
        self.__value_has_changed = True

    @property
    def has_changed(self):
        return self.__value_has_changed

    @has_changed.setter
    def has_changed(self, new_value):
        self.__value_has_changed = new_value

    @classmethod
    def from_source(cls, port = 5567, queue_lenght= None, sleep_time = .0001):
        """ A simpler way to construct the class.

        """

        # TODO: port should be internally determined.

        return cls( uuid4()
                  , NanoSocketMixin.create_socket(port, pub_sub = 'pub'))

    def __call__(self):

        if not self.__queue_length:
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

    def __init__(self, name : str, callback : Callable, value = None ):
        """

        :param name: name of the output class.
        :param source: source where the output class is outputing.
        :param callback: function to execute when the input value changes.
        """

        self._name     = name
        self._callback = callback
        self._value    = value

        # internal states
        self.__value_has_changed = False   # indicator if the value is changed

    @property
    def has_changed(self):
        return self.__value_has_changed

    @has_changed.setter
    def has_changed(self, new_value):
        self.__value_has_changed = new_value

    @classmethod
    def from_source(cls, name : str, port : int = 5567):
        return cls(name, NanoSocketMixin.create_socket(port))

    def __lshift__(self, value):
        """ Executes the transform function and publishes the value

        :param value: value to assign to the output class.
        """

        self._value = value
        self.has_changed = True


    def __call__(self):

        raise OutputClassException('Cant obtain a value of the output')

    def __or__(self, input_class):
        """ Chaining of the output to the input with | symbol (or class method).
        """

        # TODO: HERE IMPLEMENT
        pass


class InOutUpdater:

    INPUTS  = []  # observables
    OUTPUTS = []  # observers

    def input(self, source, queue_length = None):
        """ Implements the input of the updater. If queue_length is different than None,
            implement the queue.
        """

        if not queue_length:  # class without queue.

            new_input = InputClass( uuid4(), source, queue_length = queue_length )
            self.__class__.INPUTS.append(new_input)
            new_input.run()

            return new_input

        # implement here the queue version of the input


    def output(self, source):
        """

        """
        new_output = OutputClass(uuid4(), source, self.transform)
        self.__class__.OUTPUTS.append(new_output)

        return new_output

    def transform(self):
        raise NotImplementedError('transform method not implemented')


    def _run_thread(self, sleep_time = .0001):
        """ Computes the outputs every time inputs change.
        """

        while True:
            observers_changed = [observer
                                 for observer in self.__class__.INPUTS
                                 if observer.has_changed ]
            if observers_changed:  # this list is not empty
                self.transform()
                for observer in observers_changed:
                    observer.has_changed = False
            else:
                sleep(sleep_time)

    def __call__(self, sleep_time = .0001):
        """ When we call the class, it starts running.
        """

        Thread(target = self._run_thread).start()


class DoSomething(InOutUpdater):
    """ Example of InOutUpdater class.
    """

    def __init__(self):
        self.input_1 = self.input('input_1', 's1')
        self.input_2 = self.input('input_2', 's2')
        self.output_1 = self.output('output_1', 's3')

    def transform(self):
        self.output_1 << self.input_1() + self.input_2()
