# class implements the in-out updater

from time      import sleep
from uuid      import uuid4
from threading import Thread

from rm.input_classes  import InputFactory
from rm.output_classes import OutputFactory


class InOutUpdater:

    INPUTS  = []  # observables
    OUTPUTS = []  # observers

    def input(self, input_type, source, queue_length = None):
        """ Implements the input of the updater. If queue_length is different than None,
            implement the queue.
        """

        if not queue_length:  # class without queue.

            new_input = InputFactory.input( input_type, uuid4(), queue_length = queue_length )
            self.__class__.INPUTS.append(new_input)
            new_input.run()

            return new_input

        # implement here the queue version of the input

    def output(self, output_type, *args, **kwargs):
        """

        """
        new_output = OutputFactory.output(output_type, self.transform, *args, **kwargs)
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
        self.input_1 = self.input('RandomInput', 'in_1')
        self.input_2 = self.input('RandomInput2', 'in_2')
        self.output_1 = self.output('BaseOutput')

    def transform(self):
        self.output_1 << self.input_1() + self.input_2()**2


if __name__ == '__main__':
    g = DoSomething()
    g()
