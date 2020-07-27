# tests for worker classes

import datetime
import json

from unittest import TestCase

from rm.socket_msg     import NanoSocketMixin
from rm.in_out_updater import InputClass, OutputClass


class InOutClassTest(TestCase):
    """ Tests for the AirOptions worker.
    """

    def test_in_out_1(self):
        o1 = OutputClass.from_source('out')
        i1 = InputClass.from_source('in')

        o1 << i1()

    # def test_worker1(self):

    #     sender = NanoSocketMixin._create_socket(5667, pub_sub='pair,recv')  # IMPORTANT: !!! this has to come first.

    #     worker = PortfolioAirWorker( NanoSocketMixin._create_socket(5667, pub_sub='pair,send')
    #                                , datetime.date(2019, 9, 2)
    #                                , 'Worker1' )
    #     worker_thread = threading.Thread(target=worker.start)
    #     worker_thread.start()

    #     msg = [( 'XXX'
    #           , 'EWR'
    #           , 'SFO'
    #           , datetime.date(2019, 9, 15).__str__()
    #           , datetime.date(2019, 9, 30).__str__()
    #           , None
    #           , None
    #           , datetime.date(2019, 10, 1).__str__()
    #           , datetime.date(2019, 10, 15).__str__()
    #          , None
    #          , None
    #          , 100.
    #          , 'UA'
    #          , 1
    #          , 'Economy' )]

    #     msg2 = msg + msg

    #     sender.send(json.dumps(msg))
    #     feedback = sender.recv()
    #     print(feedback)

    #     sender.send(json.dumps(msg2))
    #     feedback = sender.recv()
    #     print(feedback)

    #     self.assertIsNotNone(feedback)  # kind of a simple test, but still.


pwt = InOutClassTest().test_in_out_1()
