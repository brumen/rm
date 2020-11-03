import datetime
import unittest

from rm.controller2 import Controller
from rm.sockets.socket_msg import NanoSocketMixin
from rm.old.portfolio_air_worker import start_workers


class ControllerTest(unittest.TestCase):

    def test_controller(self):
        # 3 workers configuration
        ports = list(range(5700, 5700 + 10))  # [5667, 5668, 5669, 5670, 5671, 5672]
        workers = start_workers(datetime.date(2019, 9, 1), ports)  # on separate threads

        # Nano controller w/ query
        nano_controller = Controller(NanoSocketMixin.create_socket(port=5556)
                                     , worker_sockets=[NanoSocketMixin.create_socket(port=port, pub_sub='pair,send')
                                                       for port in ports]
                                     , query_socket=NanoSocketMixin.create_socket(port=5720, pub_sub='pub'))
        nano_controller.start()

        delta_listener = DeltaListener.from_host()
        delta_listener.start()

        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
