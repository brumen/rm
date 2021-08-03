import unittest

from rm.delta_dict import DeltaDict


class DeltaDictTest(unittest.TestCase):

    d_1 = DeltaDict({'a': 1, 'b': 2})
    d_2 = DeltaDict({'a': 3, 'c': 4})

    def test_delta_dict(self):

        self.assertDictEqual(self.d_1 + self.d_2, DeltaDict({'a': 4, 'b': 2, 'c': 4}))  # TODO: CHECK IF REALLY DictEqual, maybe just Equal.
        self.assertDictEqual(-self.d_1, DeltaDict({'a': -1, 'b': -2}))


if __name__ == '__main__':
    unittest.main()
