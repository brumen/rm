#
# Delta dictionary


class DeltaDict(dict):

    def __neg__(self, *args, **kwargs):
        """ Reverses the sign of every delta.

        :param delta: delta dictionary, {'UA71': 1.,...}
        :returns: resulting delta dictionary {'UA71': -1,...}
        """

        return DeltaDict({ delta_flight_nb: - delta_flight_value
                           for delta_flight_nb, delta_flight_value in self.items() })

    def __add__(self, other):
        """ Merge the two delta dicts.

        :param other: other delta dictionary, {'UA71': 1.,...}
        :param delta_2: delta dictionary, {'UA71': 2, 'UA72': 1.,...}
        :returns: resulting delta dictionary {'UA71': 3, 'UA72': 1.,...}
        """

        result_delta = DeltaDict({})

        for delta_1_flight_nb, delta_1_flight_value in self.items():
            if delta_1_flight_nb in other.keys():
                result_delta[delta_1_flight_nb] = delta_1_flight_value + other[delta_1_flight_nb]
            else:
                result_delta[delta_1_flight_nb] = delta_1_flight_value

        for delta_2_flight_nb in set(other.keys()).difference(set(self.keys())):
            result_delta[delta_2_flight_nb] = other[delta_2_flight_nb]

        return result_delta

    def __sub__(self, other):
        return self.__add__(other.__neg__())
