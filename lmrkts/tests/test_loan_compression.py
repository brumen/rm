# testing framework for air options

from unittest import TestCase

from loan_compression import LoanBook


class TestAirOptionFlights(TestCase):

    def test_net(self):
        """ Tests whether the net before and after the compression is the same by party.

        :return:
        """

        lb = LoanBook.load_loans_from_file()
        lb_new = LoanBook(lb.reduce_gross())

        self.assertDictEqual(lb.net_by_party, lb_new.net_by_party)

    def test_large_example(self):
        """ Implements the large example given.

        """

        lb = LoanBook.load_loans_from_file()

        print('Net: ', lb.net_by_party)
        print('Gross:', lb.gross_by_party)
        print('Total gross:', lb.total_gross())
        reduced_gross_book = lb.reduce_gross()
        print(reduced_gross_book)
        new_loan_book = LoanBook(reduced_gross_book)
        print('New total gross:', new_loan_book.total_gross())
        print('New total net:', new_loan_book.net_by_party)
        print('New gross party:', new_loan_book.gross_by_party)

    def test_small_example(self):
        """ Implements small example.

        """

        lb = LoanBook.load_loans_from_file('loans_simple.csv')

        print(lb.net_by_party)
        print(lb.gross_by_party)
        print(lb.total_gross())
        print(lb.reduce_gross())
        lb.write_reduce_gross()


tao = TestAirOptionFlights()
# tao.test_small_example()
tao.test_large_example()