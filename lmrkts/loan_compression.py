# Loan book manipulations.
import numpy as np
import csv

from scipy.optimize import linprog
from typing         import List, Tuple


class LoanBookException(Exception):
    pass


class LoanBook:
    """ Class for handling all of loan metrics.

    """

    def __init__( self
                , loan_book : List[Tuple[str, str, float]]):

        self.loan_book = loan_book  # (lender, borrower, amount)

        # cached values for optimizations,
        self.__parties = None
        self.__cv = None  # constraints vector
        self.__cm = None  # constraints matrix
        self.__net_by_party = None
        self.__gross_by_party = None

    @property
    def parties(self) -> List[str]:
        """ Returns all the parties in the list of loan relationships.

        :returns: list of parties in the loan ledger.
        """

        if self.__parties:
            return self.__parties

        # go through the book
        self.__parties = set()
        for lender, borrower, _ in self.loan_book:
            self.__parties.add(lender)

        self.__parties = list(self.__parties)

        return self.__parties

    @staticmethod
    def _load_loans(loan_filename='loans.csv'):
        """ Loads the data from the file in the form:
            lender,borrower,loan_amount.
            The first line IS Lender,Borrower,Amount

        :param loan_filename: name of loan file.
        :returns: generator producing  tuples (str, str, float)]
        """

        with open(loan_filename, 'r') as loan_file:
            next(loan_file)  # skip the first line
            for lender, borrower, loan_amount in csv.reader(loan_file):
                yield (lender, borrower, float(loan_amount))

    @classmethod
    def load_loans_from_file(cls, loan_filename='loans.csv') :
        """ Loads the data from the file

        :param loan_filename: name of file where to load the loans from
        :returns: generator of list of (lender, borrower, loan_amount)
        """

        return cls(list(cls._load_loans(loan_filename)))

    def __net_gross_by_party(self):
        """ Net/gross amount by party.

        :returns: dictionary w/ keys as lenders/borrowers,
        """

        net_amount_dict = {}
        for lender, borrower, loan_amount in self.loan_book:
            if lender in net_amount_dict:
                if 'owed' in net_amount_dict[lender]:
                    net_amount_dict[lender]['owed'] += loan_amount
                else:
                    net_amount_dict[lender]['owed'] = loan_amount
            else:
                net_amount_dict[lender] = {'owed': loan_amount, 'owns': 0.}

            if borrower in net_amount_dict:
                if 'owns' in net_amount_dict[borrower]:
                    net_amount_dict[borrower]['owns'] += loan_amount
                else:
                    net_amount_dict[borrower] = {'owns': loan_amount, 'owed': 0.}
            else:
                net_amount_dict[borrower] = {'owns': loan_amount, 'owed': 0.}

        return net_amount_dict

    @property
    def net_by_party(self):

        if self.__net_by_party:
            return self.__net_by_party

        self.__net_by_party = { borrower: borrow_structure['owed'] - borrow_structure['owns']
                                for borrower, borrow_structure in self.__net_gross_by_party().items() }

        return self.__net_by_party

    @property
    def gross_by_party(self):
        """ Gross amount by party.

        """

        return { borrower: borrow_structure['owed'] + borrow_structure['owns']
                 for borrower, borrow_structure in self.__net_gross_by_party().items() }

    def total_gross(self):
        """ Returns the total gross amount of loans in the loan book.

        """

        return sum(self.gross_by_party.values())

    # REDUCE GROSS SECTION
    def __X(self, i : int, j : int) -> int:
        """ Position of X(i,j) in the matrix.

        """

        assert i != j, '__X(i={0}, j={1}) need to be different'.format(i, j)

        return i * (len(self.parties)-1) + (j-1 if j > i else j)

    def __Xinv(self, x : int) -> Tuple[int, int]:
        """  Inverse of __X

        :param x: number to invert into the tuple
        :returns: tuple of i, j parties corresponding to the x-idx element in the variable array.
        """

        i = x//(len(self.parties)-1)
        j_corr = x - i * (len(self.parties) - 1)
        # j-1 if j>i else j = j_corr
        # j_corr <= i    j = j_corr
        # j_corr > i     j = j_corr + 1

        return i, j_corr + 1 if j_corr >= i else j_corr

    def __optimizing_vector(self) -> np.array:
        """ Optimizing vector, in our case just a vector of ones. Size = N * (N-1) where N is the number of parties.

        """

        N = len(self.parties)

        return np.ones(N * (N-1))  # vector of ones - reduce the sum of all loans

    def __constraints_matrix(self) -> np.array:
        """ Implements the constraints A_eq * x = b_eq.

        :returns: constraints matrix of shape (N, N(N-1))
        """

        if self.__cm is not None:
            return self.__cm

        N = len(self.parties)
        self.__cm = np.zeros((N, N*(N-1)))

        for i in range(N):
            for j in range(N):
                if j == i:
                    continue
                self.__cm[i, self.__X(i, j)] = 1.
                self.__cm[i, self.__X(j, i)] = -1.

        return self.__cm  # cm ... mnemonic for constraints matrix

    def __constraints_vector(self) -> np.array:
        """ Vector corresponding to __constraints_matrix.

        :return:
        """

        if self.__cv is not None:
            return self.__cv

        self.__cv = np.zeros(len(self.parties))

        for party in self.parties:
            self.__cv[self.parties.index(party)] = self.net_by_party[party]

        return self.__cv

    def __upper_bound_matrix_vector(self) -> Tuple[np.array, np.array]:
        """ Constructs the upper bound matrix & vector, i.e. the restriction: x_{i,j} <= original_loan{i,j}

        :returns: tuple for upper bound
        """

        N = len(self.parties)
        ubm = np.zeros((len(self.loan_book), N * (N-1)))
        ubv = np.empty(len(self.loan_book))

        for idx_nb, (lender, borrower, loan_amount) in enumerate(self.loan_book):
            ubm[idx_nb, self.__X(self.parties.index(lender), self.parties.index(borrower))] = 1.
            ubv[idx_nb] = loan_amount

        return (ubm, ubv)

    def __reduce_gross_lp(self) -> np.array:
        """ Reduces the gross notional. Some description how this is done is in order:
            Let x_{i,j} be the amount owed to i from j. Then we are solving for the following optimization problem:

            minimize \sum x_{i,j}

            \sum_{j} x_{i,j} - \sum_{j} x_{j,i} = net(i)
            x_{i,j} >= 0.
            x_{i,j} <= original_loan(i,j)

        :returns: results of the linear optimization problem.
        """

        ubm, ubv = self.__upper_bound_matrix_vector()

        # x_{i,j} >= 0 by default.
        result = linprog( self.__optimizing_vector()
                        , A_eq = self.__constraints_matrix()
                        , b_eq = self.__constraints_vector()
                        , A_ub = ubm
                        , b_ub = ubv )

        if result.success:
            return result.x

        raise LoanBookException(result.message)

    def reduce_gross(self) -> List[Tuple[str, str, float]]:
        """ Reduces the gross notionals of loans. Uses __reduce_gross_lp, and translates from numbers to counterparties.

        :returns: dictionary of lenders, borrowers and reduced loan amounts.
        """

        gross_result = []
        for x_idx, loan_amount in enumerate(self.__reduce_gross_lp()):
            if loan_amount != 0.:
                i, j = self.__Xinv(x_idx)
                # print(x_idx, i, j)
                gross_result.append( (self.parties[i], self.parties[j], loan_amount) )

        return gross_result

    def write_reduce_gross(self, output_filename='result1.csv'):
        """ Writes the reduced gross results to a file

        :param output_filename: filename where the results are written.
        """

        with open(output_filename, 'w') as output_file:
            csv_writer = csv.writer(output_file)
            csv_writer.writerow(['Lender', 'Borrower', 'Loan Amount'])
            for loan_entry in self.reduce_gross():
                csv_writer.writerow([str(x) for x in loan_entry])
