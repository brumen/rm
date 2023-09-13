import datetime

from typing import List
from tkinter import Label, Frame, Tk, IntVar, Entry

import sys
if '/home/brumen/work/' not in sys.path:
    sys.path.append('/home/brumen/work/')

from ao.trade import AOTrade
from ao.flight import Flight
from rm.services.ao.trade_api import extract_trade_ids

# main window starts
market_date = datetime.date(2016, 7, 1)
root = Tk()


def new_trade_added(
        trade_nb: int,
        pv_label: Label,
        flights_frame: Frame,
):
    """ Adds the trade PV to the pv_label and trade flights to the trade frame.

    """

    trades = extract_trade_ids(str(trade_nb))

    if not trades:
        pv = 0  # no flights needed.
    else:
        trade = trades[0]
        pv = trade.PV(market_date)
        create_frame(trade.flights, flights_frame)  # add flights to the frame

    pv_label.config(text=str(pv))


def create_frame(flights: List[Flight], frame: Frame):
    """ Adds the

    :param flights: list of flights whose information is displayed in the frame
    :param frame: frame where the information is displayed.
    """

    for id_col, id_elt in enumerate((
            'ID',
            'ORIGIN',
            'DEST',
            'DEP',
            'ARR',
            'CARRIER',
            'PRICE',
    )):
        Label(frame, text=id_elt).grid(row=0, column=id_col)

    for flight_nb, flight in enumerate(flights):
        # add a few things to the frame
        flight_id = flight.flight_id
        orig      = flight.orig
        dest      = flight.dest
        dep_date  = flight.dep_date.date()
        arr_date  = flight.arr_date.date()
        carrier   = flight.carrier
        # prices (TODO: just takes the first price for now)
        prices = flight.prices
        if prices:  # if there is a
            price = prices[0].price
        else:
            price = None

        # pack all the elements
        for elt_idx, element in enumerate((flight_id, orig, dest, dep_date, arr_date, carrier, price)):
            elt_label = Label(frame, text=element)
            elt_label.grid(row=flight_nb + 1, column=elt_idx)


current_grid_nb = 1


def add_trade():
    # TODO: how to add a trade.
    global current_grid_nb

    # Trade entry number
    entry_nb = IntVar()
    new_trade_entry = Entry(
        root,
        textvariable=entry_nb,
    ).grid(row=current_grid_nb, column=0)

    # list of flights associated w. it.
    flights_frame = Frame(root, borderwidth=5)
    flights_frame.grid(row=current_grid_nb, column=3)

    new_trade_pv = Label(root, text='TO FILL', width=50)
    new_trade_pv.grid(row=current_grid_nb, column=1)

    new_trade_button = Button(
        root,
        text='Compute',
        command=lambda: new_trade_added(
            entry_nb.get(),
            new_trade_pv,
            flights_frame
        )
    )\
        .grid(row=current_grid_nb, column=2)

    current_grid_nb += 1


add_trade_button = Button(
    root,
    text='Add trade',
    command=lambda: add_trade()
).grid(row=0, column=0)


# main loop
root.mainloop()
