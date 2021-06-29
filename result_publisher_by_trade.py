import logging
import tkinter as tk
import numpy   as np
import pandas  as pd

from threading   import Thread
from json        import loads
from kafka       import KafkaConsumer
from pandastable import Table

logging.basicConfig(filename = '/tmp/rm_results_by_trade.log', level = logging.INFO)
logger = logging.getLogger(__name__)

# Kafka server definition
KAFKA_SERVER = 'localhost'
KAFKA_PORT   = 9092
KAFKA_TOPIC  = 'ao_results_by_trade'  # one of the topics created on the kafka server


subscriber = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=f'{KAFKA_SERVER}:{KAFKA_PORT}')


curr_value = {}
new_value  = {}
trades_curr_working = 0
trades_new_working  = 0


def get_results_ao():
    global curr_value
    global new_value
    global trades_curr_working
    global trades_new_working

    for msg in subscriber:
        field, value = loads(msg.value)  # value is json encoded
        if field == 'curr_market':
            if value is None:
                curr_value = np.array([])
            else:
                curr_value = np.array(list(value.items()))
        elif field == 'new_market':
            if value is None:
                new_value = np.array([])
            else:
                new_value = np.array(list(value.items()))
        elif field == 'curr_trades':
            trades_curr_working = value
        elif field == 'new_trades':
            trades_new_working = value

# thread for reading results
thread_updating = Thread(target=get_results_ao)
thread_updating.start()


# displaying results
root = tk.Tk()
frame = tk.Frame(root)
frame.pack()
results_table = Table(frame, showtoolbar=True, showstatusbar=True)
results_table.show()


def update_results():
    if isinstance(curr_value, np.ndarray):
        results_table.model.df = pd.DataFrame(curr_value)
        results_table.redraw()
    root.after(1, update_results)


root.after(1, update_results)
root.mainloop()  # looping the tk canvas
