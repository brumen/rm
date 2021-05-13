import logging
import curses
from threading import Thread
from json      import loads

from kafka import KafkaConsumer

logging.basicConfig(filename = '/tmp/rm_publish.log', level = logging.INFO)
logger = logging.getLogger(__name__)

# Kafka server definition
KAFKA_SERVER = 'localhost'
KAFKA_PORT   = 9092
KAFKA_TOPIC  = 'ao_results'  # one of the topics created on the kafka server

subscriber = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=f'{KAFKA_SERVER}:{KAFKA_PORT}')


curr_value = 0.
new_value  = 0.
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
            curr_value = value
        elif field == 'new_market':
            new_value = value
        elif field == 'curr_trades':
            trades_curr_working = value
        elif field == 'new_trades':
            trades_new_working = value


def display_results(w):
    global curr_value
    global new_value
    global trades_curr_working
    global trades_new_working

    while True:
        w.addstr(0, 0, f'Curr value        : {curr_value}' + ' '*20)
        w.addstr(1, 0, f'New value         : {new_value}' + ' ' * 20)
        w.addstr(2, 0, f'Trade curr working: {trades_curr_working}' + ' '*20)
        w.addstr(3, 0, f'Trade new  working: {trades_new_working}'  + ' '*20)
        w.refresh()
        curses.napms(100)


def wrap_curses():
    curses.wrapper(display_results)

thread_updating = Thread(target=get_results_ao)
thread_updating.start()

# display results
thread_display = Thread(target=wrap_curses)
thread_display.start()
