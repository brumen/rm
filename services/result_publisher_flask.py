""" Display the PV01 in a flask format, on localhost:5010 port.
"""

import requests
import logging

from flask import Flask, render_template

logger = logging.getLogger(__name__)


app = Flask(__name__, template_folder='templates')
rester_addr = 'http://localhost:5001/mkt/get_market_2'


@app.route('/pv01')
def publish_pv01():
    """ publishes the pv01 table in flask
    """

    try:
        results = requests.get(rester_addr)

    except ConnectionError as ce:
        logger.warning(f'Could not connect to {rester_addr}: {ce}')
        results_final = {}

    except Exception as e:
        logger.warning(f'Weird error: {e}')
        results_final = {}

    finally:  # no exception
        results_final = results.json()  # we got the results, convert from json

    if results_final is None:
        pv01_results = {}
    else:
        pv01_results = results_final.get('PV01', {})

    return render_template( 'pv01_table.html'
                            , title        = 'PV01 table'
                            , pv01_results = pv01_results)


if __name__ == '__main__':
    app.run(port=5010)
