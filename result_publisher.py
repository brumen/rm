# publishes the results as a rester API, uses Flask

import logging

from flask      import Flask, request, jsonify
from flask_cors import CORS
from kafka import KafkaConsumer

logging.basicConfig(filename = '/tmp/rm_publish.log', level = logging.INFO)

logger = logging.getLogger()

# rester definition
rm_rester = Flask(__name__)
CORS(rm_rester)
rm_rester.debug = True
rm_rester.use_debugger = False

# Kafka server definition
KAFKA_SERVER = 'localhost'
KAFKA_PORT   = 9092
KAFKA_TOPIC  = 'ao_results'  # one of the topics created on the kafka server

subscriber = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers='{0}:{1}'.format(KAFKA_SERVER, KAFKA_PORT))


@rm_rester.route('/rm_results', methods=['GET'])
def get_results():
    """ Returns the results of portfolio computation located on the kafka server.

    :returns: results the json version of the portfolio encapsulated object
    """

    for msg in subscriber:
        return jsonify({'valid': True, 'msg': msg})  # returns the first message


# run the rester service.
rm_rester.run()

# access in a browser through: localhost:5000/rm_results?topic=ao_results (topic not necessary)
