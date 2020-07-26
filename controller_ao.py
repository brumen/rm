# specialization of the controller to the airoptions example

from rm.controller2 import Controller


class ControllerAO(Controller):

    def __init__( self,
                , position_socket
                , position_db_address ='127.0.0.1'
                , mkt_date            = None
                , queue_size          = 100000
                , revalue_portfolio   = PortfolioAirWorker.revalue_portfolio
                , worker_sockets      = None
                , query_socket        = None
                , ):
        """ Controller class, keeps track of the system and distributes work.

        :param position_socket: socket over which new positions are obtained.
        :param position_db_address: database host where the position are read from
        :param mkt_date: market date (datetime.date), if None, revert to today
        :param queue_size: maximum size of the queue.
        :param revalue_portfolio: function computing the portfolio given.
        :param worker_sockets: sockets to the workers to distribute work.
                               {'worker_name': worker_socket}
        :param query_socket: sockets where one can subscribe to and query for results.
        """

        super().__init__( position_socket
                        , mkt_date            = mkt_date
                        , queue_size          = queue_size
                        , revalue_portfolio   = revalue_portfolio
                        , worker_sockets      = worker_sockets
                        , query_socket        = query_socket )

        self._position_db_address = position_db_address

    def _get_trade_params(self, position_id : int) -> List[Tuple]:
        """ Get trade params for trade under position_id in the self.__position_db_address mysql db.

        :param position_id: position id of the trade considered.
        :returns: list of tuples for position_id
        """

        with MysqlConnectorEnv(host=self._position_db_address) as db_conn:
            cursor = db_conn.cursor()
            # TODO: THIS CAN BE OPTIMIZED TO ACCEPT position_id lists
            cursor.execute('SELECT * FROM option_positions WHERE position_id = {0}'.format(position_id))
            return cursor.fetchall()

    def __report_results(self, sleep_time=.5):
        """ Reporting thread.
        """

        # TODO: THIS HAS TO BE IMPROVED.

        while True:
            logger.info('Current delta: {0}'.format(str(self.curr_delta)))
            if self.__query_socket:  # publish if provided.
                self.__query_socket.send(self._encode_msg(self.curr_delta))
            time.sleep(sleep_time)
