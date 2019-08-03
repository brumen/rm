my_bool zmq_client_init(UDF_INIT *initid
                        , UDF_ARGS *args
                        , char *message);

long long zmq_client( UDF_INIT *initid
                , UDF_ARGS *args
                , char *result
                , unsigned long *length
                , char *is_null
                , char *error );

long long nanomsg_send(char *message, char *url);

void zmq_client_deinit(UDF_INIT *initid);
