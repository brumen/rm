/*
  MySQL publisher

  Author: Gorazd Brumen (gorazd.brumen@gmail.com)
*/

#include <stdlib.h>
#include <stdio.h>
#include <mysql/mysql.h>
#include <string.h>
#include <unistd.h>
#include <error.h>

#include <nanomsg/nn.h>
#include <nanomsg/pubsub.h>

#include "mysql-publisher.h"

void fatal(const char *func) {
  fprintf(stderr, "%s: %s\n", func, nn_strerror(nn_errno()));
  exit(1);
}

/* client publisher */

my_bool zmq_client_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {

  if (args->arg_count != 2) {
    strncpy(message, "two arguments must be supplied: zmq_client('socket', 'message').", MYSQL_ERRMSG_SIZE);
    return 1;
  }

  args->arg_type[0]= STRING_RESULT;
  return 0;
}

long long zmq_client(UDF_INIT *initid, UDF_ARGS *args,
                 __attribute__ ((unused)) char *result,
                 unsigned long *length,
                 __attribute__ ((unused)) char *is_null,
                 __attribute__ ((unused)) char *error) {

/* url = args->args[0];
   message = args->args[1];
*/
  return nanomsg_send((char*) (args->args[1]), (char*) (args->args[0]));

}


long long nanomsg_send(char *message, char *url) {
  /* sends the message over nanomsg interface */

  int socket, eid; /* socket, endpoint id */

  if ((socket = nn_socket(AF_SP, NN_PUB)) < 0)
    return -1;

  if ((eid = nn_bind(socket, url)) < 0)
    return -2;

  /* Sleeping here is necessary, otherwise the connection is too fast - 1 is the minim, as it has to be an integer */
  sleep(1);

  int bytes = nn_send(socket, message, strlen(message), 0);  /* before was strlen + 1 */
  if (bytes < 0)
    return -3;

  nn_shutdown(socket, eid);

  return (long long) 0;
}


void zmq_client_deinit(UDF_INIT *initid) {
  return ;
}

