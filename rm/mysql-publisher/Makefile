all:
	gcc -fPIC -shared mysql-publisher.c -lnanomsg -o mysql-publisher.so
#	gcc -shared mysql-publisher.o  -o mysql-publisher.so

# MYSQL commands:
#  drop function zmq_client;
#  create function zmq_client returns string soname 'libzeromq.so';
# nanomsg: tcp://127.0.0.1:5555"
