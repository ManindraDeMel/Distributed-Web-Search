# This is the Makefile used to build the db_server executable. You may modify it 
# to better suit the structure of your code, but ensure that regardless of the
# changes you make it still produces a single `db_server` executable.

CC=gcc

CFLAGS=-Wall -g

all: db_server

csapp.o: src/csapp/csapp.c
	"$(CC)" $(CFLAGS) -c -w $^

%.o : src/%.c 
	"$(CC)"	$(CFLAGS) -c $^

db_server : node.o utils.o csapp.o
	"$(CC)" $(CFLAGS) -o $@ $^ -lpthread -lrt

.PHONY: clean

clean: 
	rm -rf *.o *.exe db_server output/
