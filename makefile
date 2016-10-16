CC=g++
CFLAGS= -std=c++11 -w
objects = easy_server.o  worker_thread.o main.o
all:$(objects) nedmalloc.o
	g++  -g -o test_easy_server -std=c++11 $(objects) nedmalloc.o -pthread -levent
$(objects): %.o: %.cpp 
	$(CC) -c -g $(CFLAGS) $< -o $@
nedmalloc.o:
	g++ -c -g -std=c++11 -w nedmalloc.c -o nedmalloc.o
dataencap.o:
	g++ -c -g -std=c++11 -w dataencap.c -o dataencap.o
clean:
	rm -f *.o

