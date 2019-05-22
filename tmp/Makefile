CFLAGS = -I./include -g
LDFLAGS = -L.
LIBS = -lleveldb -lpthread

load_dump_range_example: load_dump_range_example.c
	$(CXX) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)
