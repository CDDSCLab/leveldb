CFLAGS = -I./include -g
LDFLAGS = -L.
LIBS = -lleveldb -lpthread

all: load_dump_range_example ingest_ranges_example

%.o: %.cc
	$(CXX) $(CFLAGS) -c -o $@ $<

load_dump_range_example: load_dump_range_example.o
	$(CXX) -o $@ $^ $(LDFLAGS) $(LIBS)

ingest_ranges_example: ingest_ranges_example.o
	$(CXX) -o $@ $^ $(LDFLAGS) $(LIBS)
