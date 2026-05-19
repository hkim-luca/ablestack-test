ifndef ACE_ROOT
$(error ACE_ROOT is not set. Please set ACE_ROOT before building.)
endif

CXX      = g++
CXXFLAGS = -O2 -Wall -Wextra -std=c++17
INCLUDES = -I$(ACE_ROOT)
LDFLAGS  = -L$(ACE_ROOT)/lib
LDLIBS   = -lACE -lpthread -lrt

all: producer consumer

debug: CXXFLAGS = -g -O0 -Wall -Wextra -std=c++17
debug: producer consumer

producer: producer.cpp common.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ producer.cpp $(LDFLAGS) $(LDLIBS)

consumer: consumer.cpp common.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ consumer.cpp $(LDFLAGS) $(LDLIBS)

clean:
	rm -f producer consumer *.o *.log core

.PHONY: all clean
