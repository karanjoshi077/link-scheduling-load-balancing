CXX      = g++
CXXFLAGS = -std=c++17 -O2 -pthread -I./include -Wall -Wextra

# lb is the load-balancer deliverable. server/client are the UNMODIFIED file-server sources
# (copied into partA/), built here so the experiment scripts are self-contained.
all: lb server client

lb: lb.cpp
	$(CXX) $(CXXFLAGS) -o lb lb.cpp

server: partA/server.cpp
	$(CXX) $(CXXFLAGS) -o server partA/server.cpp

client: partA/client.cpp
	$(CXX) $(CXXFLAGS) -o client partA/client.cpp

clean:
	rm -f lb server client health_log.csv lb_dispatch.csv

.PHONY: all clean
