CXX=g++
CXXFLAGS=-Wall -g
LDFLAGS=

PRODUCT=fdtreetest
SOURCES = $(wildcard *.cpp)
HEADERS = $(wildcard *.h)
OBJECTS = $(patsubst %.cpp,%.o,$(SOURCES))

all: 	$(PRODUCT)

%.o: 	%.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) -o $@ -c $<

$(PRODUCT): 	$(OBJECTS)
		$(CXX) $(OBJECTS) $(LDFLAGS) -o $@

clean: execclean objclean

execclean: 
	-rm -f $(PRODUCT)

objclean:
	-rm -f *.o .depend
