IDABUILDDIRPREFIX:=$(if $(IDABUILDDIRPREFIX),$(IDABUILDDIRPREFIX),.)
ifeq ($(HOST),ivm64)
    CC=ivm64-gcc
    CXX=ivm64-g++
    IVM_AS:=$(or $(IVM_AS), ivm64-as --noopt)
    IVM_EMU:=$(or $(IVM_EMU), ivm64-emu)
    BUILDDIR=$(IDABUILDDIRPREFIX)/run-$(HOST)
    MAKEMINIZIP=Makefile
    IVMLDFLAGS=-Xlinker -mbin
    IVM_FSGEN:=$(if $(IVM_FSGEN),$(IVM_FSGEN),ivm64-fsgen)
    IVMFS=$(BUILDDIR)/ivmfs.c
    IVMFSOBJ=$(BUILDDIR)/ivmfs.o
else
    HOST=
    CC=gcc
    CXX=g++
    IVM_AS=
    IVM_EMU=
    BUILDDIR=$(IDABUILDDIRPREFIX)/run-linux
    MAKEMINIZIP=Makefile
    IVMLDFLAGS=
    IVM_FSGEN=true
    IVMFS=
    IVMFSOBJ=
endif

CDEFFLAGS=-O2
CXXDEFFLAGS=-O2
CFLAGS := $(if $(CFLAGS), $(CFLAGS), $(CDEFFLAGS))
CXXFLAGS := $(if $(CXXFLAGS), $(CXXFLAGS), $(CXXDEFFLAGS))

LIBDIR=$(BUILDDIR)/lib
INCDIR=$(BUILDDIR)/include

THIRDPARTYDIR := $(if $(THIRDPARTYDIR), $(THIRDPARTYDIR), ./thirdparty)
ZLIBDIR=$(THIRDPARTYDIR)/zlib
TINYXML2LIBDIR=$(THIRDPARTYDIR)/tinyxml2
#XML2LIBDIR=$(THIRDPARTYDIR)/libxml2

SIARDDATADIR=data
SIARDEXAMPLE=$(SIARDDATADIR)/simpledb.siard

.PHONY: clean libsiard2sql tests

# directory for includes
INC=-I. -I $(INCDIR)

# zconf.h is in the zlib building directory
INC+=-I $(ZLIBDIR) -I $(ZLIBDIR)/zlib -I $(ZLIBDIR)/contrib/minizip/

# tinyxml2.h is in the tinyxml2 root directory
INC+= -I $(TINYXML2LIBDIR)

# C sources
SRC=main.c

# Headers
HDR=siard2sql.h

#siard2sql: $(LIBDIR)/libminizip.a $(LIBDIR)/libtinyxml2.a libsiard2sql $(SRC)
#	$(CC) $(CFLAGS) -o $(BUILDDIR)/$@ $(SRC) $(INC) -L $(BUILDDIR)/lib/ -lsiard2sql -lminizip -lz -ltinyxml2 -lm -lstdc++
#	cp -ar $(SIARDDATADIR) $(BUILDDIR)/
#	@echo; echo "Run as: (cd $(BUILDDIR); ./$@ $(SIARDEXAMPLE) out.sql)"; echo

siard2sql: $(BUILDDIR)/siard2sql
	@echo; echo "Run as: (cd $(BUILDDIR); ./$@ $(SIARDEXAMPLE) out.sql)"; echo

$(BUILDDIR)/siard2sql: $(LIBDIR)/libminizip.a $(LIBDIR)/libtinyxml2.a libsiard2sql $(BUILDDIR)/ivmfs.o $(SRC) $(HDR)
	$(CC) $(CFLAGS) -o $@ $(SRC) $(BUILDDIR)/ivmfs.o $(INC) -L $(BUILDDIR)/lib/ -lsiard2sql -lminizip -lz -ltinyxml2 -lm -lstdc++
	cp -ar $(SIARDDATADIR) $(BUILDDIR)/

libsiard2sql: $(LIBDIR)/libsiard2sql.a

$(LIBDIR)/libminizip.a: $(LIBDIR)/libz.a  $(ZLIBDIR)/contrib/minizip/ida_miniunz.c $(ZLIBDIR)/contrib/minizip/ida_miniunz_utils.cpp
	+cd $(ZLIBDIR)/contrib/minizip; make clean; CXXFLAGS="$(CXXFLAGS)" CFLAGS="$(CFLAGS) -Dmain=_IDA_miniunz_main_" CC=$(CC) CXX=$(CXX) make -f $(MAKEMINIZIP) libminizip.a
	cp $(ZLIBDIR)/contrib/minizip/libminizip.a $(LIBDIR)

$(LIBDIR)/libz.a:
	mkdir -p $(BUILDDIR) || exit -1
	mkdir -p $(LIBDIR) || exit -1
	mkdir -p $(INCDIR) || exit -1
	+cd $(ZLIBDIR); rm -rf build; mkdir -p build; cd build; CXXFLAGS="$(CXXFLAGS)" CFLAGS="$(CFLAGS)" CC=$(CC) CXX=$(CXX) ../configure --static; CFLAGS="$(CFLAGS)" CC=$(CC) make
	cp $(ZLIBDIR)/build/libz.a $(ZLIBDIR)/libz.a
	cp $(ZLIBDIR)/build/libz.a $(LIBDIR)

$(LIBDIR)/libsiard2sql.a:  $(BUILDDIR)/libsiardxml.o $(BUILDDIR)/libsiardunzip.o
	mkdir -p $(BUILDDIR) || exit -1
	mkdir -p $(LIBDIR) || exit -1
	mkdir -p $(INCDIR) || exit -1
	ar r $@ $^

$(BUILDDIR)/libsiardunzip.o: libsiardunzip.c $(HDR)
	mkdir -p $(BUILDDIR) || exit -1
	$(CC) $(CFLAGS) $(INC) -c libsiardunzip.c -o $(BUILDDIR)/libsiardunzip.o

$(BUILDDIR)/libsiardxml.o: libsiardxml.cpp $(HDR)
	mkdir -p $(BUILDDIR) || exit -1
	$(CXX) $(CXXFLAGS) $(INC) -c libsiardxml.cpp -o $(BUILDDIR)/libsiardxml.o

$(LIBDIR)/libtinyxml2.a:
	mkdir -p $(BUILDDIR) || exit -1
	mkdir -p $(LIBDIR) || exit -1
	mkdir -p $(INCDIR) || exit -1
	$(CXX) $(CXXFLAGS) $(INC) -c $(TINYXML2LIBDIR)/tinyxml2.cpp -o $(BUILDDIR)/tinyxml2.o
	ar r $@ $(BUILDDIR)/tinyxml2.o

$(BUILDDIR)/ivmfs.o: $(BUILDDIR)/ivmfs.c
	mkdir -p $(BUILDDIR) || exit -1
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILDDIR)/ivmfs.c:
	mkdir -p $(BUILDDIR) || exit -1
	$(IVM_FSGEN) /tmp `find $(SIARDDATADIR)` > $(BUILDDIR)/ivmfs.c

tests: $(BUILDDIR)/test1 $(BUILDDIR)/test2 $(BUILDDIR)/test3
	@echo; echo; echo "Run tests as: (cd $(BUILDDIR); ./test<N> arg1 arg2 ...)"

$(BUILDDIR)/test%:  $(BUILDDIR)/ivmfs.o  $(BUILDDIR)/siard2sql tests/test%.cpp $(HDR)
	$(CXX) $(CXXFLAGS) -o $@ $(BUILDDIR)/ivmfs.o tests/$(notdir $@).cpp $(INC) -L $(BUILDDIR)/lib/ -lsiard2sql -lminizip -lz -ltinyxml2 -lm

clean: cleanbuild clean3rparty

clean3rparty:
	-cd  $(ZLIBDIR)/contrib/minizip/ && make clean
	rm -rf $(ZLIBDIR)/build $(ZLIBDIR)/lib*.a $(ZLIBDIR)/contrib/minizip/lib*.a

cleanbuild:
	rm -rf $(BUILDDIR)
