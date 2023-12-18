ifeq ($(HOST),ivm64)
    CC=ivm64-gcc
    CXX=ivm64-g++
    IVM_AS:=$(or $(IVM_AS), ivm64-as --noopt)
    IVM_EMU:=$(or $(IVM_EMU), ivm64-emu)
    BUILDDIR=./run-$(HOST)
#    MAKEMINIZIP=Makefile-$(HOST)
    MAKEMINIZIP=Makefile
    IVMLDFLAGS=-Xlinker -mbin
    XMLCONFOPT=--without-pic
    IVM_FSGEN:=$(if $(IVM_FSGEN),$(IVM_FSGEN),ivm64-fsgen)
    IVMFS=$(BUILDDIR)/ivmfs.c
else
    HOST=
    CC=gcc
    CXX=g++
    IVM_AS=
    IVM_EMU=
    BUILDDIR=./run-linux
    MAKEMINIZIP=Makefile
    IVMLDFLAGS=
    XMLCONFOPT=
    IVM_FSGEN=true
    IVMFS=
endif

CFLAGS := $(if $(CFLAGS), $(CFLAGS), -O2 -static)
CXXFLAGS := $(if $(CXXFLAGS), $(CXXFLAGS), -O2 -static)

LIBDIR=$(BUILDDIR)/lib
INCDIR=$(BUILDDIR)/include

THIRDPARTYDIR := $(if $(THIRDPARTYDIR), $(THIRDPARTYDIR), ./thirdparty)
ZLIBDIR=$(THIRDPARTYDIR)/zlib
TINYXML2LIBDIR=$(THIRDPARTYDIR)/tinyxml2
#XML2LIBDIR=$(THIRDPARTYDIR)/libxml2

SIARDDATADIR=data
SIARDEXAMPLE=$(SIARDDATADIR)/simpledb.siard

.PHONY: clean siard2sql libsiard2sql

# directory for includes
INC=-I. -I $(INCDIR)

# zconf.h is in the zlib building directory (???)
INC+=-I $(ZLIBDIR) -I $(ZLIBDIR)/zlib -I $(ZLIBDIR)/contrib/minizip/

# xmlversion.h is in the xml2 building directory (???)
#INC+= -I $(XML2LIBDIR)/include -I $(XML2LIBDIR)/include/libxml

# tinyxml2.h is in the tinyxml2 root directory
INC+= -I $(TINYXML2LIBDIR)

# C sources
SRC=$(IVMFS) main.c

siard2sql: $(LIBDIR)/libminizip.a $(LIBDIR)/libtinyxml2.a libsiard2sql $(SRC)
	$(CC) $(CFLAGS) -o $(BUILDDIR)/$@ $(SRC) $(INC) -L $(BUILDDIR)/lib/ -lsiard2sql -lminizip -lz -ltinyxml2 -lm -lstdc++
	cp -ar $(SIARDDATADIR) $(BUILDDIR)/
	@echo; echo; echo "Run as: (cd $(BUILDDIR); ./$@ $(SIARDEXAMPLE) out.sql)"

libsiard2sql: $(LIBDIR)/libsiard2sql.a

$(LIBDIR)/libminizip.a: $(LIBDIR)/libz.a
	cd $(ZLIBDIR)/contrib/minizip; make clean; CFLAGS="$(CFLAGS) -Dmain=_IDA_miniunz_main_" CC=$(CC) make -f $(MAKEMINIZIP) libminizip.a
	cp $(ZLIBDIR)/contrib/minizip/libminizip.a $(LIBDIR)

$(LIBDIR)/libz.a:
	mkdir -p $(BUILDDIR) || exit -1
	mkdir -p $(LIBDIR) || exit -1
	mkdir -p $(INCDIR) || exit -1
	cd $(ZLIBDIR); rm -rf build; mkdir -p build; cd build; CFLAGS="$(CFLAGS)" CC=$(CC) ../configure --static; CFLAGS="$(CFLAGS)" CC=$(CC) make
	cp $(ZLIBDIR)/build/libz.a $(ZLIBDIR)/libz.a
	cp $(ZLIBDIR)/build/libz.a $(LIBDIR)

$(LIBDIR)/libsiard2sql.a:  $(BUILDDIR)/libsiardxml.o $(BUILDDIR)/libsiardunzip.o
	mkdir -p $(BUILDDIR) || exit -1
	mkdir -p $(LIBDIR) || exit -1
	mkdir -p $(INCDIR) || exit -1
	ar r $@ $^

$(BUILDDIR)/libsiardunzip.o: libsiardunzip.c
	mkdir -p $(BUILDDIR) || exit -1
	$(CC) $(CFLAGS) $(INC) -c libsiardunzip.c -o $(BUILDDIR)/libsiardunzip.o

$(BUILDDIR)/libsiardxml.o: libsiardxml.cpp
	mkdir -p $(BUILDDIR) || exit -1
	$(CXX) $(CXXFLAGS) $(INC) -c libsiardxml.cpp -o $(BUILDDIR)/libsiardxml.o

$(LIBDIR)/libtinyxml2.a:
	mkdir -p $(BUILDDIR) || exit -1
	mkdir -p $(LIBDIR) || exit -1
	mkdir -p $(INCDIR) || exit -1
	$(CXX) $(CXXFLAGS) $(INC) -c $(TINYXML2LIBDIR)/tinyxml2.cpp -o $(BUILDDIR)/tinyxml2.o
	ar r $@ $(BUILDDIR)/tinyxml2.o

$(BUILDDIR)/ivmfs.c:
	mkdir -p $(BUILDDIR) || exit -1
	$(IVM_FSGEN) /tmp `find $(SIARDDATADIR)` `find /tmp/zz`  > $(BUILDDIR)/ivmfs.c

clean: cleanbuild clean3rparty

clean3rparty:
	-cd  $(ZLIBDIR)/contrib/minizip/ && make clean
	rm -rf $(ZLIBDIR)/build $(ZLIBDIR)/lib*.a $(ZLIBDIR)/contrib/minizip/lib*.a

cleanbuild:
	rm -rf $(BUILDDIR)
