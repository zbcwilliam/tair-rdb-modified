noinst_LIBRARIES=libmockdb.a 

LDADD=$(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a 


AM_CPPFLAGS=-I$(TBLIB_ROOT)/include/tbsys -I$(TBLIB_ROOT)/include/tbnet -I${top_srcdir}/src -I${top_srcdir}/src/common -I. -DTAIR_STORAGE -Wall
AM_LDFLAGS=-lpthread    ${GCOV_LIB} -Wall


libmockdb_a_SOURCES=mockdb_manager.h mockdb_manager.cpp


