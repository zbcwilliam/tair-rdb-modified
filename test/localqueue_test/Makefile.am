AM_CPPFLAGS= @BOOST_CPPFLAGS@ -I$(TBLIB_ROOT)/include/tbsys \
			 -I$(TBLIB_ROOT)/include/tbnet \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/packets \
			 -I${top_srcdir}/src/storage/fdb \
			 -I${top_srcdir}/src/storage/mdb \
			 -I${top_srcdir}/src/storage/kdb \
			 -I${top_srcdir}/src/storage \
			 -I${top_srcdir}/src/client \
       -I${top_srcdir}/src  \
       -I/opt/gtest-1.7.0/include \
       -I${top_srcdir}/src/storage/kdb/kyotocabinet

LDADD= \
	  $(top_builddir)/src/client/.libs/libtairclientapi.a \
	  $(top_builddir)/src/storage/mdb/libmdb.a \
	  $(top_builddir)/src/storage/fdb/libfdb.a \
	  $(top_builddir)/src/storage/kdb/libkdb.a \
    ${top_builddir}/src/storage/kdb/kyotocabinet/libkyotocabinet.a \
	  $(TBLIB_ROOT)/lib/libtbnet.a \
	  $(TBLIB_ROOT)/lib/libtbsys.a 

source_list=test_queue_node.hpp \
			disk_queue_unittest.cpp \
			mem_queue_unittest.cpp \
			local_queue_unittest.cpp

AM_LDFLAGS=-static -lpthread -L${top_srcdir}/test/gtest/lib -lgtest_main -lgtest  -lz -lrt ${BOOST_LDFLAGS} ${GCOV_LIB}  @BOOST_THREAD_LIB@

sbin_PROGRAMS=disk_queue_unittest mem_queue_unittest local_queue_unittest

disk_queue_unittest_SOURCES=disk_queue_unittest.cpp
mem_queue_unittest_SOURCES=mem_queue_unittest.cpp
local_queue_unittest_SOURCES=local_queue_unittest.cpp
