AM_CPPFLAGS= @BOOST_CPPFLAGS@ -I$(TBLIB_ROOT)/include/tbsys \
			 -I$(TBLIB_ROOT)/include/tbnet \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/packets \
			 -I${top_srcdir}/src/storage/fdb \
			 -I${top_srcdir}/src/storage/mdb \
			 -I${top_srcdir}/src/storage/kdb \
			 -I${top_srcdir}/src/storage \
			 -I${top_srcdir}/src/client \
			 -I${top_srcdir}/src/dataserver \
			 -I${top_srcdir}/src/plugin \
       -I${top_srcdir}/src  \
       -I${top_srcdir}/test/gtest \
       -I${top_srcdir}/src/storage/kdb/kyotocabinet

LDADD= \
	  $(top_builddir)/src/client/.libs/libtairclientapi.a \
	  $(top_builddir)/src/storage/mdb/libmdb.a \
	  $(top_builddir)/src/storage/fdb/libfdb.a \
	  $(top_builddir)/src/storage/kdb/libkdb.a \
    ${top_builddir}/src/storage/kdb/kyotocabinet/libkyotocabinet.a \
	  $(TBLIB_ROOT)/lib/libtbnet.a \
	  $(TBLIB_ROOT)/lib/libtbsys.a

AM_LDFLAGS=-static -lpthread -L${top_srcdir}/test/gtest/lib -lgtest_main -lgtest  -lz -lrt ${BOOST_LDFLAGS} ${GCOV_LIB}  @BOOST_THREAD_LIB@

sbin_PROGRAMS=binlog_test

binlog_test_SOURCES=binlog_test.cpp
