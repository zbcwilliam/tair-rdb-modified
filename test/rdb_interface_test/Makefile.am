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
       -I/opt/gtest-1.7.0/include \
       -I${top_srcdir}/src/storage/kdb/kyotocabinet

LDADD= \
	  $(top_builddir)/src/client/.libs/libtairclientapi.a \
	  $(TBLIB_ROOT)/lib/libtbnet.a \
	  $(TBLIB_ROOT)/lib/libtbsys.a 

source_list=base_test.hpp \
            test_helper.hpp \
            test_helper.cpp

AM_LDFLAGS=-static -lpthread -L/opt/gtest-1.7.0/lib -lgtest_main -lgtest  -lz -lrt ${BOOST_LDFLAGS} ${GCOV_LIB}  @BOOST_THREAD_LIB@

sbin_PROGRAMS=sadd_general_test smembers_general_test srem_general_test hset_general_test hget_general_test hmget_general_test hmset_general_test hgetall_general_test hdel_general_test hash_normal_test set_normal_test test_test_helper

sadd_general_test_SOURCES=sadd_general_test.cpp ${source_list}
smembers_general_test_SOURCES=smembers_general_test.cpp ${source_list}
srem_general_test_SOURCES=srem_general_test.cpp ${source_list}
hset_general_test_SOURCES=hset_general_test.cpp ${source_list}
hget_general_test_SOURCES=hget_general_test.cpp ${source_list}
hmset_general_test_SOURCES=hmset_general_test.cpp ${source_list}
hmget_general_test_SOURCES=hmget_general_test.cpp ${source_list}
hgetall_general_test_SOURCES=hgetall_general_test.cpp ${source_list}
hdel_general_test_SOURCES=hdel_general_test.cpp ${source_list}
hash_normal_test_SOURCES=hash_normal_test.cpp ${source_list}
set_normal_test_SOURCES=set_normal_test.cpp ${source_list}
test_test_helper_SOURCES=test_test_helper.cpp ${source_list}
