if WITH_KDB
KDB_CPPFLAGS= -DWITH_KDB -I${top_srcdir}/src/storage/kdb -I${top_srcdir}/src/storage/kdb/kyotocabinet
KDB_LDFLAGS= $(top_builddir)/src/storage/kdb/libkdb.a $(top_builddir)/src/storage/kdb/kyotocabinet/libkyotocabinet.a
endif

AM_CPPFLAGS= -I$(TBLIB_ROOT)/include/tbsys \
	     -I$(TBLIB_ROOT)/include/tbnet \
	     -I${top_srcdir}/src \
	     -I$(TBLIB_ROOT)/src/configserver \
	     -I${top_srcdir}/src/storage \
	     -I${top_srcdir}/src/dataserver \
	     -I${top_srcdir}/src/common \
	     -I${top_srcdir}/src/packets \
	     -I${top_srcdir}/src/storage/fdb \
	     -I${top_srcdir}/src/storage/mdb \
	     -I${top_srcdir}/src/storage/rdb \
	     -I${top_srcdir}/src/storage/mock \
	     $(KDB_CPPFLAGS) \
	     -I${top_srcdir}/src/plugin \
	     @BOOST_CPPFLAGS@

AM_LDFLAGS= -rdynamic $(BOOST_LDFLAGS)  ${GCOV_LIB} -ltcmalloc -Wall -Werror

LDADD=$(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a \
      $(top_builddir)/src/storage/mdb/libmdb.a \
      $(top_builddir)/src/storage/fdb/libfdb.a \
      $(top_builddir)/src/storage/rdb/librdb.a \
      $(top_builddir)/src/storage/rdb/libredisdb.a \
      $(top_builddir)/src/storage/rdb/redis/libredis.a \
      $(top_builddir)/src/storage/rdb/libredisprofiler.a \
      $(top_builddir)/src/storage/mock/libmockdb.a \
      $(KDB_LDFLAGS) \
      $(top_builddir)/src/plugin/libtair_plugins.a \
      $(top_builddir)/src/packets/libtair_packets.a \
      $(top_builddir)/src/storage/libdump.a \
      $(TBLIB_ROOT)/lib/libtbsys.a \
      -lz -lrt -lpthread \
      -ldl \
      @BOOST_THREAD_LIB@


sbin_PROGRAMS=tair_server

include_HEADERS=tair_manager.hpp table_manager.hpp duplicate_manager.hpp duplicate_base.hpp \
 		update_log.hpp

tair_server_SOURCES=duplicate_manager.cpp request_processor.cpp tair_manager.cpp update_log.cpp \
		    heartbeat_thread.cpp migrate_manager.cpp table_manager.cpp tair_server.cpp \
		    $(top_builddir)/src/common/stat_helper.cpp	\
		    base_migrate.hpp	\
		    base_tair_manager.hpp	\
		    duplicate_base.hpp	\
		    duplicate_manager.hpp	\
		    heartbeat_thread.hpp	\
		    migrate_manager.hpp	\
		    request_processor.hpp	\
		    table_manager.hpp	\
		    tair_manager.hpp	\
		    tair_server.hpp	\
		    update_log.hpp
