SUBDIRS=redis
noinst_LIBRARIES=libredisdb.a librdb.a libredisprofiler.a

LDADD=$(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a


AM_CPPFLAGS=-I$(TBLIB_ROOT)/include/tbsys -I$(TBLIB_ROOT)/include/tbnet -I${top_srcdir}/src -I${top_srcdir}/src/common -I. -DTAIR_STORAGE -Wall
AM_LDFLAGS=-lpthread ${GCOV_LIB} -Wall 

libredisdb_a_SOURCES=redis_define.h scope_lock.h    \
                     redis_db_context.h redis_db_context.cpp \
                     redis_db_session.h redis_db_session.cpp \
                     redis_server_cron.h redis_server_cron.cpp

librdb_a_SOURCES=rdb_manager.h rdb_manager.cpp
libredisprofiler_a_SOURCES=redis_profiler.cpp
EXTRA_DIST= rdb_manager.h  redis_db.h   redis_define.h  redis_server_cron.h redis_db_context.h  redis_db_session.h  redis_lib.h scope_lock.h
