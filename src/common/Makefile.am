AM_CPPFLAGS= -I$(TBLIB_ROOT)/include/tbsys \
	     -I$(TBLIB_ROOT)/include/tbnet \
	     -I${top_srcdir}/src/storage \
	     @BOOST_CPPFLAGS@

AM_LDFLAGS= -Wall -Werror -rdynamic $(BOOST_LDFLAGS)  ${GCOV_LIB} \
      -lz -lrt -lpthread \
      -ldl \
      @BOOST_THREAD_LIB@

LDADD=$(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

base_src=data_dumpper.cpp	\
	 stat_helper.cpp	\
	 util.cpp	\
	 data_dumpper.hpp	\
	 data_entry.hpp	\
	 define.hpp	\
	 dump_data_info.hpp	\
	 hash.hpp	\
	 item_data_info.hpp	\
	 log.hpp	\
	 mmap_file.hpp	\
	 stat_helper.hpp	\
	 stat_info.hpp	\
	 util.hpp	\
	 wait_object.hpp \
	 queue_config.h \
	 file_op.hpp \
	 binlog.hpp \
	 base_queue_node.hpp \
	 disk_queue.hpp \
	 mem_queue.hpp \
	 local_queue.hpp

noinst_LIBRARIES=libtair_common.a

include_HEADERS=item_data_info.hpp data_entry.hpp define.hpp util.hpp hash.hpp log.hpp

libtair_common_a_SOURCES = ${base_src}

