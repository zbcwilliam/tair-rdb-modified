AM_CPPFLAGS= -I$(TBLIB_ROOT)/include/tbsys \
			 -I$(TBLIB_ROOT)/include/tbnet \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/storage/fdb \
			 -I${top_srcdir}/src/storage \
			 -I${top_srcdir}/src/dataserver \
	     	@BOOST_CPPFLAGS@

LDADD=$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a

AM_LDFLAGS=-lz -lrt -lpthread -ldl  ${GCOV_LIB} -Wall -Werror

include_HEADERS= packet_factory.hpp base_packet.hpp \
		 duplicate_packet.hpp \
		 put_packet.hpp get_packet.hpp \
		 response_return_packet.hpp

noinst_LIBRARIES=libtair_packets.a
libtair_packets_a_SOURCES=packet_factory.cpp	\
			  base_packet.hpp	\
			  conf_heartbeat_packet.hpp	\
			  data_server_ctrl_packet.hpp	\
			  dump_bucket_packet.hpp	\
			  dump_packet.hpp	\
			  duplicate_packet.hpp	\
			  get_group_packet.hpp	\
			  get_migrate_machine.hpp	\
			  get_packet.hpp	\
			  get_server_table_packet.hpp	\
			  group_names_packet.hpp	\
			  heartbeat_packet.hpp	\
			  inc_dec_packet.hpp	\
			  migrate_finish_packet.hpp	\
			  mupdate_packet.hpp	\
			  packet_factory.hpp	\
			  ping_packet.hpp	\
			  put_packet.hpp	\
			  query_info_packet.hpp	\
			  remove_area_packet.hpp	\
			  remove_packet.hpp	\
			  response_return_packet.hpp	\
			  server_hash_table_packet.hpp	\
			  lrpop_packet.hpp	\
              lrpush_packet.hpp \
              hgetall_packet.hpp \
              hincrby_packet.hpp \
              hmset_packet.hpp   \
              hset_packet.hpp    \
              hsetnx_packet.hpp  \
              hget_packet.hpp    \
              hmget_packet.hpp   \
              hvals_packet.hpp   \
              hdel_packet.hpp    \
              ltrim_packet.hpp   \
              lrem_packet.hpp    \
              lrange_packet.hpp  \
              llen_packet.hpp    \
              scard_packet.hpp   \
              smembers_packet.hpp   \
              sadd_packet.hpp     \
              spop_packet.hpp     \
              srem_packet.hpp     \
              zscore_packet.hpp   \
              zrange_packet.hpp   \
              zrevrange_packet.hpp  \
              zrangebyscore_packet.hpp  \
              zadd_packet.hpp           \
              zrank_packet.hpp          \
              zcard_packet.hpp          \
              expire_packet.hpp         \
			  expireat_packet.hpp       \
              persist_packet.hpp        \
              ttl_packet.hpp            \
              type_packet.hpp           \
              lazy_remove_area_packet.hpp \
              generic_zrangebyscore_packet.hpp \
              info_packet.hpp \
              getset_packet.hpp \
              hkeys_packet.hpp \
              exists_packet.hpp \
              hexists_packet.hpp \
              dump_area_packet.hpp \
              load_area_packet.hpp \
              addfilter_packet.hpp \
              removefilter_packet.hpp \
              set_master_packet.hpp
EXTRA_DIST=lindex_packet.hpp llen_packet.hpp lrange_packet.hpp lrem_packet.hpp lrpop_packet.hpp lrpush_packet.hpp ltrim_packet.hpp zadd_packet.hpp zcard_packet.hpp zcount_packet.hpp zincrby_packet.hpp zrangebyscore_packet.hpp zrange_packet.hpp zrank_packet.hpp zrem_packet.hpp zremrangebyrank_packet.hpp zremrangebyscore_packet.hpp zrevrange_packet.hpp zrevrank_packet.hpp zscore_packet.hpp hlen_packet.hpp
