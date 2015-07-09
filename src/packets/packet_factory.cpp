/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * packet factory creates packet according to packet code
 *
 * Version: $Id: packet_factory.cpp 603 2012-03-08 03:28:19Z choutian.xmm@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include "packet_factory.hpp"
#include "base_packet.hpp"
#include "conf_heartbeat_packet.hpp"
#include "data_server_ctrl_packet.hpp"
#include "dump_bucket_packet.hpp"
#include "dump_packet.hpp"
#include "duplicate_packet.hpp"
#include "get_group_packet.hpp"
#include "get_migrate_machine.hpp"
#include "get_packet.hpp"
#include "get_server_table_packet.hpp"
#include "group_names_packet.hpp"
#include "heartbeat_packet.hpp"
#include "inc_dec_packet.hpp"
#include "migrate_finish_packet.hpp"
#include "mupdate_packet.hpp"
#include "ping_packet.hpp"
#include "put_packet.hpp"
#include "getset_packet.hpp"
#include "query_info_packet.hpp"
#include "remove_area_packet.hpp"
#include "lazy_remove_area_packet.hpp"
#include "dump_area_packet.hpp"
#include "load_area_packet.hpp"
#include "remove_packet.hpp"
#include "response_return_packet.hpp"
#include "server_hash_table_packet.hpp"
#include "set_master_packet.hpp"
#include "lrpop_packet.hpp"
#include "lrpush_packet.hpp"
#include "lindex_packet.hpp"
#include "llen_packet.hpp"

#include "hexists_packet.hpp"
#include "hgetall_packet.hpp"
#include "hincrby_packet.hpp"
#include "hmset_packet.hpp"
#include "hset_packet.hpp"
#include "hsetnx_packet.hpp"
#include "hget_packet.hpp"
#include "hmget_packet.hpp"
#include "hvals_packet.hpp"
#include "hkeys_packet.hpp"
#include "hdel_packet.hpp"
#include "hlen_packet.hpp"

#include "ltrim_packet.hpp"
#include "lrange_packet.hpp"
#include "lrem_packet.hpp"

#include "scard_packet.hpp"
#include "smembers_packet.hpp"
#include "spop_packet.hpp"
#include "sadd_packet.hpp"
#include "srem_packet.hpp"

#include "zscore_packet.hpp"
#include "zrange_packet.hpp"
#include "zrevrange_packet.hpp"
#include "zrangebyscore_packet.hpp"
#include "generic_zrangebyscore_packet.hpp"
#include "zadd_packet.hpp"
#include "zrank_packet.hpp"
#include "zrevrank_packet.hpp"
#include "zcount_packet.hpp"
#include "zincrby_packet.hpp"
#include "zcard_packet.hpp"
#include "zrem_packet.hpp"
#include "zremrangebyscore_packet.hpp"
#include "zremrangebyrank_packet.hpp"

#include "expire_packet.hpp"
#include "expireat_packet.hpp"
#include "persist_packet.hpp"
#include "ttl_packet.hpp"
#include "type_packet.hpp"
#include "exists_packet.hpp"
#include "addfilter_packet.hpp"
#include "removefilter_packet.hpp"

namespace tair {
   tbnet::Packet *tair_packet_factory::createPacket(int pcode) {
      return _createPacket(pcode);
   }
   tbnet::Packet *tair_packet_factory::_createPacket(int pcode)
   {
      tbnet::Packet *packet = NULL;
      switch (pcode) {
         case TAIR_REQ_PUT_PACKET:
            packet = new tair::request_put();
            break;
         case TAIR_REQ_PUTNX_PACKET:
            packet = new tair::request_putnx();
            break;
         case TAIR_REQ_GET_PACKET:
            packet = new request_get();
            break;
         case TAIR_REQ_QUERY_INFO_PACKET:
            packet = new request_query_info();
            break;
         case TAIR_REQ_REMOVE_PACKET:
            packet = new request_remove();
            break;
         case TAIR_REQ_REMOVE_AREA_PACKET:
            packet = new request_remove_area();
            break;
         case TAIR_REQ_LAZY_REMOVE_AREA_PACKET:
            packet = new request_lazy_remove_area();
            break;
         case TAIR_REQ_DUMP_AREA_PACKET:
            packet = new request_dump_area();
            break;
         case TAIR_REQ_LOAD_AREA_PACKET:
            packet = new request_load_area();
            break;
         case TAIR_REQ_EXISTS_PACKET:
            packet = new request_exists();
            break;
         case TAIR_REQ_HEXISTS_PACKET:
            packet = new request_hexists();
            break;
         case TAIR_REQ_PING_PACKET:
            packet = new request_ping();
            break;
         case TAIR_REQ_DUMP_PACKET:
            packet = new request_dump();
            break;
         case TAIR_REQ_HEARTBEAT_PACKET:
            packet = new request_heartbeat();
            break;
         case TAIR_REQ_INCDEC_PACKET:
            packet = new request_inc_dec();
            break;
         case TAIR_RESP_RETURN_PACKET:
            packet = new response_return();
            break;
         case TAIR_RESP_GET_PACKET:
            packet = new response_get();
            break;
         case TAIR_REQ_GETSET_PACKET:
            packet = new request_getset();
            break;
         case TAIR_RESP_GETSET_PACKET:
            packet = new response_getset();
            break;
         case TAIR_RESP_QUERY_INFO_PACKET:
            packet = new response_query_info();
            break;
         case TAIR_RESP_HEARTBEAT_PACKET:
            packet = new response_heartbeat();
            break;
         case TAIR_RESP_INCDEC_PACKET:
            packet = new response_inc_dec();
            break;
         case TAIR_REQ_GET_GROUP_PACKET:
            packet = new request_get_group();
            break;
         case TAIR_REQ_GET_SVRTAB_PACKET:
            packet = new request_get_server_table();
            break;
         case TAIR_REQ_CONFHB_PACKET:
            packet = new request_conf_heartbeart();
            break;
         case TAIR_REQ_SETMASTER_PACKET:
            packet = new request_set_master();
            break;
         case TAIR_REQ_GROUP_NAMES_PACKET:
            packet = new request_group_names();
            break;
         case TAIR_RESP_GET_GROUP_PACKET:
            packet = new response_get_group();
            break;
         case TAIR_RESP_GET_SVRTAB_PACKET:
            packet = new response_get_server_table();
            break;
         case TAIR_RESP_GROUP_NAMES_PACKET:
            packet = new response_group_names();
            break;
         case TAIR_REQ_DUPLICATE_PACKET :
            packet = new request_duplicate();
            break;
         case TAIR_RESP_DUPLICATE_PACKET :
            packet = new response_duplicate();
            break;
         case TAIR_REQ_MIG_FINISH_PACKET:
            packet = new request_migrate_finish();
            break;
         case TAIR_REQ_DATASERVER_CTRL_PACKET:
            packet = new request_data_server_ctrl();
            break;
         case TAIR_REQ_GET_MIGRATE_MACHINE_PACKET:
            packet = new request_get_migrate_machine();
            break;
         case TAIR_RESP_GET_MIGRATE_MACHINE_PACKET:
            packet = new response_get_migrate_machine();
            break;

         case TAIR_REQ_EXPIRE_PACKET:
            packet = new request_expire();
            break;
         case TAIR_RESP_EXPIRE_PACKET:
            packet = new response_expire();
            break;
         case TAIR_REQ_EXPIREAT_PACKET:
            packet = new request_expireat();
            break;
         case TAIR_RESP_EXPIREAT_PACKET:
            packet = new response_expireat();
            break;
         case TAIR_REQ_PERSIST_PACKET:
            packet = new request_persist();
            break;
         case TAIR_RESP_PERSIST_PACKET:
            packet = new response_persist();
            break;
         case TAIR_REQ_ADD_FILTER_PACKET:
            packet = new request_addfilter();
            break;
         case TAIR_REQ_REMOVE_FILTER_PACKET:
            packet = new request_removefilter();
            break;

         //list
         case TAIR_REQ_LLEN_PACKET:
            packet = new request_llen();
            break;
         case TAIR_RESP_LLEN_PACKET:
            packet = new response_llen();
            break;
         case TAIR_REQ_LPOP_PACKET:
            packet = new request_lrpop(IS_LEFT);
            break;
         case TAIR_REQ_RPOP_PACKET:
            packet = new request_lrpop(IS_RIGHT);
            break;
         case TAIR_RESP_LPOP_PACKET:
            packet = new response_lrpop(IS_LEFT);
            break;
         case TAIR_RESP_RPOP_PACKET:
            packet = new response_lrpop(IS_RIGHT);
            break;
         case TAIR_REQ_LPUSH_PACKET:
            packet = new request_lrpush(IS_NOT_EXIST, IS_LEFT);
            break;
         case TAIR_REQ_RPUSH_PACKET:
            packet = new request_lrpush(IS_NOT_EXIST, IS_RIGHT);
            break;
         case TAIR_RESP_LPUSH_PACKET:
            packet = new response_lrpush(IS_NOT_EXIST, IS_LEFT);
            break;
         case TAIR_RESP_RPUSH_PACKET:
            packet = new response_lrpush(IS_NOT_EXIST, IS_RIGHT);
            break;
         case TAIR_REQ_LPUSHX_PACKET:
            packet = new request_lrpush(IS_EXIST, IS_LEFT);
            break;
         case TAIR_REQ_RPUSHX_PACKET:
            packet = new request_lrpush(IS_EXIST, IS_RIGHT);
            break;
         case TAIR_RESP_LPUSHX_PACKET:
            packet = new response_lrpush(IS_EXIST, IS_LEFT);
            break;
         case TAIR_RESP_RPUSHX_PACKET:
            packet = new response_lrpush(IS_EXIST, IS_RIGHT);
            break;
         case TAIR_REQ_LPUSH_LIMIT_PACKET:
            packet = new request_lrpush_limit(IS_NOT_EXIST, IS_LEFT);
            break;
         case TAIR_REQ_RPUSH_LIMIT_PACKET:
            packet = new request_lrpush_limit(IS_NOT_EXIST, IS_RIGHT);
            break;
         case TAIR_REQ_LPUSHX_LIMIT_PACKET:
            packet = new request_lrpush_limit(IS_EXIST, IS_LEFT);
            break;
         case TAIR_REQ_RPUSHX_LIMIT_PACKET:
            packet = new request_lrpush_limit(IS_EXIST, IS_RIGHT);
            break;
         case TAIR_REQ_LINDEX_PACKET:
            packet = new request_lindex();
            break;
         case TAIR_RESP_LINDEX_PACKET:
            packet = new response_lindex();
            break;
         case TAIR_REQ_LTRIM_PACKET:
            packet = new request_ltrim();
            break;
         case TAIR_RESP_LTRIM_PACKET:
            packet = new response_ltrim();
            break;
         case TAIR_REQ_LRANGE_PACKET:
            packet = new request_lrange();
            break;
         case TAIR_RESP_LRANGE_PACKET:
            packet = new response_lrange();
            break;
         case TAIR_REQ_LREM_PACKET:
            packet = new request_lrem();
            break;
         case TAIR_RESP_LREM_PACKET:
            packet = new response_lrem();
            break;

         //hset
         case TAIR_REQ_HGETALL_PACKET:
            packet = new request_hgetall();
            break;
         case TAIR_RESP_HGETALL_PACKET:
            packet = new response_hgetall();
            break;
         case TAIR_REQ_HINCRBY_PACKET:
            packet = new request_hincrby();
            break;
         case TAIR_RESP_HINCRBY_PACKET:
            packet = new response_hincrby();
            break;
         case TAIR_REQ_HMSET_PACKET:
            packet = new request_hmset();
            break;
         case TAIR_RESP_HMSET_PACKET:
            packet = new response_hmset();
            break;
         case TAIR_REQ_HSET_PACKET:
            packet = new request_hset();
            break;
         case TAIR_RESP_HSET_PACKET:
            packet = new response_hset();
            break;
         case TAIR_REQ_HSETNX_PACKET:
            packet = new request_hsetnx();
            break;
         case TAIR_RESP_HSETNX_PACKET:
            packet = new response_hsetnx();
            break;
         case TAIR_REQ_HGET_PACKET:
            packet = new request_hget();
            break;
         case TAIR_RESP_HGET_PACKET:
            packet = new response_hget();
            break;
         case TAIR_REQ_HMGET_PACKET:
            packet = new request_hmget();
            break;
         case TAIR_RESP_HMGET_PACKET:
            packet = new response_hmget();
            break;
         case TAIR_REQ_HDEL_PACKET:
            packet = new request_hdel();
            break;
         case TAIR_RESP_HDEL_PACKET:
            packet = new response_hdel();
            break;
         case TAIR_REQ_HVALS_PACKET:
            packet = new request_hvals();
			break;
         case TAIR_RESP_HVALS_PACKET:
            packet = new response_hvals();
			break;
         case TAIR_REQ_HKEYS_PACKET:
            packet = new request_hkeys();
			break;
         case TAIR_RESP_HKEYS_PACKET:
            packet = new response_hkeys();
			break;
		 case TAIR_REQ_HLEN_PACKET:
			packet = new request_hlen();
			break;
		 case TAIR_RESP_HLEN_PACKET:
			packet = new response_hlen();
			break;
         //set
         case TAIR_REQ_SCARD_PACKET:
            packet = new request_scard();
            break;
         case TAIR_RESP_SCARD_PACKET:
            packet = new response_scard();
            break;
         case TAIR_REQ_SMEMBERS_PACKET:
            packet = new request_smembers();
            break;
         case TAIR_RESP_SMEMBERS_PACKET:
            packet = new response_smembers();
            break;
         case TAIR_REQ_SADD_PACKET:
            packet = new request_sadd();
            break;
         case TAIR_RESP_SADD_PACKET:
            packet = new response_sadd();
            break;
         case TAIR_REQ_SPOP_PACKET:
            packet = new request_spop();
            break;
         case TAIR_RESP_SPOP_PACKET:
            packet = new response_spop();
            break;
         case TAIR_REQ_SREM_PACKET:
            packet = new request_srem();
            break;
         case TAIR_RESP_SREM_PACKET:
            packet = new response_srem();
            break;
         case TAIR_REQ_SADDMULTI_PACKET:
            packet = new request_sadd_multi();
            break;
         case TAIR_REQ_SREMMULTI_PACKET:
            packet = new request_srem_multi();
            break;
         case TAIR_REQ_SMEMBERSMULTI_PACKET:
            packet = new request_smembers_multi();
            break;
         case TAIR_RESP_SMEMBERSMULTI_PACKET:
            packet = new response_smembers_multi();
            break;
         //zset
         case TAIR_REQ_ZSCORE_PACKET:
            packet = new request_zscore();
            break;
         case TAIR_RESP_ZSCORE_PACKET:
            packet = new response_zscore();
            break;
         case TAIR_REQ_ZRANGE_PACKET:
            packet = new request_zrange();
            break;
         case TAIR_RESP_ZRANGE_PACKET:
            packet = new response_zrange();
            break;
         case TAIR_REQ_ZREVRANGE_PACKET:
            packet = new request_zrevrange();
            break;
         case TAIR_RESP_ZREVRANGE_PACKET:
            packet = new response_zrevrange();
            break;
         case TAIR_REQ_ZRANGEBYSCORE_PACKET:
            packet = new request_zrangebyscore();
            break;
         case TAIR_RESP_ZRANGEBYSCORE_PACKET:
            packet = new response_zrangebyscore();
            break;
         case TAIR_REQ_GENERIC_ZRANGEBYSCORE_PACKET:
            packet = new request_generic_zrangebyscore();
            break;
         case TAIR_RESP_GENERIC_ZRANGEBYSCORE_PACKET:
            packet = new response_generic_zrangebyscore();
            break;
         case TAIR_REQ_ZADD_PACKET:
            packet = new request_zadd();
            break;
         case TAIR_RESP_ZADD_PACKET:
            packet = new response_zadd();
            break;
         case TAIR_REQ_ZRANK_PACKET:
            packet = new request_zrank();
            break;
         case TAIR_RESP_ZRANK_PACKET:
            packet = new response_zrank();
            break;
		 case TAIR_REQ_ZREVRANK_PACKET:
			packet = new request_zrevrank();
			break;
		 case TAIR_RESP_ZREVRANK_PACKET:
			packet = new response_zrevrank();
			break;
		 case TAIR_REQ_ZCOUNT_PACKET:
			packet = new request_zcount();
			break;
		 case TAIR_RESP_ZCOUNT_PACKET:
			packet = new response_zcount();
			break;
		 case TAIR_REQ_ZINCRBY_PACKET:
			packet = new request_zincrby();
			break;
		 case TAIR_RESP_ZINCRBY_PACKET:
			packet = new response_zincrby();
			break;
         case TAIR_REQ_ZCARD_PACKET:
            packet = new request_zcard();
            break;
         case TAIR_RESP_ZCARD_PACKET:
            packet = new response_zcard();
            break;
		 case TAIR_REQ_ZREM_PACKET:
			packet = new request_zrem();
			break;
		 case TAIR_RESP_ZREM_PACKET:
			packet = new response_zrem();
			break;
		 case TAIR_REQ_ZREMRANGEBYRANK_PACKET:
			packet = new request_zremrangebyrank();
			break;
		 case TAIR_RESP_ZREMRANGEBYRANK_PACKET:
			packet = new response_zremrangebyrank();
			break;
		 case TAIR_REQ_ZREMRANGEBYSCORE_PACKET:
			packet = new request_zremrangebyscore();
			break;
		 case TAIR_RESP_ZREMRANGEBYSCORE_PACKET:
			packet = new response_zremrangebyscore();
			break;
		//common
         case TAIR_REQ_TTL_PACKET:
            packet = new request_ttl();
            break;
         case TAIR_RESP_TTL_PACKET:
            packet = new response_ttl();
            break;
         case TAIR_REQ_TYPE_PACKET:
            packet = new request_type();
            break;
         case TAIR_RESP_TYPE_PACKET:
            packet = new response_type();
            break;
         default:
            log_error("createpacket error: pcode=%d", pcode);
            break;
      }
      if (packet) {
         assert(pcode == packet->getPCode());
      }
      return packet;
   }

   tair_packet_factory::~tair_packet_factory()
   {
   }

   int tair_packet_factory::set_return_packet(base_packet *packet, int code,const char *msg, uint32_t version)
   {
      response_return *return_packet = new response_return(packet->getChannelId(), code, msg);
      return_packet->config_version = version;
      if (packet->get_connection()->postPacket(return_packet) == false) {
         log_warn("send ReturnPacket failure, request pcode: %d", packet->getPCode());
         delete return_packet;
      }
      return EXIT_SUCCESS;
   }


}
