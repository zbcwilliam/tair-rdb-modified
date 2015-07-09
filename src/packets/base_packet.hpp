/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * packet code and base packet are defined here
 *
 * Version: $Id: base_packet.hpp 603 2012-03-08 03:28:19Z choutian.xmm@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKETS_BASE_H
#define TAIR_PACKETS_BASE_H
#include <string>
#include <map>
#include <set>
#include <vector>
#include <tbsys.h>
#include <tbnet.h>
#include <stdint.h>
#include <zlib.h>
#include "define.hpp"
#include "data_entry.hpp"
#include "util.hpp"
#include "log.hpp"

using namespace std;
namespace tair
{
  using namespace common;

  enum
  {
    TAIR_SIMPLE_STREAM_PACKET = 0,
    TAIR_REQ_PUT_PACKET = 1,
    TAIR_REQ_GET_PACKET,
    TAIR_REQ_REMOVE_PACKET,
    TAIR_REQ_REMOVE_AREA_PACKET,
    TAIR_REQ_STAT_PACKET,
    TAIR_REQ_PING_PACKET,
    TAIR_REQ_DUMP_PACKET,
    TAIR_REQ_PARAM_PACKET,
    TAIR_REQ_HEARTBEAT_PACKET,
    TAIR_REQ_INCDEC_PACKET = 11,
    TAIR_REQ_MUPDATE_PACKET = 13,
    TAIR_REQ_MPUT_PACKET = 15,

    TAIR_RESP_RETURN_PACKET = 101,
    TAIR_RESP_GET_PACKET,
    TAIR_RESP_STAT_PACKET,
    TAIR_RESP_HEARTBEAT_PACKET,
    TAIR_RESP_INCDEC_PACKET,

    TAIR_REQ_GET_GROUP_PACKET = 1002,
    TAIR_REQ_GET_SVRTAB_PACKET,
    TAIR_REQ_CONFHB_PACKET,
    TAIR_REQ_SETMASTER_PACKET,
    TAIR_REQ_GROUP_NAMES_PACKET,
    TAIR_REQ_QUERY_INFO_PACKET = 1009,

    TAIR_RESP_GET_GROUP_PACKET = 1102,
    TAIR_RESP_GET_SVRTAB_PACKET,
    TAIR_RESP_GROUP_NAMES_PACKET,
    TAIR_RESP_QUERY_INFO_PACKET = 1106,

    TAIR_REQ_DUMP_BUCKET_PACKET = 1200,
    TAIR_REQ_MIG_FINISH_PACKET,

    TAIR_REQ_DUPLICATE_PACKET = 1300,
    TAIR_RESP_DUPLICATE_PACKET,
    TAIR_RESP_GET_MIGRATE_MACHINE_PACKET,

    //items
    TAIR_REQ_ADDITEMS_PACKET = 1400,
    TAIR_REQ_GETITEMS_PACKET,
    TAIR_REQ_REMOVEITEMS_PACKET,
    TAIR_REQ_GETANDREMOVEITEMS_PACKET,
    TAIR_REQ_GETITEMSCOUNT_PACKET,
    TAIR_RESP_GETITEMS_PACKET,

    TAIR_REQ_DATASERVER_CTRL_PACKET = 1500,
    TAIR_REQ_GET_MIGRATE_MACHINE_PACKET,

    //list
    TAIR_REQ_LPOP_PACKET = 2100,
    TAIR_RESP_LPOP_PACKET = 2200,

    TAIR_REQ_LPUSH_PACKET = 2101,
    TAIR_RESP_LPUSH_PACKET = 2201,

    TAIR_REQ_RPOP_PACKET = 2102,
    TAIR_RESP_RPOP_PACKET = 2202,

    TAIR_REQ_RPUSH_PACKET = 2103,
    TAIR_RESP_RPUSH_PACKET = 2203,

    TAIR_REQ_LPUSHX_PACKET = 2104,
    TAIR_RESP_LPUSHX_PACKET = 2204,

    TAIR_REQ_RPUSHX_PACKET = 2105,
    TAIR_RESP_RPUSHX_PACKET = 2205,

    TAIR_REQ_LINDEX_PACKET = 2106,
    TAIR_RESP_LINDEX_PACKET = 2206,

    //hset
    TAIR_REQ_HGETALL_PACKET = 2107,
    TAIR_RESP_HGETALL_PACKET = 2207,

    TAIR_REQ_HINCRBY_PACKET = 2108,
    TAIR_RESP_HINCRBY_PACKET = 2208,

    TAIR_REQ_HMSET_PACKET = 2109,
    TAIR_RESP_HMSET_PACKET = 2209,

    TAIR_REQ_HSET_PACKET = 2110,
    TAIR_RESP_HSET_PACKET = 2210,

    TAIR_REQ_HSETNX_PACKET = 2111,
    TAIR_RESP_HSETNX_PACKET = 2211,

    TAIR_REQ_HGET_PACKET = 2112,
    TAIR_RESP_HGET_PACKET = 2212,

    TAIR_REQ_HMGET_PACKET = 2113,
    TAIR_RESP_HMGET_PACKET = 2213,

    TAIR_REQ_HVALS_PACKET = 2114,
    TAIR_RESP_HVALS_PACKET = 2214,

    TAIR_REQ_HDEL_PACKET = 2115,
    TAIR_RESP_HDEL_PACKET = 2215,

    //set
    TAIR_REQ_SCARD_PACKET = 2116,
    TAIR_RESP_SCARD_PACKET = 2216,

    TAIR_REQ_SMEMBERS_PACKET = 2117,
    TAIR_RESP_SMEMBERS_PACKET = 2217,

    TAIR_REQ_SADD_PACKET = 2118,
    TAIR_RESP_SADD_PACKET = 2218,

    TAIR_REQ_SPOP_PACKET = 2119,
    TAIR_RESP_SPOP_PACKET = 2219,


    //zset
    TAIR_REQ_ZRANGE_PACKET = 2120,
    TAIR_RESP_ZRANGE_PACKET = 2220,

    TAIR_REQ_ZREVRANGE_PACKET = 2121,
    TAIR_RESP_ZREVRANGE_PACKET = 2221,

    TAIR_REQ_ZSCORE_PACKET = 2122,
    TAIR_RESP_ZSCORE_PACKET = 2222,

    TAIR_REQ_ZRANGEBYSCORE_PACKET = 2123,
    TAIR_RESP_ZRANGEBYSCORE_PACKET = 2223,

    TAIR_REQ_ZADD_PACKET = 2124,
    TAIR_RESP_ZADD_PACKET = 2224,

    TAIR_REQ_ZRANK_PACKET = 2125,
    TAIR_RESP_ZRANK_PACKET = 2225,

    TAIR_REQ_ZCARD_PACKET = 2126,
    TAIR_RESP_ZCARD_PACKET = 2226,

    TAIR_REQ_EXPIRE_PACKET = 2127,
    TAIR_RESP_EXPIRE_PACKET = 2227,

    TAIR_REQ_LTRIM_PACKET = 2128,
    TAIR_RESP_LTRIM_PACKET = 2228,

    TAIR_REQ_LREM_PACKET = 2129,
    TAIR_RESP_LREM_PACKET = 2229,

    TAIR_REQ_LRANGE_PACKET = 2130,
    TAIR_RESP_LRANGE_PACKET = 2230,

    TAIR_REQ_EXPIREAT_PACKET = 2131,
    TAIR_RESP_EXPIREAT_PACKET = 2231,

    TAIR_REQ_PERSIST_PACKET = 2132,
    TAIR_RESP_PERSIST_PACKET = 2232,

    TAIR_REQ_LLEN_PACKET = 2133,
    TAIR_RESP_LLEN_PACKET = 2233,

    TAIR_REQ_TTL_PACKET = 2134,
    TAIR_RESP_TTL_PACKET = 2234,

    TAIR_REQ_TYPE_PACKET = 2135,
    TAIR_RESP_TYPE_PACKET = 2235,

	TAIR_REQ_HLEN_PACKET = 2136,
	TAIR_RESP_HLEN_PACKET = 2236,

	TAIR_REQ_ZREM_PACKET = 2137,
	TAIR_RESP_ZREM_PACKET = 2237,

	TAIR_REQ_ZREMRANGEBYRANK_PACKET = 2138,
	TAIR_RESP_ZREMRANGEBYRANK_PACKET = 2238,

	TAIR_REQ_ZREMRANGEBYSCORE_PACKET = 2139,
	TAIR_RESP_ZREMRANGEBYSCORE_PACKET = 2239,

	TAIR_REQ_ZREVRANK_PACKET = 2140,
	TAIR_RESP_ZREVRANK_PACKET = 2240,

	TAIR_REQ_ZCOUNT_PACKET = 2141,
	TAIR_RESP_ZCOUNT_PACKET = 2241,

	TAIR_REQ_ZINCRBY_PACKET = 2142,
	TAIR_RESP_ZINCRBY_PACKET = 2242,

    TAIR_RESP_ZRANGEWITHSCORE_PACKET = 2243,
    TAIR_RESP_ZREVRANGEWITHSCORE_PACKET = 2244,

    TAIR_REQ_SREM_PACKET = 2145,
	TAIR_RESP_SREM_PACKET = 2245,

    TAIR_REQ_SADDMULTI_PACKET = 2146,
    TAIR_RESP_SADDMULTI_PACKET = 2246,

    TAIR_REQ_SREMMULTI_PACKET = 2147,
    TAIR_RESP_SREMMULTI_PACKET = 2247,

    TAIR_REQ_SMEMBERSMULTI_PACKET = 2148,
    TAIR_RESP_SMEMBERSMULTI_PACKET = 2248,

    TAIR_REQ_PUTNX_PACKET = 2149,

    TAIR_REQ_MPUTNX_PACKET = 2150,

    TAIR_REQ_GENERIC_ZRANGEBYSCORE_PACKET = 2151,
    TAIR_RESP_GENERIC_ZRANGEBYSCORE_PACKET = 2251,

    TAIR_REQ_LAZY_REMOVE_AREA_PACKET = 2152,

    TAIR_REQ_EXISTS_PACKET = 2153,

    TAIR_REQ_INFO_PACKET = 2154,
    TAIR_RESP_INFO_PACKET = 2254,

    TAIR_REQ_GETSET_PACKET = 2155,
    TAIR_RESP_GETSET_PACKET = 2255,

    TAIR_REQ_HKEYS_PACKET = 2156,
    TAIR_RESP_HKEYS_PACKET = 2256,

    TAIR_REQ_LPUSH_LIMIT_PACKET = 2157,

    TAIR_REQ_RPUSH_LIMIT_PACKET = 2158,

    TAIR_REQ_LPUSHX_LIMIT_PACKET = 2159,

    TAIR_REQ_RPUSHX_LIMIT_PACKET = 2160,

    TAIR_REQ_HEXISTS_PACKET = 2161,

    TAIR_REQ_ADD_FILTER_PACKET = 2162,

    TAIR_REQ_REMOVE_FILTER_PACKET = 2163,

    TAIR_REQ_DUMP_AREA_PACKET = 2164,

    TAIR_REQ_LOAD_AREA_PACKET = 2165
  };

  enum
  {
    DIRECTION_RECEIVE = 1,
    DIRECTION_SEND
  };

  enum
  {
    TAIR_STAT_TOTAL = 1,
    TAIR_STAT_SLAB = 2,
    TAIR_STAT_HASH = 3,
    TAIR_STAT_AREA = 4,
    TAIR_STAT_GET_MAXAREA = 5,
    TAIR_STAT_ONEHOST = 256
  };

#define CLEAR_DATA_VECTOR(values, needfree) do {                    \
    if ((needfree)) {                                               \
        data_entry *entry = NULL;                                   \
        for(size_t i = 0; i < (values).size(); ++i) {               \
	        entry = (values)[i];                                    \
	        if (entry != NULL) {                                    \
	            delete (entry);                                     \
            }                                                       \
        }                                                           \
    }                                                               \
    (values).clear();                                               \
}while(0)

#define PUT_DATAVECTOR_TO_BUFFER(output,values) do {                \
    data_entry *entry = NULL;                                       \
    size_t value_size = (values).size();                            \
    output->writeInt32(value_size);                                 \
    for (size_t i = 0; i < value_size; i++) {                       \
        entry = (values)[i];                                        \
        (output)->writeInt32(entry->get_size());                    \
        (output)->writeBytes(entry->get_data(), entry->get_size()); \
    }                                                               \
}while(0)

#define PUT_DATAENTRY_TO_BUFFER(output,key) do {                    \
    (output)->writeInt32((key).get_size());                         \
    (output)->writeBytes((key).get_data(), (key).get_size());       \
}while(0)

#define GETKEY_FROM_DATAENTRY(input,key) do {                  \
    if((input)->getDataLen() < 4) {                            \
        log_warn("buffer data too few.");                      \
        return false;                                          \
    }                                                          \
    int32_t len = (input)->readInt32();                        \
    if (len < 0 || (input)->getDataLen() < len) {              \
        log_warn("buffer data too few.");                      \
        return false;                                          \
    }                                                          \
    (key).set_data(NULL, len);                                 \
    if ((input)->readBytes((key).get_data(), len) == false) {  \
        log_warn("buffer data too few.");                      \
        return false;                                          \
    }                                                          \
}while(0)

#define PUT_INT64_TO_BUFFER(output,key)  (output)->writeInt64((key))

#define GETKEY_FROM_INT64(input,key) do {                     \
    if((input)->getDataLen() < 8) {                           \
        log_warn("buffer data too few");                      \
        return false;                                         \
    }                                                         \
    (key) = (input)->readInt64();                             \
}while(0)

#define PUT_INT32_TO_BUFFER(output,key)  (output)->writeInt32((key))

#define GETKEY_FROM_INT32(input,key) do {                   \
    if((input)->getDataLen() < 4) {                         \
        log_warn("buffer data too few");                    \
        return false;                                       \
    }                                                       \
    (key) = (input)->readInt32();                           \
}while(0)

#define PUT_INT16_TO_BUFFER(output,key)  (output)->writeInt16((key))

#define GETKEY_FROM_INT16(input,key) do {                   \
    if((input)->getDataLen() < 2) {                         \
        log_warn("buffer data too few");                    \
        return false;                                       \
    }                                                       \
    (key) = (input)->readInt16();                           \
}while(0)

#define PUT_DOUBLE_TO_BUFFER(output,score) do {             \
    char buffer[8];                                         \
    DOUBLE_TO_BYTES((score), buffer);                       \
    (output)->writeBytes(buffer,8);                         \
}while(0)

#define GETKEY_FROM_DOUBLE(input,score) do {                \
    if((input)->getDataLen() < 8) {                         \
        log_warn("buffer data too few");                    \
        return false;                                       \
    }                                                       \
    char buffer[8];                                         \
	(input)->readBytes(buffer,8);                           \
	BYTES_TO_DOUBLE(buffer, (score));					    \
}while(0)

//use bigdian
#define BYTES_TO_DOUBLE(buffer, score) (score) = *(double*)(buffer)

#define DOUBLE_TO_BYTES(score, buffer) memcpy((buffer), &(score), 8)

#define GETKEY_FROM_DATAVECTOR(input,values) do {                       \
        if((input)->getDataLen() < 4) {                                 \
            log_warn("buffer data too few");                            \
            return false;                                               \
        }                                                               \
        int32_t vlen    = (input)->readInt32();                         \
        int32_t dlen    = 0;                                            \
        data_entry *entry = NULL;                                       \
        for (int i = 0; i < vlen; ++i) {                                \
            if((input)->getDataLen() < 4) {                             \
                log_warn("buffer data too few");                        \
                return false;                                           \
            }                                                           \
            dlen = (input)->readInt32();                                \
            if (dlen == 0) {                                            \
                continue;                                               \
            }                                                           \
            entry = new data_entry();                                   \
            entry->set_data(NULL, dlen);                                \
            if((input)->readBytes(entry->get_data(), dlen) == false) {  \
                log_warn("buffer data too few");                        \
                return false;                                           \
            }                                                           \
                                                                        \
            (values).push_back(entry);                                  \
        }                                                               \
}while(0)

#define CREATE_HEADER do {                                  \
    (output)->writeInt8((server_flag));                     \
    (output)->writeInt16((area));                           \
}while(0)

#define HEADER_VERIFY do {                                  \
    if ((header)->_dataLen < 3) {                           \
        log_warn( "buffer data too few.");                  \
        return false;                                       \
    }                                                       \
    (server_flag) = (input)->readInt8();                    \
    (area) = (input)->readInt16();                          \
}while(0)


  class base_packet:public tbnet::Packet
  {
  public:
    base_packet ()
    {
      connection   = NULL;
      direction    = DIRECTION_SEND;
      no_free      = false;
      server_flag  = 0;
      request_time = 0;
      hash_code    = -1;
    }

    virtual ~ base_packet ()
    {
    }

    // Connection
    tbnet::Connection * get_connection ()
    {
      return connection;
    }

    // connection
    void set_connection (tbnet::Connection * connection)
    {
      this->connection = connection;
    }

    // direction
    void set_direction (int direction)
    {
      this->direction = direction;
    }

    // direction
    int get_direction ()
    {
      return direction;
    }

    void free ()
    {
      if (!no_free) {
	    delete this;
      }
    }

    void set_no_free ()
    {
      no_free = true;
    }

    virtual int get_code()
    {
      assert(0);
      return 0;
    }

  private:
    base_packet & operator = (const base_packet &);

    tbnet::Connection * connection;
    int direction;
    bool no_free;
  public:
    int64_t hash_code;
    uint8_t server_flag;
    int64_t request_time;
  };
}
#endif
