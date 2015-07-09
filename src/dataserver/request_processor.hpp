/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * request dispatcher header
 *
 * Version: $Id: request_processor.hpp 603 2012-03-08 03:28:19Z choutian.xmm@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include "data_entry.hpp"
#include "tair_manager.hpp"
#include "heartbeat_thread.hpp"

#include "duplicate_packet.hpp"
#include "mupdate_packet.hpp"

#include "response_return_packet.hpp"
#include "lazy_remove_area_packet.hpp"
#include "dump_area_packet.hpp"
#include "load_area_packet.hpp"
#include "expire_packet.hpp"
#include "expireat_packet.hpp"
#include "persist_packet.hpp"
#include "ttl_packet.hpp"
#include "type_packet.hpp"
#include "info_packet.hpp"
#include "exists_packet.hpp"

#include "duplicate_packet.hpp"
#include "mupdate_packet.hpp"

#include "put_packet.hpp"
#include "get_packet.hpp"
#include "getset_packet.hpp"
#include "remove_packet.hpp"
#include "inc_dec_packet.hpp"

#include "hexists_packet.hpp"
#include "hgetall_packet.hpp"
#include "hincrby_packet.hpp"
#include "hmset_packet.hpp"
#include "hset_packet.hpp"
#include "hsetnx_packet.hpp"
#include "hget_packet.hpp"
#include "hmget_packet.hpp"
#include "hkeys_packet.hpp"
#include "hvals_packet.hpp"
#include "hdel_packet.hpp"
#include "hlen_packet.hpp"

#include "lindex_packet.hpp"
#include "lrpush_packet.hpp"
#include "lrpop_packet.hpp"
#include "ltrim_packet.hpp"
#include "llen_packet.hpp"
#include "lrem_packet.hpp"
#include "lrange_packet.hpp"

#include "scard_packet.hpp"
#include "smembers_packet.hpp"
#include "sadd_packet.hpp"
#include "spop_packet.hpp"
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
#include "zremrangebyrank_packet.hpp"
#include "zremrangebyscore_packet.hpp"

#include "addfilter_packet.hpp"
#include "removefilter_packet.hpp"

namespace tair {
   class request_processor {
   public:
      request_processor(tair_manager *tair_mgr, heartbeat_thread *heart_beat, tbnet::ConnectionManager *connection_mgr);
      ~request_processor();

      int process(request_duplicate *request, bool &send_return);
      int process(request_mupdate *request,bool &send_return);

      /* common */
      int process (request_info * request, bool & send_return);
      int process (request_expire * request, bool & send_return);
      int process (request_expireat * request, bool & send_return);
      int process (request_persist * request, bool & send_return);
      int process (request_remove * request, bool & send_return);
      int process (request_ttl * request, bool & send_return);
      int process (request_type * request, bool & send_return);
      int process (request_exists * request, bool & send_return);
      int process (request_lazy_remove_area * request, bool & send_return);
      int process (request_load_area * request, bool & send_return);
      int process (request_dump_area * request, bool & send_return);
      int process (request_addfilter * request, bool & send_return);
      int process (request_removefilter * request, bool & send_return);

      /* string */
      int process (request_put * request, bool & send_return);
      int process (request_putnx * request, bool & send_return);
      int process (request_get * request, bool & send_return);
      int process (request_getset * request, bool & send_return);
      int process (request_inc_dec * request, bool & send_return);

      /* list */
      int process (request_lindex * request, bool & send_return);
      int process (request_lrpop * request, bool & send_return);
      int process (request_lrpush * request, bool & send_return);
      int process (request_lrpush_limit * request, bool & send_return);
      int process (request_ltrim * request, bool & send_return);
      int process (request_lrem * request, bool & send_return);
      int process (request_llen * request, bool & send_return);
      int process (request_lrange * request, bool & send_return);

      /* hset */
      int process (request_hexists * request, bool & send_return);
      int process (request_hgetall * request, bool & send_return);
      int process (request_hkeys * request, bool & send_return);
      int process (request_hincrby * request, bool & send_return);
      int process (request_hmset * request, bool & send_return);
      int process (request_hset * request, bool & send_return);
      int process (request_hsetnx * request, bool & send_return);
      int process (request_hget * request, bool & send_return);
      int process (request_hmget * request, bool & send_return);
      int process (request_hvals * request, bool & send_return);
      int process (request_hdel * request, bool & send_return);
      int process (request_hlen * request, bool & send_return);

      /* set */
      int process (request_scard * request, bool & send_return);
      int process (request_smembers * request, bool & send_return);
      int process (request_sadd * request, bool & send_return);
      int process (request_spop * request, bool & send_return);
      int process (request_srem * request, bool & send_return);
      int process (request_sadd_multi * request, bool & send_return);
      int process (request_srem_multi * request, bool & send_return);
      int process (request_smembers_multi * request, bool & send_return);

      /* zset */
      int process (request_zscore * request, bool & send_return);
      int process (request_zrange * request, bool & send_return);
      int process (request_zrevrange * request, bool & send_return);
      int process (request_zrangebyscore * request, bool & send_return);
      int process (request_generic_zrangebyscore * request, bool & send_return);
      int process (request_zadd * request, bool & send_return);
      int process (request_zrank * request, bool & send_return);
      int process (request_zrevrank * request, bool & send_return);
	  int process (request_zcount * request, bool & send_return);
	  int process (request_zincrby * request, bool & send_return);
      int process (request_zcard * request, bool & send_return);
      int process (request_zrem * request, bool &send_return);
	  int process (request_zremrangebyrank * request, bool &send_return);
	  int process (request_zremrangebyscore * request, bool &send_return);

   private:
      bool do_proxy(uint64_t target_server_id, base_packet *proxy_packet, base_packet *packet);
      template<class T>
      bool do_duplicate(int rc, T* & request);
	  template<class T, class P>
      static int process_before(request_processor *processor,
			  tair_manager *tair_mgr, P* & request, T* & resp);
      template<class T, class P>
	  static int process_after(heartbeat_thread *heart_beat, T* & resp, P* & request,
		   int& rc, bool& send_return);
   private:
      tair_manager *tair_mgr;
      heartbeat_thread *heart_beat;
      tbnet::ConnectionManager *connection_mgr;
   };
}
