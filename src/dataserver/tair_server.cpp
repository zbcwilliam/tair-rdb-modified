/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * tair_server daemon impl
 *
 * Version: $Id: tair_server.cpp 603 2012-03-08 03:28:19Z choutian.xmm@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include "tair_server.hpp"

namespace tair {

   tair_server::tair_server()
   {
      is_stoped = 0;
      tair_mgr = NULL;
      req_processor = NULL;
      conn_manager = NULL;
      worker_thread_count = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_PROCESS_THREAD_COUNT, -1);
      duplicate_thread_count = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, DUPLICATE_THREAD_NUM,
              DEFAULT_DUPLICATE_THREAD_NUM);
      task_queue_size = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION,TAIR_TASK_QUEUE_SIZE,100);
      if (worker_thread_count < 0) {
          worker_thread_count = sysconf(_SC_NPROCESSORS_CONF);
      }
      if (worker_thread_count > 0) {
         setBatchPushPacket(true);
      }
      if (task_queue_size < 0){
         task_queue_size = 100;
      }
   }

   tair_server::~tair_server()
   {
   }

   void tair_server::start()
   {
      if (!initialize()) {
         return;
      }

      if (worker_thread_count > 0) {
         task_queue_thread.start();
      }
      if (duplicate_thread_count > 0) {
         duplicate_task_queue_thread.start();
      } else {
         log_warn("duplicate in cluster is closed, if want use duplicator,"
                " please set duplicate_thread_num large than zero");
      }

      heartbeat.start();
      async_task_queue_thread.start();
      TAIR_STAT.start();

      char spec[32];
      bool ret = true;
      if (ret) {
         int port = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_PORT, TAIR_SERVER_DEFAULT_PORT);
         sprintf(spec, "tcp::%d", port);
         if (transport.listen(spec, &streamer, this) == NULL) {
            log_error("listen on port %d failed", port);
            ret = false;
         } else {
            log_info("listened on port %d", port);
         }
      }

      if (ret) {
         log_info("---- tair_server started, pid: %d ----", getpid());
      } else {
         stop();
      }

      if (worker_thread_count > 0) {
         task_queue_thread.wait();
      }
      if (duplicate_thread_count > 0) {
         duplicate_task_queue_thread.wait();
      }

      heartbeat.wait();
      async_task_queue_thread.wait();
      transport.wait();

      destroy();
   }

   void tair_server::stop()
   {
      if (is_stoped == 0) {
         is_stoped = 1;
         log_info("will stop transport");
         transport.stop();
         if (worker_thread_count > 0) {
            log_info("will stop worker taskQueue");
            task_queue_thread.stop();
         }
         if (duplicate_thread_count > 0) {
            log_info("will stop duplicate taskQueue");
            duplicate_task_queue_thread.stop();
         }

         log_info("will stop heartbeatThread");
         heartbeat.stop();
         log_info("will stop yncTaskQueue");
         async_task_queue_thread.stop();
         log_info("will stop TAIR_STAT");
         TAIR_STAT.stop();
      }
   }

   bool tair_server::initialize()
   {

      const char *dev_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_DEV_NAME, NULL);
      uint32_t local_ip = tbsys::CNetUtil::getLocalAddr(dev_name);
      int port = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_PORT, TAIR_SERVER_DEFAULT_PORT);
      local_server_ip::ip =  tbsys::CNetUtil::ipToAddr(local_ip, port);

      // packet_streamer
      streamer.setPacketFactory(&factory);

      if (worker_thread_count > 0) {
         task_queue_thread.setThreadParameter(worker_thread_count, this, NULL);
      }
      if (duplicate_thread_count > 0) {
         duplicate_task_queue_thread.setThreadParameter(duplicate_thread_count, this, NULL);
      }

      async_task_queue_thread.setThreadParameter(1, this, NULL);

      conn_manager = new tbnet::ConnectionManager(&transport, &streamer, this);
      conn_manager->setDefaultQueueLimit(0, 500);

      transport.start();

      // m_tairManager
      tair_mgr = new tair_manager();
      bool init_rc = tair_mgr->initialize(&transport, &streamer);
      if (!init_rc) {
         return false;
      }

      heartbeat.set_thread_parameter(this, &streamer, tair_mgr);

      req_processor = new request_processor(tair_mgr, &heartbeat, conn_manager);
      return true;
   }

   bool tair_server::destroy()
   {
      if (req_processor != NULL) {
         delete req_processor;
         req_processor = NULL;
      }

      if (tair_mgr != NULL) {
         log_info("will destroy m_tairManager");
         delete tair_mgr;
         tair_mgr = NULL;
      }
      if (conn_manager != NULL) {
         log_info("will destroy m_connmgr");
         delete conn_manager;
         conn_manager = NULL;
      }
      return true;
   }

   bool tair_server::handleBatchPacket(tbnet::Connection *connection, tbnet::PacketQueue &packetQueue)
   {
      heartbeat.request_count += packetQueue.size();

      tbnet::Packet *list = packetQueue.getPacketList();
      while (list != NULL) {
         base_packet *bp = (base_packet*) list;
         bp->set_connection(connection);
         bp->set_direction(DIRECTION_RECEIVE);

         if ((TBSYS_LOG_LEVEL_DEBUG<=TBSYS_LOGGER._level)) {
            bp->request_time = tbsys::CTimeUtil::getTime();
         }

         if (worker_thread_count == 0) {
            handlePacketQueue(bp, NULL);
         }

         list = list->getNext();
      }

      if (worker_thread_count > 0) {
         task_queue_thread.pushQueue(packetQueue);
      } else {
         packetQueue.clear();
      }

      return true;
   }

   tbnet::IPacketHandler::HPRetCode tair_server::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
   {
      if (!packet->isRegularPacket()) {
         log_error("ControlPacket, cmd:%d", ((tbnet::ControlPacket*)packet)->getCommand());
         return tbnet::IPacketHandler::FREE_CHANNEL;
      }

      base_packet *bp = (base_packet*) packet;
      bp->set_connection(connection);
      bp->set_direction(DIRECTION_RECEIVE);

      if ((TBSYS_LOG_LEVEL_DEBUG<=TBSYS_LOGGER._level)) {
         bp->request_time = tbsys::CTimeUtil::getTime();
      }

      if (worker_thread_count == 0) {
         handlePacketQueue(bp, NULL);
         delete packet;
      } else {
         int pcode = packet->getPCode();
         if (TAIR_REQ_DUPLICATE_PACKET == pcode) {
            duplicate_task_queue_thread.push(packet);
         }else {
            if ( !task_queue_thread.push(packet,task_queue_size,false) ){
               bp->free();
               return tbnet::IPacketHandler::KEEP_CHANNEL;
            }
         }
      }
      heartbeat.request_count ++;

      return tbnet::IPacketHandler::FREE_CHANNEL;
   }

   bool tair_server::handlePacketQueue(tbnet::Packet *apacket, void *args)
   {
      base_packet *packet = (base_packet*)apacket;
      int pcode = packet->getPCode();

      bool send_return = true;
      int ret = TAIR_RETURN_SUCCESS;
      const char *msg = "";
      char buf[100];
      sprintf(buf, "pcode is %d, ip is %u", pcode, (uint32_t)(packet->get_connection()->getServerId() & 0xffffffff));
      PROFILER_START("process request");
      PROFILER_BEGIN(buf);
      switch (pcode) {
		  case TAIR_REQ_TTL_PACKET:
		  {
		      request_ttl *npacket = (request_ttl *)packet;
			  ret = req_processor->process(npacket, send_return);
			  break;
		  }
		  case TAIR_REQ_TYPE_PACKET:
		  {
			  request_type *npacket = (request_type *)packet;
			  ret = req_processor->process(npacket, send_return);
			  break;
		  }
		  case TAIR_REQ_EXISTS_PACKET:
		  {
			  request_exists *npacket = (request_exists *)packet;
			  ret = req_processor->process(npacket, send_return);
			  break;
		  }
		  case TAIR_REQ_PUT_PACKET:
		  {
			  request_put *npacket = (request_put *)packet;
			  ret = req_processor->process (npacket, send_return);
			  break;
		  }
          case TAIR_REQ_PUTNX_PACKET:
          {
              request_putnx *npacket = (request_putnx *)packet;
              ret = req_processor->process (npacket, send_return);
              break;
          }
		  case TAIR_REQ_GET_PACKET:
		  {
			  request_get *npacket = (request_get *) packet;
			  ret = req_processor->process (npacket, send_return);
			  send_return = false;
			  break;
		  }
		  case TAIR_REQ_GETSET_PACKET:
		  {
			  request_getset *npacket = (request_getset *)packet;
			  ret = req_processor->process(npacket, send_return);
			  break;
		  }
		  case TAIR_REQ_REMOVE_PACKET:
		  {
			  request_remove *npacket = (request_remove *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end remove,prepare to send return packet");
			  break;
		  }
		  case TAIR_REQ_LINDEX_PACKET:
		  {
			  request_lindex *npacket = (request_lindex *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end lindex, prepare to send return packet");
			  break;
		  }
		  case TAIR_REQ_LPOP_PACKET:
		  case TAIR_REQ_RPOP_PACKET:
		  {
			  request_lrpop *npacket = (request_lrpop *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end lrpop, prepare to send return packet");
			  break;
		  }
		  case TAIR_REQ_LPUSH_PACKET:
		  case TAIR_REQ_RPUSH_PACKET:
		  case TAIR_REQ_LPUSHX_PACKET:
		  case TAIR_REQ_RPUSHX_PACKET:
		  {
			  request_lrpush *npacket = (request_lrpush *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end lrpush, prepare to send return packet");
			  break;
		  }
		  case TAIR_REQ_LPUSH_LIMIT_PACKET:
		  case TAIR_REQ_RPUSH_LIMIT_PACKET:
		  case TAIR_REQ_LPUSHX_LIMIT_PACKET:
		  case TAIR_REQ_RPUSHX_LIMIT_PACKET:
		  {
			  request_lrpush_limit *npacket = (request_lrpush_limit *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end lrpush limit, prepare to send return packet");
			  break;
		  }
          case TAIR_REQ_HEXISTS_PACKET:
          {
              request_hexists *npacket = (request_hexists *) packet;
              ret = req_processor->process (npacket, send_return);
			  log_debug ("end hexists, prepare to send return packet");

			  break;
          }
		  case TAIR_REQ_HGETALL_PACKET:
		  {
			  request_hgetall *npacket = (request_hgetall *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end hgetall, prepare to send return packet");

			  break;
		  }
          case TAIR_REQ_HKEYS_PACKET:
          {
              request_hkeys *npacket = (request_hkeys *) packet;
              ret = req_processor->process (npacket, send_return);
              log_debug ("end hkeys, prepare to send return packet");

              break;
          }
		  case TAIR_REQ_HINCRBY_PACKET:
		  {
			  request_hincrby *npacket = (request_hincrby *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end hincrby, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_HMSET_PACKET:
		  {
			  request_hmset *npacket = (request_hmset *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end hmset, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_HSET_PACKET:
		  {
			  request_hset *npacket = (request_hset *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end hset, prepare to send return packet");
			  break;
		  }
	  	  case TAIR_REQ_HSETNX_PACKET:
		  {
			  request_hsetnx *npacket = (request_hsetnx *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end hsetnx, prepare to send return packet");
			  break;
		  }
	      case TAIR_REQ_HGET_PACKET:
		  {
			  request_hget *npacket = (request_hget *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end hget, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_HMGET_PACKET:
		  {
			  request_hmget *npacket = (request_hmget *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end hmget, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_HVALS_PACKET:
		  {
			  request_hvals *npacket = (request_hvals *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end hvals, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_HDEL_PACKET:
		  {
			  request_hdel *npacket = (request_hdel *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end hdel, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_HLEN_PACKET:
		  {
			  request_hlen *npacket = (request_hlen *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end llen, prepare to sen return packet");

			  break;
		  }
		  case TAIR_REQ_LTRIM_PACKET:
		  {
			  request_ltrim *npacket = (request_ltrim *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end ltrim, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_LREM_PACKET:
		  {
			  request_lrem *npacket = (request_lrem *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end lrem, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_LLEN_PACKET:
		  {
			  request_llen *npacket = (request_llen *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end llen, prepare to sen return packet");

			  break;
		  }
		  case TAIR_REQ_LRANGE_PACKET:
		  {
			  request_lrange *npacket = (request_lrange *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end lrange, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_SCARD_PACKET:
		  {
			  request_scard *npacket = (request_scard *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end scard, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_SMEMBERS_PACKET:
		  {
			  request_smembers *npacket = (request_smembers *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end smembers, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_SADD_PACKET:
		  {
			  request_sadd *npacket = (request_sadd *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end sadd, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_SREM_PACKET:
		  {
			  request_srem *npacket = (request_srem *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end srem, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_SPOP_PACKET:
		  {
			  request_spop *npacket = (request_spop *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end lrpush, prepare to send return packet");

			  break;
		  }
          case TAIR_REQ_SADDMULTI_PACKET:
          {
              request_sadd_multi *npacket = (request_sadd_multi *) packet;
              ret = req_processor->process (npacket, send_return);
              log_debug ("end request sadd multi, prepare to send return packet");

              break;
          }
          case TAIR_REQ_SREMMULTI_PACKET:
          {
              request_srem_multi *npacket = (request_srem_multi *) packet;
              ret = req_processor->process (npacket, send_return);
              log_debug ("end request srem multi, prepare to send return packet");

              break;
          }
          case TAIR_REQ_SMEMBERSMULTI_PACKET:
          {
              request_smembers_multi *npacket = (request_smembers_multi *) packet;
              ret = req_processor->process (npacket, send_return);
              log_debug ("end request smembers multi, prepare to send retrun packet");

              break;
          }
		  case TAIR_REQ_ZSCORE_PACKET:
		  {
			  request_zscore *npacket = (request_zscore *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end zscore, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_ZRANGE_PACKET:
		  {
			  request_zrange *npacket = (request_zrange *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end zrange, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_ZREVRANGE_PACKET:
		  {
			  request_zrevrange *npacket = (request_zrevrange *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end zrevrange, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_ZRANGEBYSCORE_PACKET:
		  {
			  request_zrangebyscore *npacket = (request_zrangebyscore *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end zrangebyscore, prepare to send return packet");

			  break;
		  }
          case TAIR_REQ_GENERIC_ZRANGEBYSCORE_PACKET:
          {
              request_generic_zrangebyscore *npacket = (request_generic_zrangebyscore *)packet;
              ret = req_processor->process (npacket, send_return);
			  log_debug ("end generic zrangebyscore, prepare to send return packet, send_return=%d",send_return);

			  break;
          }
		  case TAIR_REQ_ZADD_PACKET:
		  {
			  request_zadd *npacket = (request_zadd *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end zadd, prepare to send return packet, send_return=%d", send_return);

			  break;
		  }
		  case TAIR_REQ_ZRANK_PACKET:
		  {
			  request_zrank *npacket = (request_zrank *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end zrank, prepare to send return packet");
			  break;
		  }
		  case TAIR_REQ_ZREVRANK_PACKET:
		  {
			  request_zrevrank *npacket = (request_zrevrank *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end zrevrank, prepare to send return packet, send_return=%d",send_return);
			  break;
		  }
		  case TAIR_REQ_ZCOUNT_PACKET:
		  {
			  request_zcount *npacket = (request_zcount *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end zcount, prepare to send return packet");
			  break;
		  }
		  case TAIR_REQ_ZINCRBY_PACKET:
		  {
			  request_zincrby *npacket = (request_zincrby *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end zincrby, prepare to send return packet");
			  break;
		  }
		  case TAIR_REQ_ZCARD_PACKET:
		  {
			  request_zcard *npacket = (request_zcard *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end zcard, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_ZREM_PACKET:
		  {
			  request_zrem *npacket = (request_zrem *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end zrem, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_ZREMRANGEBYRANK_PACKET:
		  {
			  request_zremrangebyrank *npacket = (request_zremrangebyrank *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end zremrangebyrank, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_ZREMRANGEBYSCORE_PACKET:
		  {
			  request_zremrangebyscore *npacket = (request_zremrangebyscore *) packet;
			  ret = req_processor->process (npacket, send_return);
		      log_debug ("end zremrangebyscore, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_EXPIRE_PACKET:
		  {
			  request_expire *npacket = (request_expire *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end expire, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_EXPIREAT_PACKET:
		  {
			  request_expireat *npacket = (request_expireat *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end expireat, prepare to send return packet");

			  break;
		  }
		  case TAIR_REQ_PERSIST_PACKET:
		  {
			  request_persist *npacket = (request_persist *) packet;
			  ret = req_processor->process (npacket, send_return);
			  log_debug ("end persist, prepare to send return packet");

			  break;
		  }
          case TAIR_REQ_INFO_PACKET:
          {
              request_info *npacket = (request_info *) packet;
              ret = req_processor->process (npacket, send_return);
              log_debug ("end info, prepare to send return packet");

              break;
          }
          case TAIR_REQ_LAZY_REMOVE_AREA_PACKET:
          {
              request_lazy_remove_area *npacket = (request_lazy_remove_area *)packet;
              ret = req_processor->process (npacket, send_return);
			  log_debug ("end lazy remove area, prepare to send return packet");

			  break;
          }
          case TAIR_REQ_DUMP_AREA_PACKET:
          {
              request_dump_area *npacket = (request_dump_area *)packet;
              ret = req_processor->process (npacket, send_return);
			  log_debug ("end dump area, prepare to send return packet");

			  break;
          }
          case TAIR_REQ_LOAD_AREA_PACKET:
          {
              request_load_area *npacket = (request_load_area *)packet;
              ret = req_processor->process (npacket, send_return);
			  log_debug ("end dump area, prepare to send return packet");

			  break;
          }
          case TAIR_REQ_ADD_FILTER_PACKET:
          {
              request_addfilter *npacket = (request_addfilter *)packet;
              ret = req_processor->process (npacket, send_return);
              log_debug ("end add filter area, prepare to send return packet");

              break;
          }
          case TAIR_REQ_REMOVE_FILTER_PACKET:
          {
              request_removefilter *npacket = (request_removefilter *)packet;
              ret = req_processor->process (npacket, send_return);
              log_debug ("end add filter area, prepare to send return packet");

              break;
          }
		 case TAIR_REQ_REMOVE_AREA_PACKET:
         {
            request_remove_area *npacket = (request_remove_area*)packet;
            if (npacket->get_direction() == DIRECTION_RECEIVE) {
               async_task_queue_thread.push(new request_remove_area(*npacket));
            } else {
               if (tair_mgr->clear(npacket->area) == false) {
                  ret = EXIT_FAILURE;
               }
            }
            break;
         }
         case TAIR_REQ_PING_PACKET:
         {
            ret = ((request_ping*)packet)->value;
            break;
         }
         case TAIR_REQ_DUMP_PACKET:
         {
            request_dump *npacket = (request_dump*)packet;
            if (npacket->get_direction() == DIRECTION_RECEIVE) {
               async_task_queue_thread.push(new request_dump(*npacket));
            } else {
               tair_mgr->do_dump(npacket->info_set);
            }
            break;
         }
         case TAIR_REQ_DUMP_BUCKET_PACKET:
         {
            ret = EXIT_FAILURE;
            break;
         }
         case TAIR_REQ_INCDEC_PACKET:
         {
            request_inc_dec *npacket = (request_inc_dec*)packet;
            ret = req_processor->process(npacket, send_return);
            break;
         }
         case TAIR_REQ_DUPLICATE_PACKET:
         {
            request_duplicate *dpacket = (request_duplicate *)packet;
            ret = req_processor->process(dpacket, send_return);
            if (ret == TAIR_RETURN_SUCCESS) send_return = false;
            break;
         }
         case TAIR_REQ_MUPDATE_PACKET:
         {
            request_mupdate *mpacket = (request_mupdate *)(packet);
            ret = req_processor->process(mpacket, send_return);
            break;
         }
         default:
         {
            ret = EXIT_FAILURE;
            log_warn("unknow packet, pcode: %d", pcode);
         }
      }
      PROFILER_END();
      PROFILER_DUMP();
      PROFILER_STOP();

      if (ret == TAIR_RETURN_PROXYED) {
         // request is proxyed
         return false;
      }

      if (send_return && packet->get_direction() == DIRECTION_RECEIVE) {
         log_debug("send return packet, return code: %d", ret);
         tair_packet_factory::set_return_packet(packet, ret, msg, heartbeat.get_client_version());
      }

      if ((TBSYS_LOG_LEVEL_DEBUG<=TBSYS_LOGGER._level)) {
         int64_t now = tbsys::CTimeUtil::getTime();
         if (packet->get_direction() == DIRECTION_RECEIVE && now-packet->request_time>100000LL) {
            log_warn("Slow, pcode: %d, %ld us", pcode, now-packet->request_time);
         }
      }

      return true;
   }

   tbnet::IPacketHandler::HPRetCode tair_server::handlePacket(tbnet::Packet *packet, void *args)
   {
      base_packet *resp = (base_packet*)packet;
      base_packet *req = (base_packet*)args;
      if (!packet->isRegularPacket()) {
         tbnet::ControlPacket *cp = (tbnet::ControlPacket*)packet;
         log_warn("ControlPacket, cmd:%d", cp->getCommand());
         if (cp->getCommand() == tbnet::ControlPacket::CMD_DISCONN_PACKET) {
            return tbnet::IPacketHandler::FREE_CHANNEL;
         }
         resp = NULL;
      }

      if (req != NULL) {
         if (resp == NULL) {
            if (req->getPCode() == TAIR_REQ_GET_PACKET) {
               response_get *p = new response_get();
               p->config_version = heartbeat.get_client_version();
               resp = p;
            } else {
               response_return *p = new response_return();
               p->config_version = heartbeat.get_client_version();
               resp = p;
            }
         }
         resp->setChannelId(req->getChannelId());
         if (req->get_connection()->postPacket(resp) == false) {
            delete resp;
         }

         if ((TBSYS_LOG_LEVEL_DEBUG<=TBSYS_LOGGER._level)) {
            int64_t now = tbsys::CTimeUtil::getTime();
            if (now-req->request_time>100000LL) {
               log_warn("Slow, migrate, pcode: %d, %ld us", req->getPCode(), now-req->request_time);
            }
         }
         delete req;
      } else if (packet != NULL) {
         packet->free();
      }
      return tbnet::IPacketHandler::KEEP_CHANNEL;
   }


} // namespace end

////////////////////////////////////////////////////////////////////////////////////////////////
// Main
////////////////////////////////////////////////////////////////////////////////////////////////
tair::tair_server *tair_server = NULL;
uint64_t tair::common::local_server_ip::ip = 0;
tbsys::CThreadMutex mutex;

void sign_handler(int sig)
{
   switch (sig) {
      case SIGTERM:
      case SIGINT:
         mutex.lock();
         if (tair_server != NULL) {
            log_info("will stop tairserver");
            tair_server->stop();
         }
         mutex.unlock();
         break;
      case 40:
         TBSYS_LOGGER.checkFile();
         break;
      case 41:
      case 42:
         if (sig==41) {
            TBSYS_LOGGER._level ++;
         } else {
            TBSYS_LOGGER._level --;
         }
         log_error("TBSYS_LOGGER._level: %d", TBSYS_LOGGER._level);
         break;
      case 43:
         PROFILER_SET_STATUS((tbsys::util::Profiler::m_profiler.status + 1) % 2);
         log_error("profiler is %s", (tbsys::util::Profiler::m_profiler.status == 1) ? "enabled" : "disabled");
         break;
      case 44: //remove expired
         mutex.lock();
         if (tair_server != NULL) {
           tair_server->get_tair_manager()->clear(-1);
         }
         mutex.unlock();
         break;
      case 45: //balance
         mutex.lock();
         if (tair_server != NULL) {
           tair_server->get_tair_manager()->clear(-2);
         }
         mutex.unlock();
         break;
      case 46: //clear all
         mutex.lock();
         if (tair_server != NULL){
           tair_server->get_tair_manager()->clear(-3);
         }
      default:
         log_error("sig: %d", sig);
   }
}

void print_usage(char *prog_name)
{
   fprintf(stderr, "%s -f config_file\n"
           "    -f, --config_file  config file name\n"
           "    -h, --help         display this help and exit\n"
           "    -V, --version      version and build time\n\n",
           prog_name);
}

char *parse_cmd_line(int argc, char *const argv[])
{
   int opt;
   const char *opt_string = "hVf:";
   struct option long_opts[] = {
      {"config_file", 1, NULL, 'f'},
      {"help", 0, NULL, 'h'},
      {"version", 0, NULL, 'V'},
      {0, 0, 0, 0}
   };

   char *config_file = NULL;
   while ((opt = getopt_long(argc, argv, opt_string, long_opts, NULL)) != -1) {
      switch (opt) {
         case 'f':
            config_file = optarg;
            break;
         case 'V':
            fprintf(stderr, "BUILD_TIME: %s %s\n", __DATE__, __TIME__);
            return NULL;
         case 'h':
            print_usage(argv[0]);
            return NULL;
      }
   }
   return config_file;
}

int main(int argc, char *argv[])
{
   // parse cmd
   char *config_file = parse_cmd_line(argc, argv);
   if (config_file == NULL) {
      print_usage(argv[0]);
      return EXIT_FAILURE;
   }

   if (TBSYS_CONFIG.load(config_file)) {
      fprintf(stderr, "load config file (%s) failed\n", config_file);
      return EXIT_FAILURE;
   }

   int pid;
   const char *pid_file_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_PID_FILE, "server.pid");
   const char *log_file_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_LOG_FILE, "server.log");
   if (1) {
      char *p, dir_path[256];
      sprintf(dir_path, "%s", pid_file_name);
      p = strrchr(dir_path, '/');
      if (p != NULL) *p = '\0';
      if (p != NULL && !tbsys::CFileUtil::mkdirs(dir_path)) {
         fprintf(stderr, "mkdir failed: %s\n", dir_path);
         return EXIT_FAILURE;
      }
      sprintf(dir_path, "%s", log_file_name);
      p = strrchr(dir_path, '/');
      if (p != NULL) *p = '\0';
      if (p != NULL && !tbsys::CFileUtil::mkdirs(dir_path)) {
         fprintf(stderr, "mkdir failed: %s\n", dir_path);
         return EXIT_FAILURE;
      }
      const char *se_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_SENGINE, NULL);
      if (se_name == NULL || (strcmp(se_name, "mdb") == 0)) {
      } else if (strcmp(se_name, "fdb") == 0){
         sprintf(dir_path, "%s", TBSYS_CONFIG.getString(TAIRFDB_SECTION, FDB_DATA_DIR, FDB_DEFAULT_DATA_DIR));
      } else if (strcmp(se_name, "kdb") == 0){
         sprintf(dir_path, "%s", TBSYS_CONFIG.getString(TAIRKDB_SECTION, KDB_DATA_DIR, KDB_DEFAULT_DATA_DIR));
      } else if (strcmp(se_name, "rdb") == 0){
         sprintf(dir_path, "%s", TBSYS_CONFIG.getString(TAIRRDB_SECTION, RDB_DATA_DIR, RDB_DEFAULT_DATA_DIR));
      }



      if(!tbsys::CFileUtil::mkdirs(dir_path)) {
         fprintf(stderr, "mkdir failed: %s\n", dir_path);
         return EXIT_FAILURE;
      }
      const char *dump_path = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_DUMP_DIR, TAIR_DEFAULT_DUMP_DIR);
      sprintf(dir_path, "%s", dump_path);
      if (!tbsys::CFileUtil::mkdirs(dir_path)) {
         fprintf(stderr, "create dump directory {%s} failed.", dir_path);
         return EXIT_FAILURE;
      }

   }

   if ((pid = tbsys::CProcess::existPid(pid_file_name))) {
      fprintf(stderr, "tair_server already running: pid=%d\n", pid);
      return EXIT_FAILURE;
   }

   const char *log_level = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_LOG_LEVEL, "info");
   TBSYS_LOGGER.setLogLevel(log_level);

   // disable profiler by default
   PROFILER_SET_STATUS(0);
   // set the threshold
   int32_t profiler_threshold = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_PROFILER_THRESHOLD, 10000);
   PROFILER_SET_THRESHOLD(profiler_threshold);

   if (tbsys::CProcess::startDaemon(pid_file_name, log_file_name) == 0) {
      for (int i=0; i<64; i++) {
         if (i==9 || i==SIGINT || i==SIGTERM || i==40) continue;
         signal(i, SIG_IGN);
      }
      signal(SIGINT, sign_handler);
      signal(SIGTERM, sign_handler);
      signal(40, sign_handler);
      signal(41, sign_handler);
      signal(42, sign_handler);
      signal(43, sign_handler); // for switch profiler enable/disable status
      signal(44, sign_handler); // remove expired item
      signal(45, sign_handler); // remove all item

      log_error("profiler disabled by default, threshold has been set to %d", profiler_threshold);

      tair_server = new tair::tair_server();
      tair_server->start();
      mutex.lock();
      delete tair_server;
      tair_server = NULL;
      mutex.unlock();

      log_info("tair_server exit.");
   }

   return EXIT_SUCCESS;
}
////////////////////////////////END
