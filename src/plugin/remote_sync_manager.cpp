/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * duplicate_manager.cpp is to performe the duplicate func when copy_count > 1
 *
 * Version: $Id: dup_sync_manager.cpp 28 2011-09-19 05:18:09Z xinshu.wzx@taobao.com $
 *
 * Authors:
 *   xinshu <xinshu.wzx@taobao.com>
 *
 */

#include "put_packet.hpp"
#include "inc_dec_packet.hpp"
#include "getset_packet.hpp"
#include "expire_packet.hpp"
#include "expireat_packet.hpp"
#include "persist_packet.hpp"
#include "ltrim_packet.hpp"
#include "lrem_packet.hpp"
#include "hincrby_packet.hpp"
#include "hsetnx_packet.hpp"
#include "spop_packet.hpp"
#include "zadd_packet.hpp"
#include "zincrby_packet.hpp"
#include "zrem_packet.hpp"
#include "zremrangebyrank_packet.hpp"

#include "remote_sync_manager.hpp"

namespace tair{

   remote_sync_manager::remote_sync_manager(LocalQueue::serialize_func_ serializer,
           LocalQueue::deserialize_func_ deserializer)
       : local_queue(serializer, deserializer)
   {
       m_inited = false;
   }

   remote_sync_manager::~remote_sync_manager()
   {
       this->stop();
       this->wait();
   }

   void remote_sync_manager::clean()
   {
       log_debug("remote_sync_manager::clean()");
       return;
   }

   bool remote_sync_manager::init()
   {
       log_error("sync plugin can't init without para");
       return false;
   }

   int remote_sync_manager::get_hook_point() {
       return plugin::HOOK_POINT_RESPONSE;
   }

   int remote_sync_manager::get_property() {
       return 0;
   }

   int remote_sync_manager::get_plugin_type() {
       return plugin::PLUGIN_TYPE_REMOTE_SYNC;
   }

   extern "C" remote_sync_manager* create()
   {
       return new remote_sync_manager(HashPacketPair::hp_serialize_func_,
             HashPacketPair::hp_deserialize_func_);
   }

   extern "C" void destroy (remote_sync_manager* p)
   {
       delete p;
   }

   void remote_sync_manager::do_response(int ret, uint32_t hashcode, base_packet *packet, void* exv) {
       if(!m_inited) {
           log_error("remote sync enabled but not inited.");
           return;
       }
       if (packet == NULL) {
           return;
       }

       HashPacketPair* hpPair = new HashPacketPair(ret, hashcode, packet);

       bool isok = false;
       int trytime = 0;
       while(isok == false && trytime < 3) {
           isok = local_queue.push(hpPair);
           trytime++;
       }
       if (isok == false) {
           bool append_ok = log_push_failed->append(ret, 0, packet->getPCode(), try_get_key(packet));
           if (append_ok == false) {
               log_error("log_writer append failed");
           }
       }
   }

   bool remote_sync_manager::init(const std::string& para)
   {
       if(m_inited) {
           return m_inited;
       }

       //remote sync manager's queue push failed
       log_push_failed = new BinLogWriter("/rsm_qpf", true);
       if (log_push_failed == NULL) {
           return false;
       }

       log_send_failed = new BinLogWriter("/rsm_sf", false);
       if (log_send_failed == NULL) {
           return false;
       }

       if(!local_queue.isInit()) {
           return false;
       }

       char *base_home = getenv("TAIR_HOME");
       if(base_home) {
           strncpy(m_base_home,base_home,NAME_MAX);  //base_home never null
       } else {
#ifdef TAIR_DEBUG
           strcpy(m_base_home,"./");
#else
           strncpy(m_base_home,"/home/admin/tair_bin/",NAME_MAX);  //base_home never null
#endif
       }

       //{10.232.4.25,10.232.4.26,group_dup}
       //init paras
       if(para.size() <= 0 || !_remote_conf.init(para.c_str())) {
           log_error("parse sync config error,para=%s",para.c_str());
           return false;
       }
       log_info("remote plugin start init with para=%s",para.c_str());

       //check para.
       if(strlen(_remote_conf.master_ip) <= 0 || strlen(_remote_conf.group_name) <= 0) {
           log_error("sync config not master or group");
           return false;
       }

       _remote_tairclient.set_timeout(2000);
       if(!_remote_tairclient.startup(_remote_conf.master_ip, _remote_conf.slave_ip,
                   _remote_conf.group_name)) {
           log_warn("remote:%s,%s can not connect,wait reconnect",
                   _remote_conf.master_ip, _remote_conf.group_name);
       }
       //we just use only one thread to send packet, so we can use local queue only with push lock
       setThreadCount(1);
       this->start();

       m_inited = true;

       log_info("remote plugin finish init with para=%s",para.c_str());
       return true;
   }

   bool remote_sync_manager::init_tair_client(tair_client_impl& _client,
           struct sync_conf &_conf) {
       if(!_client.startup(_conf.master_ip, _conf.slave_ip, _conf.group_name)) {
           log_error("remote:%s,%s can not connect ",
                   _conf.master_ip, _conf.group_name);
           return false;
       }
       return true;
   }

   void remote_sync_manager::run(tbsys::CThread *thread, void *arg)
   {
       UNUSED(thread);
       while (!_stop) {
           handle_send_queue();
       }
   }

   void remote_sync_manager::handle_send_queue()
   {
       HashPacketPair* hpPair = NULL;
       int ret = local_queue.pop((BaseQueueNode**)&hpPair);
       if(!ret) {
           if (hpPair != NULL) {
               base_packet* packet = hpPair->get_packet();
               if (packet) {
                   delete packet;
               }
           }
           usleep(1);
           return;
       }

       ret = do_send_packet(hpPair->get_hashcode(), hpPair->get_packet());

       if (ret != hpPair->get_ret()) {
           bool append_ok = log_send_failed->append(hpPair->get_ret(), ret,
                   hpPair->get_packet()->getPCode(), try_get_key(hpPair->get_packet()));
           if (append_ok == false) {
               log_error("log_writer append failed");
           }
       }
       delete hpPair;
   }

   int remote_sync_manager::do_send_packet(const uint32_t hashcode, base_packet* packet)
   {
       if(!_remote_tairclient.isinited() && !init_tair_client(_remote_tairclient, _remote_conf))
       {
           return  TAIR_RETURN_REMOTE_NOLOCAL;
       }

       return _remote_tairclient.send_packet(hashcode, packet);
   }


//try trun base packet to child class, then get key, if have no key, or can't translate,
//then retrun null
   const data_entry* remote_sync_manager::try_get_key(base_packet *bpacket)
   {
       switch(bpacket->getPCode()) {
           case TAIR_REQ_PUT_PACKET:
               return &(((request_put *)(bpacket))->key);
           case TAIR_REQ_PUTNX_PACKET:
               return &(((request_putnx *)(bpacket))->key);
           case TAIR_REQ_REMOVE_PACKET:
               return (((request_remove *)(bpacket))->key);
           case TAIR_REQ_INCDEC_PACKET:
               return &(((request_inc_dec *)(bpacket))->key);
           case TAIR_REQ_GETSET_PACKET:
               return &(((request_getset *)(bpacket))->key);
           case TAIR_REQ_EXPIRE_PACKET:
               return &(((request_expire *)(bpacket))->key);
           case TAIR_REQ_EXPIREAT_PACKET:
               return &(((request_expireat *)(bpacket))->key);
           case TAIR_REQ_PERSIST_PACKET:
               return &(((request_persist *)(bpacket))->key);

               //list
           case TAIR_REQ_LPOP_PACKET:
           case TAIR_REQ_RPOP_PACKET:
               return &(((request_lrpop *)(bpacket))->key);
           case TAIR_REQ_LPUSH_PACKET:
           case TAIR_REQ_RPUSH_PACKET:
           case TAIR_REQ_LPUSHX_PACKET:
           case TAIR_REQ_RPUSHX_PACKET:
               return &(((request_lrpush *)(bpacket))->key);
           case TAIR_REQ_LPUSH_LIMIT_PACKET:
           case TAIR_REQ_RPUSH_LIMIT_PACKET:
           case TAIR_REQ_LPUSHX_LIMIT_PACKET:
           case TAIR_REQ_RPUSHX_LIMIT_PACKET:
               return &(((request_lrpush_limit *)(bpacket))->key);
           case TAIR_REQ_LTRIM_PACKET:
               return &(((request_ltrim *)(bpacket))->key);
           case TAIR_REQ_LREM_PACKET:
               return &(((request_lrem *)(bpacket))->key);

               //hset
           case TAIR_REQ_HINCRBY_PACKET:
               return &(((request_hincrby *)(bpacket))->key);
           case TAIR_REQ_HMSET_PACKET:
               return &(((request_hmset *)(bpacket))->key);
           case TAIR_REQ_HSET_PACKET:
               return &(((request_hset *)(bpacket))->key);
           case TAIR_REQ_HSETNX_PACKET:
               return &(((request_hsetnx *)(bpacket))->key);
           case TAIR_REQ_HDEL_PACKET:
               return &(((request_hdel *)(bpacket))->key);
               //set
           case TAIR_REQ_SADD_PACKET:
               return &(((request_sadd *)(bpacket))->key);
           case TAIR_REQ_SPOP_PACKET:
               return &(((request_spop *)(bpacket))->key);
           case TAIR_REQ_SREM_PACKET:
               return &(((request_srem *)(bpacket))->key);
           case TAIR_REQ_SADDMULTI_PACKET:  //used for duplicate so this only have one key
               return ((request_sadd_multi *)(bpacket))->get_random_key();
           case TAIR_REQ_SREMMULTI_PACKET:
               return ((request_srem_multi *)(bpacket))->get_random_key();
               //zset
           case TAIR_REQ_ZADD_PACKET:
               return &(((request_zadd *)(bpacket))->key);
           case TAIR_REQ_ZINCRBY_PACKET:
               return &(((request_zincrby *)(bpacket))->key);
           case TAIR_REQ_ZREM_PACKET:
               return &(((request_zrem *)(bpacket))->key);
           case TAIR_REQ_ZREMRANGEBYRANK_PACKET:
               return &(((request_zremrangebyrank *)(bpacket))->key);
           case TAIR_REQ_ZREMRANGEBYSCORE_PACKET:
               return &(((request_zremrangebyrank *)(bpacket))->key);
               //common
           default:
               log_error("try get key error: pcode = %d", bpacket->getPCode());
               return NULL;
       }
   }
}

