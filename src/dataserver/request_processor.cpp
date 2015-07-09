/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * request dispatcher impl
 *
 * Version: $Id: request_processor.cpp 603 2012-03-08 03:28:19Z choutian.xmm@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#include "request_processor.hpp"

#define is_need_duplicate(_ret) \
    (tair_mgr->plugins_manager.exist_type_response_plugins(plugin::PLUGIN_TYPE_REMOTE_SYNC) && \
    ((_ret) == TAIR_RETURN_SUCCESS || \
     (_ret) == TAIR_RETURN_ALREADY_EXIST || \
     (_ret) == TAIR_RETURN_DATA_LEN_LIMITED))


#define PROC_BEFORE(type) type* resp = NULL;					\
	 int rc = process_before(this, tair_mgr,  request, resp);	\
	 if (rc != TAIR_RETURN_FAILED)								\
		 return rc

#define PROC_AFTER	process_after(heart_beat, resp, request, rc, send_return)

namespace tair {

   template<class T>
   bool request_processor::do_duplicate(int rc, T* & request) {
       if (is_need_duplicate(rc)) {
           uint32_t hashcode = util::string_util::mur_mur_hash(request->key.get_data(),
                   request->key.get_size());
           base_packet* packet = new T((*request));
           if (packet == NULL) {
               log_error("duplicate new cpacket failed");
               return false;
           }
           plugin::plugins_root* plugin_root = NULL;
           tair_mgr->plugins_manager.do_response_plugins(plugin::PLUGIN_TYPE_REMOTE_SYNC,
                   rc, hashcode, packet, plugin_root);
       }
       return true;
   }

   template<class T, class P>
   int request_processor::process_before(request_processor *processor,
		   tair_manager *tair_mgr, P* & request, T* & resp)
   {
     	int rc = TAIR_RETURN_FAILED;
    	if (tair_mgr->is_working () == false)
		{
   		   return TAIR_RETURN_SERVER_CAN_NOT_WORK;
		}

        uint64_t target_server_id = 0;
        if (request->hash_code != -1 &&
                tair_mgr->should_proxy(request->hash_code, target_server_id)) {
           return TAIR_RETURN_SHOULD_PROXY;
        }

 		resp = new T();

		return rc;
   }

   template<class T, class P>
   int request_processor::process_after(heartbeat_thread *heart_beat,
		   T* & resp, P* & request, int& rc, bool& send_return)
   {
     resp->setChannelId(request->getChannelId());
     resp->set_meta(heart_beat->get_client_version(), rc);
     resp->set_version((request->key).get_version());
     if (request->get_connection()->postPacket(resp) == false)
     {
       delete resp;
       resp = 0;
     }
     send_return = false;
     return rc;
   }

   request_processor::request_processor(tair_manager *tair_mgr, heartbeat_thread *heart_beat,
		   tbnet::ConnectionManager *connection_mgr)
   {
     this->tair_mgr = tair_mgr;
     this->heart_beat = heart_beat;
     this->connection_mgr = connection_mgr;
   }

   request_processor::~request_processor()
   {
   }

   int request_processor::process (request_info * request, bool & send_return)
   {
     int rc = TAIR_RETURN_FAILED;
     if (tair_mgr->is_working () == false)
     {
       return TAIR_RETURN_SERVER_CAN_NOT_WORK;
     }

     response_info *resp = new response_info();
     rc = tair_mgr->info (resp->info);
     resp->setChannelId(request->getChannelId());
     resp->set_meta(heart_beat->get_client_version(), rc);
     if (request->get_connection()->postPacket(resp) == false)
     {
       delete resp;
       resp = 0;
     }
     send_return = false;
     return rc;
   }

   int request_processor::process (request_ttl * request, bool & send_return)
   {
     PROC_BEFORE(response_ttl);
	 rc = tair_mgr->ttl(request->area, request->key, &(resp->retnum));
     return PROC_AFTER;
   }

   int request_processor::process (request_type * request, bool & send_return)
   {
     PROC_BEFORE(response_type);
	 rc = tair_mgr->type(request->area, request->key, &(resp->retnum));
   	 return PROC_AFTER;
   }

   int request_processor::process (request_exists * request, bool & send_return)
   {
     int rc = TAIR_RETURN_FAILED;
     if (tair_mgr->is_working () == false) {
   	   return TAIR_RETURN_SERVER_CAN_NOT_WORK;
     }

     uint64_t target_server_id = 0;
     if (request->hash_code != -1 &&
             tair_mgr->should_proxy(request->hash_code, target_server_id)) {
       return TAIR_RETURN_SHOULD_PROXY;
     }

	 rc = tair_mgr->exists(request->area, request->key);
     response_return *resp = new response_return(request->getChannelId(), rc, "");
     if (request->get_connection()->postPacket(resp) == false)
     {
       delete resp;
       resp = 0;
     }
     send_return = false;
     return rc;
   }

   int request_processor::process (request_lazy_remove_area * request, bool & send_return)
   {
     int rc = TAIR_RETURN_FAILED;
     if (tair_mgr->is_working () == false)
   	   return TAIR_RETURN_SERVER_CAN_NOT_WORK;

     rc = tair_mgr->lazyclear(request->area);
     //remove area don't support duplicate,it will trigger N*M request to other cluster
     response_return *resp = new response_return(request->getChannelId(), rc, "");
     if (request->get_connection()->postPacket(resp) == false)
     {
       delete resp;
       resp = 0;
     }
     send_return = false;
     return rc;
   }

   int request_processor::process (request_dump_area * request, bool & send_return)
   {
     int rc = TAIR_RETURN_FAILED;
     if (tair_mgr->is_working () == false)
   	   return TAIR_RETURN_SERVER_CAN_NOT_WORK;

     rc = tair_mgr->dumparea(request->area);
     //remove area don't support duplicate,it will trigger N*M request to other cluster
     response_return *resp = new response_return(request->getChannelId(), rc, "");
     if (request->get_connection()->postPacket(resp) == false)
     {
       delete resp;
       resp = 0;
     }
     send_return = false;
     return rc;
   }

   int request_processor::process (request_load_area * request, bool & send_return)
   {
     int rc = TAIR_RETURN_FAILED;
     if (tair_mgr->is_working () == false)
   	   return TAIR_RETURN_SERVER_CAN_NOT_WORK;

     rc = tair_mgr->loadarea(request->area);
     //remove area don't support duplicate,it will trigger N*M request to other cluster
     response_return *resp = new response_return(request->getChannelId(), rc, "");
     if (request->get_connection()->postPacket(resp) == false)
     {
       delete resp;
       resp = 0;
     }
     send_return = false;
     return rc;
   }

   int request_processor::process (request_addfilter * request, bool & send_return)
   {
     int rc = TAIR_RETURN_FAILED;
     if (tair_mgr->is_working () == false)
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;

     rc = tair_mgr->addfilter(request->area, request->key, request->field, request->value);
     response_return *resp = new response_return(request->getChannelId(), rc, "");
     if (request->get_connection()->postPacket(resp) == false)
     {
       delete resp;
       resp = 0;
     }
     send_return = false;
     return rc;
   }

   int request_processor::process (request_removefilter * request, bool & send_return)
   {
     int rc = TAIR_RETURN_FAILED;
     if (tair_mgr->is_working () == false)
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;

     rc = tair_mgr->removefilter(request->area, request->key, request->field, request->value);
     response_return *resp = new response_return(request->getChannelId(), rc, "");
     if (request->get_connection()->postPacket(resp) == false)
     {
       delete resp;
       resp = 0;
     }
     send_return = false;
     return rc;
   }

   int request_processor::process (request_lindex * request,
 				  bool & send_return)
   {
     PROC_BEFORE(response_lindex);
	 rc = tair_mgr->lindex(request->area, request->key, request->index, resp->value);
	 return PROC_AFTER;
   }

   int request_processor::process (request_ltrim * request, bool & send_return)
   {
	 PROC_BEFORE(response_ltrim);
	 rc = tair_mgr->ltrim (request->area, request->key,
 			  request->start, request->end, request->version, request->expire);

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process (request_llen * request, bool & send_return)
   {
	 PROC_BEFORE(response_llen);
	 rc = tair_mgr->llen(request->area, request->key, &(resp->retnum));
	 return PROC_AFTER;
   }

   int request_processor::process (request_lrem * request, bool & send_return)
   {
     PROC_BEFORE(response_lrem);
	 rc = tair_mgr->lrem (request->area, request->key, request->count,
 			 request->value, &(resp->retnum), request->version, request->expire);

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process (request_lrange * request,
 				  bool & send_return)
   {
     PROC_BEFORE(response_lrange);
	 rc = tair_mgr->lrange (request->area, request->key,
 			   request->start, request->end, resp->values);
	 return PROC_AFTER;
   }

   int request_processor::process (request_expire * request,
 				  bool & send_return)
   {
	 PROC_BEFORE(response_expire);
     rc = tair_mgr->expire(request->area, request->key, request->expiretime);
	 return PROC_AFTER;
   }


   int request_processor::process (request_expireat * request,
 				  bool & send_return)
   {
     PROC_BEFORE(response_expireat);
	 rc = tair_mgr->expireat(request->area, request->key, request->expiretime);
	 return PROC_AFTER;
   }

   int request_processor::process (request_persist * request,
 				  bool & send_return)
   {
     PROC_BEFORE(response_persist);
	 rc = tair_mgr->persist(request->area, request->key);
	 return PROC_AFTER;
   }


   /* hset */
   int request_processor::process (request_hexists * request, bool & send_return)
   {
     int rc = TAIR_RETURN_FAILED;
     if (tair_mgr->is_working () == false) {
   	   return TAIR_RETURN_SERVER_CAN_NOT_WORK;
     }

     uint64_t target_server_id = 0;
     if (request->hash_code != -1 &&
             tair_mgr->should_proxy(request->hash_code, target_server_id)) {
       return TAIR_RETURN_SHOULD_PROXY;
     }

     rc = tair_mgr->hexists(request->area, request->key, request->field);
     response_return *resp = new response_return(request->getChannelId(), rc, "");
     if (request->get_connection()->postPacket(resp) == false)
     {
       delete resp;
       resp = 0;
     }
     send_return = false;
     return rc;
   }

   int request_processor::process (request_hgetall * request,
 				  bool & send_return)
   {
	 PROC_BEFORE(response_hgetall);
     rc = tair_mgr->hgetall(request->area, request->key, resp->values);
	 return PROC_AFTER;
   }

   int request_processor::process (request_hincrby * request,
 				  bool & send_return)
   {
	 PROC_BEFORE(response_hincrby);
     rc = tair_mgr->hincrby (request->area, request->key, request->field,
 			    request->value, &(resp->retnum), request->version, request->expire);

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process (request_hmset * request, bool & send_return)
   {
	 PROC_BEFORE(response_hmset);
     rc = tair_mgr->hmset(request->area, request->key, request->field_values,
             &(resp->retvalue), request->version, request->expire);

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process (request_hset * request, bool & send_return)
   {
	 PROC_BEFORE(response_hset);
     rc = tair_mgr->hset (request->area, request->key, request->field,
 			 request->value, request->version, request->expire);

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process (request_hsetnx * request,
 				  bool & send_return)
   {
	 PROC_BEFORE(response_hsetnx);
     rc = tair_mgr->hsetnx (request->area, request->key, request->field,
 			   request->value, request->version, request->expire);

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process (request_hget * request, bool & send_return)
   {
	 PROC_BEFORE(response_hget);
     rc = tair_mgr->hget(request->area, request->key, request->field, resp->value);
	 return PROC_AFTER;
   }

   int request_processor::process (request_hmget * request, bool & send_return)
   {
	 PROC_BEFORE(response_hmget);
     rc = tair_mgr->hmget(request->area, request->key, request->fields,
 			  resp->values);
	 return PROC_AFTER;
   }

   int request_processor::process (request_hkeys * request, bool & send_return)
   {
	 PROC_BEFORE(response_hkeys);
     rc = tair_mgr->hkeys (request->area, request->key, resp->keys);
	 return PROC_AFTER;
   }

   int request_processor::process (request_hvals * request, bool & send_return)
   {
	 PROC_BEFORE(response_hvals);
     rc = tair_mgr->hvals (request->area, request->key, resp->values);
	 return PROC_AFTER;
   }

   int request_processor::process (request_hdel * request, bool & send_return)
   {
	 PROC_BEFORE(response_hdel);
     rc = tair_mgr->hdel (request->area, request->key, request->field,
             request->version, request->expire);

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process (request_hlen * request, bool & send_return)
   {
	 PROC_BEFORE(response_hlen);
	 rc = tair_mgr->hlen(request->area, request->key, &(resp->retnum));
	 return PROC_AFTER;
   }

   /* set */
   int request_processor::process (request_scard * request, bool & send_return)
   {
	 PROC_BEFORE(response_scard);
     rc = tair_mgr->scard (request->area, request->key, &(resp->retnum));
	 return PROC_AFTER;
   }

   int request_processor::process (request_smembers * request,
 				  bool & send_return)
   {
	 PROC_BEFORE(response_smembers);
     rc = tair_mgr->smembers (request->area, request->key, resp->values);
	 return PROC_AFTER;
   }

   int request_processor::process (request_sadd * request, bool & send_return)
   {
	 PROC_BEFORE(response_sadd);
     rc = tair_mgr->sadd(request->area, request->key, request->value,
             request->version, request->expire);

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process (request_spop * request, bool & send_return)
   {
	 PROC_BEFORE(response_spop);
     rc = tair_mgr->spop(request->area, request->key, resp->value,
             request->version, request->expire);

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process (request_srem * request, bool &send_return)
   {
	 PROC_BEFORE(response_srem);
	 rc = tair_mgr->srem (request->area, request->key, request->value,
			 request->version, request->expire);

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process (request_sadd_multi * request,
           bool & send_return) {
     PROC_BEFORE(response_sadd);
     bool failed = false;
     map<data_entry*, vector<data_entry *>*> &mapper = request->keys_values_map;
     map<data_entry*, vector<data_entry *>*>::iterator iter;
     for(iter = mapper.begin(); iter != mapper.end(); iter++) {
       data_entry* key = iter->first;
       vector<data_entry*>* values = iter->second;
       if (values == NULL) {
         continue;
       }
       vector<data_entry*>::iterator val_iter;
       for(val_iter = values->begin(); val_iter != values->end(); val_iter++) {
         data_entry* value = *val_iter;
         rc = tair_mgr->sadd(request->area, (*key), (*value), 0, request->expire);
         if (rc != TAIR_RETURN_SUCCESS && rc != TAIR_RETURN_ALREADY_EXIST) {
           failed = true;
           break;
         }
       }
       if (failed == true) {
         break;
       }

       if (is_need_duplicate(rc)) {
         uint32_t hashcode = util::string_util::mur_mur_hash(key->get_data(),
                 key->get_size());
         //it will copy key and values
         request_sadd_multi* packet = new request_sadd_multi(request->area, request->expire, key, values);
         if (packet == NULL) {
             log_error("msadd duplicate new msadd packet failed");
             continue;
         }

         plugin::plugins_root* plugin_root = NULL;
         tair_mgr->plugins_manager.do_response_plugins(plugin::PLUGIN_TYPE_REMOTE_SYNC,
                 rc, hashcode, packet, plugin_root);
       }
     }

     if (rc != TAIR_RETURN_SUCCESS)
     {
       log_debug ("rc = %d not TAIR_RETURN_SUCCESS", rc);
     }
     resp->setChannelId(request->getChannelId());
     resp->set_meta(heart_beat->get_client_version(), rc);
     if (request->get_connection()->postPacket(resp) == false)
     {
       delete resp;
       resp = 0;
     }
     send_return = false;
     return rc;
   }

   int request_processor::process (request_srem_multi * request,
           bool & send_return) {
     PROC_BEFORE(response_srem);
     bool failed = false;
     map<data_entry*, vector<data_entry *>*> &mapper = request->keys_values_map;
     map<data_entry*, vector<data_entry *>*>::iterator iter;
     for(iter = mapper.begin(); iter != mapper.end(); iter++) {
       data_entry* key = iter->first;
       vector<data_entry*>* values = iter->second;
       if (values == NULL) {
         continue;
       }
       vector<data_entry*>::iterator val_iter;
       for(val_iter = values->begin(); val_iter != values->end(); val_iter++) {
         data_entry* value = *val_iter;
         rc = tair_mgr->srem(request->area, (*key), (*value), 0, request->expire);
         if (rc != TAIR_RETURN_SUCCESS && rc != TAIR_RETURN_DATA_NOT_EXIST) {
           failed = true;
           break;
         }
       }
       if (failed == true) {
         break;
       }

       if (is_need_duplicate(rc)) {
         uint32_t hashcode = util::string_util::mur_mur_hash(key->get_data(),
                 key->get_size());
         //it will copy key and values
         request_srem_multi* packet = new request_srem_multi(request->area, request->expire, key, values);
         if (packet == NULL) {
             log_error("msrem duplicate new msrem packet failed");
             continue;
         }

         plugin::plugins_root* plugin_root = NULL;
         tair_mgr->plugins_manager.do_response_plugins(plugin::PLUGIN_TYPE_REMOTE_SYNC,
                 rc, hashcode, packet, plugin_root);
       }
     }

     if (rc != TAIR_RETURN_SUCCESS)
     {
       log_debug ("rc = %d not TAIR_RETURN_SUCCESS", rc);
     }
     resp->setChannelId(request->getChannelId());
     resp->set_meta(heart_beat->get_client_version(), rc);
     if (request->get_connection()->postPacket(resp) == false)
     {
       delete resp;
       resp = 0;
     }
     send_return = false;
     return rc;
   }

   int request_processor::process (request_smembers_multi * request,
           bool & send_return) {
     PROC_BEFORE(response_smembers_multi);

     vector<data_entry*>::iterator iter;
     vector<data_entry*> &keys = request->keys;
     for(iter = keys.begin(); iter != keys.end(); iter++) {
       data_entry* key = *iter;
       vector<data_entry*>* values = new vector<data_entry*>();
       if (values == NULL) {
           rc = TAIR_RETURN_FAILED;
           break;
       }
       rc = tair_mgr->smembers(request->area, (*key), (*values));
       resp->add_version_values_pair(key->get_version(), values);
     }

     if (rc != TAIR_RETURN_SUCCESS)
     {
       log_debug ("rc not TAIR_RETURN_SUCCESS");
     }
     resp->setChannelId(request->getChannelId());
     resp->set_meta(heart_beat->get_client_version(), rc);
     if (request->get_connection()->postPacket(resp) == false)
     {
       delete resp;
       resp = 0;
     }
     send_return = false;
     return rc;
   }

   /* zset */
   int request_processor::process (request_zscore * request,
 				  bool & send_return)
   {
	 PROC_BEFORE(response_zscore);
     rc = tair_mgr->zscore (request->area, request->key, request->value,
 			   &(resp->score));
	 return PROC_AFTER;
   }

   int request_processor::process (request_zrange * request,
 				  bool & send_return)
   {
	 if (request->withscore == 0) {
        PROC_BEFORE(response_zrange);
        rc = tair_mgr->zrange(request->area, request->key,
 			   request->start, request->end, resp->values, resp->scores, 0);
	    return PROC_AFTER;
     } else {
        PROC_BEFORE(response_zrangewithscore);
        rc = tair_mgr->zrange(request->area, request->key,
 			   request->start, request->end, resp->values, resp->scores, 1);
	    return PROC_AFTER;
     }
   }

   int request_processor::process (request_zrevrange * request,
 				  bool & send_return)
   {
	 if (request->withscore == 0) {
       PROC_BEFORE(response_zrevrange);
       rc = tair_mgr->zrevrange (request->area, request->key,
 	        request->start, request->end, resp->values, resp->scores, 0);
	   return PROC_AFTER;
     } else {
       PROC_BEFORE(response_zrevrangewithscore);
       rc = tair_mgr->zrevrange (request->area, request->key,
 			request->start, request->end, resp->values, resp->scores, 1);
	   return PROC_AFTER;
     }
   }

   int request_processor::process (request_zrangebyscore * request,
 				  bool & send_return)
   {
	 PROC_BEFORE(response_zrangebyscore);
     vector<double> scores;
     rc = tair_mgr->zrangebyscore (request->area, request->key,
 				  request->start, request->end, resp->values, scores, -1, 0);
	 return PROC_AFTER;
   }

   int request_processor::process (request_generic_zrangebyscore * request,
           bool & send_return)
   {
     PROC_BEFORE(response_generic_zrangebyscore);
     if (request->reverse) {
         rc = tair_mgr->zrangebyscore(request->area, request->key,
                 request->start, request->end, resp->values, resp->scores, request->limit, request->withscore);
     } else {
         rc = tair_mgr->zrevrangebyscore(request->area, request->key,
                 request->start, request->end, resp->values, resp->scores, request->limit, request->withscore);
     }

     return PROC_AFTER;
   }

   int request_processor::process (request_zadd * request, bool & send_return)
   {
     PROC_BEFORE(response_zadd);
     rc = tair_mgr->zadd (request->area, request->key, request->score,
 		      request->value, request->version, request->expire);

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process (request_zrank * request, bool & send_return)
   {
	 PROC_BEFORE(response_zrank);
     rc = tair_mgr->zrank (request->area, request->key,
 			  request->value, &(resp->retnum));
	 return PROC_AFTER;
   }

   int request_processor::process (request_zrevrank * request, bool & send_return)
   {
	 PROC_BEFORE(response_zrevrank);
     rc = tair_mgr->zrevrank (request->area, request->key,
 			  request->value, &(resp->retnum));
	 return PROC_AFTER;
   }

   int request_processor::process (request_zcount * request, bool & send_return)
   {
	 PROC_BEFORE(response_zcount);
     rc = tair_mgr->zcount (request->area, request->key,
 				  request->start, request->end, &(resp->retnum));
	 return PROC_AFTER;
   }

   int request_processor::process (request_zincrby * request, bool & send_return)
   {
	 PROC_BEFORE(response_zincrby);
     rc = tair_mgr->zincrby (request->area, request->key, request->value,
 			    request->addscore, &(resp->retnum), request->version, request->expire);

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process (request_zcard * request, bool & send_return)
   {
	 PROC_BEFORE(response_zcard);
     rc = tair_mgr->zcard (request->area, request->key, &(resp->retnum));
	 return PROC_AFTER;
   }

   int request_processor::process (request_zrem * request, bool &send_return)
   {
	 PROC_BEFORE(response_zrem);
	 rc = tair_mgr->zrem (request->area, request->key, request->value,
			 request->version, request->expire);

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process (request_zremrangebyrank * request, bool &send_return)
   {
	 PROC_BEFORE(response_zremrangebyrank);
	 rc = tair_mgr->zremrangebyrank (request->area, request->key, request->start,
			request->end, request->version, request->expire, &(resp->retnum));

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process (request_zremrangebyscore * request, bool &send_return)
   {
	 PROC_BEFORE(response_zremrangebyscore);
	 rc = tair_mgr->zremrangebyscore (request->area, request->key, request->start,
			request->end, request->version, request->expire, &(resp->retnum));

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process (request_lrpush * request,
 				  bool & send_return)
   {
     int pcode = request->getPCode ();

     if (tair_mgr->is_working () == false) {
       return TAIR_RETURN_SERVER_CAN_NOT_WORK;
     }

     uint64_t target_server_id = 0;
     if (request->hash_code != -1 &&
             tair_mgr->should_proxy(request->hash_code, target_server_id)) {
         return TAIR_RETURN_SHOULD_PROXY;
     }

     response_lrpush *resp = NULL;

     if (pcode == TAIR_REQ_LPUSH_PACKET)
       resp = new response_lrpush (IS_NOT_EXIST, IS_LEFT);
     else if (pcode == TAIR_REQ_RPUSH_PACKET)
       resp = new response_lrpush (IS_NOT_EXIST, IS_RIGHT);
     else if (pcode == TAIR_REQ_LPUSHX_PACKET)
       resp = new response_lrpush (IS_EXIST, IS_LEFT);
     else
       resp = new response_lrpush (IS_EXIST, IS_RIGHT);

     int rc = tair_mgr->lrpush (request->area, request->key, request->values,
 			   &(resp->pushed_num), &(resp->list_len), pcode, 0,
                request->version, request->expire);

     do_duplicate(rc, request);

     resp->setChannelId (request->getChannelId ());
     resp->set_meta (heart_beat->get_client_version (), rc);
     if (request->get_connection ()->postPacket (resp) == false)
     {
       delete resp;
       resp = 0;
     }

     send_return = false;
     return rc;
   }

   int request_processor::process (request_lrpush_limit * request, bool & send_return)
   {
     int pcode = request->getPCode ();

     if (tair_mgr->is_working () == false) {
       return TAIR_RETURN_SERVER_CAN_NOT_WORK;
     }

     uint64_t target_server_id = 0;
     if (request->hash_code &&
             tair_mgr->should_proxy(request->hash_code, target_server_id)) {
         return TAIR_RETURN_SHOULD_PROXY;
     }

     response_lrpush *resp = NULL;

     if (pcode == TAIR_REQ_LPUSH_LIMIT_PACKET)
       resp = new response_lrpush (IS_NOT_EXIST, IS_LEFT);
     else if (pcode == TAIR_REQ_RPUSH_LIMIT_PACKET)
       resp = new response_lrpush (IS_NOT_EXIST, IS_RIGHT);
     else if (pcode == TAIR_REQ_LPUSHX_LIMIT_PACKET)
       resp = new response_lrpush (IS_EXIST, IS_LEFT);
     else
       resp = new response_lrpush (IS_EXIST, IS_RIGHT);

     int rc = tair_mgr->lrpush (request->area, request->key, request->values,
 			   &(resp->pushed_num), &(resp->list_len), pcode, request->max_count,
                request->version, request->expire);

     do_duplicate(rc, request);

     resp->setChannelId (request->getChannelId ());
     resp->set_meta (heart_beat->get_client_version (), rc);
     if (request->get_connection ()->postPacket (resp) == false)
     {
       delete resp;
       resp = 0;
     }

     send_return = false;
     return rc;
   }

   int request_processor::process (request_lrpop * request, bool & send_return)
   {
     int pcode = request->getPCode ();

     if (tair_mgr->is_working () == false) {
       return TAIR_RETURN_SERVER_CAN_NOT_WORK;
     }

     uint64_t target_server_id = 0;
     if (request->hash_code != -1 &&
             tair_mgr->should_proxy(request->hash_code, target_server_id)) {
         return TAIR_RETURN_SHOULD_PROXY;
     }

     response_lrpop *resp = NULL;
     if (pcode == TAIR_REQ_LPOP_PACKET)
       resp = new response_lrpop (IS_LEFT);
     else
       resp = new response_lrpop (IS_RIGHT);

     int rc = tair_mgr->lrpop (request->area, request->key, request->count,
 		       resp->values, pcode, request->version, request->expire);

     do_duplicate(rc, request);

     resp->setChannelId(request->getChannelId ());
     resp->set_meta(heart_beat->get_client_version (), rc);
     resp->set_version((request->key).get_version());
     if (request->get_connection()->postPacket(resp) == false)
     {
       delete resp;
       resp = 0;
     }

     send_return = false;
     return rc;
   }

   int request_processor::process(request_putnx *request, bool &send_return)
   {
      int rc = 0;

      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      uint64_t target_server_id = 0;
      if (request->hash_code != -1 &&
              tair_mgr->should_proxy(request->hash_code, target_server_id)) {
         return TAIR_RETURN_SHOULD_PROXY;
      }

      request->key.server_flag = request->server_flag;
      request->key.data_meta.version = request->version;
      send_return = true;
      rc = tair_mgr->putnx(request->area, request->key, request->data, request->expired);

      do_duplicate(rc, request);

      return rc;
   }

   int request_processor::process(request_put *request, bool &send_return)
   {
      int rc = 0;

      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      uint64_t target_server_id = 0;
      if (request->hash_code != -1 &&
              tair_mgr->should_proxy(request->hash_code, target_server_id)) {
         return TAIR_RETURN_SHOULD_PROXY;
      }

      request->key.server_flag = request->server_flag;
      request->key.data_meta.version = request->version;
      send_return = true;

      rc = tair_mgr->put(request->area, request->key, request->data, request->expired);

      do_duplicate(rc, request);

      return rc;
   }

   int request_processor::process(request_get *request, bool &send_return)
   {
      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      set<data_entry*, data_entry_comparator>::iterator it;
      data_entry *data = NULL;
      response_get *resp = new response_get();

      int rc = TAIR_RETURN_FAILED;

      if (request->key_list != NULL) {
         uint32_t count = 0;
         for (it = request->key_list->begin(); it != request->key_list->end(); ++it) {
            if (data == NULL) {
               data = new data_entry();
            }
            data_entry *key = (*it);
            int rev = tair_mgr->get(request->area, *key, *data);
            if (rev != TAIR_RETURN_SUCCESS) {
               continue;
            }
            ++count;
            resp->add_key_data(new data_entry(*key), data);
            data = NULL;
         }
         if (data != NULL) {
            delete data;
            data = NULL;
         }
         if(count == request->key_count) {
            rc = TAIR_RETURN_SUCCESS;
         }else{
            rc = TAIR_RETURN_PARTIAL_SUCCESS;
         }
      } else if (request->key != NULL) {
         data = new data_entry();
         rc = tair_mgr->get(request->area, *(request->key), *data);
         if (rc == TAIR_RETURN_SUCCESS) {
            resp->add_key_data(request->key, data);
            request->key = NULL;
         } else {
            delete data;
         }
      }

      resp->config_version = heart_beat->get_client_version();
      resp->setChannelId(request->getChannelId());
      resp->set_code(rc);
      if(request->get_connection()->postPacket(resp) == false) {
         delete resp;
         resp = 0;
      }

      send_return = false;
      return rc;
   }

   int request_processor::process (request_getset * request, bool & send_return)
   {
	 PROC_BEFORE(response_getset);
     rc = tair_mgr->getset (request->area, request->key, request->value,
 			 resp->value, request->version, request->expire);

     do_duplicate(rc, request);

	 return PROC_AFTER;
   }

   int request_processor::process(request_remove *request, bool &send_return)
   {
      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      int rc = TAIR_RETURN_SUCCESS;
      set<data_entry*, data_entry_comparator>::iterator it;
      send_return = true;

      if (request->key_list != NULL) {
         for (it = request->key_list->begin(); it != request->key_list->end(); ++it) {
            data_entry *key = (*it);

            int rev = tair_mgr->remove(request->area, *key);

            if (rev != TAIR_RETURN_SUCCESS && rev != TAIR_RETURN_DATA_NOT_EXIST) {
                log_warn("remove list return code = %d", rev);
            }

            if (is_need_duplicate(rc)) {
                uint32_t hashcode = util::string_util::mur_mur_hash(key->get_data(),
                        key->get_size());
                data_entry *dup_key = new data_entry();
                dup_key->clone(*key);
                base_packet* packet = new request_remove(request->area, dup_key);
                plugin::plugins_root* plugin_root = NULL;
                tair_mgr->plugins_manager.do_response_plugins(plugin::PLUGIN_TYPE_SYSTEM,
                        rc, hashcode, packet, plugin_root);
            }
         }
      } else if (request->key != NULL) {
         rc = tair_mgr->remove(request->area, *(request->key));

         if (rc != TAIR_RETURN_SUCCESS && rc != TAIR_RETURN_DATA_NOT_EXIST) {
            log_warn("remove return code = %d", rc);
         }

         if (is_need_duplicate(rc)) {
             uint32_t hashcode = util::string_util::mur_mur_hash(request->key->get_data(),
                     request->key->get_size());
             base_packet* packet = new request_remove((*request));
             if (packet == NULL) {
                 return false;
             }
             plugin::plugins_root* plugin_root = NULL;
             tair_mgr->plugins_manager.do_response_plugins(plugin::PLUGIN_TYPE_REMOTE_SYNC,
                     rc, hashcode, packet, plugin_root);
         }
      }

      return rc;
   }

   int request_processor::process(request_inc_dec *request, bool &send_return)
   {
      int rc = TAIR_RETURN_SUCCESS;
      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      uint64_t target_server_id = 0;
      if (request->hash_code != -1 &&
              tair_mgr->should_proxy(request->hash_code, target_server_id)) {
         return TAIR_RETURN_SHOULD_PROXY;
      }

      response_inc_dec *resp = new response_inc_dec();
      if (resp == NULL) {
          send_return = true;
          return TAIR_RETURN_FAILED;
      }

      rc = tair_mgr->add_count(request->area, request->key,
              request->add_count, request->init_value, &resp->value,request->expired);

      do_duplicate(rc, request);

      if (rc != TAIR_RETURN_SUCCESS) {
          delete resp;
          send_return = true;
      } else {
          resp->config_version = heart_beat->get_client_version();
          resp->set_code(rc);
          resp->setChannelId(request->getChannelId());
          if (request->get_connection()->postPacket(resp) == false) {
              delete resp;
          }
          send_return = false;
      }

      return rc;
   }

   int request_processor::process(request_duplicate *request, bool &sendReturn)
   {
      int ret = 0;

      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      log_debug("request key serverflag = %d", request->key.server_flag);
      if (request->data.data_meta.flag == TAIR_ITEM_FLAG_DELETED) {
         // remove
         log_debug("duplicate remove operate");
         request->key.data_meta.version = 0; // force remove
         ret = tair_mgr->remove(request->area, request->key);
      } else {
         // put
         log_debug("duplicate put operate");
         ret = tair_mgr->put(request->area, request->key, request->data, request->key.data_meta.edate);
      }
      log_debug("duplicate return code: %d", ret);
      if (ret == TAIR_RETURN_SUCCESS || ret == TAIR_RETURN_DATA_NOT_EXIST) {
         log_debug("send return packet to duplicate source");
         response_duplicate *dresp = new response_duplicate();
         dresp->setChannelId(request->getChannelId());
         dresp->server_id = local_server_ip::ip;
         dresp->packet_id = request->packet_id;
         dresp->bucket_id = request->bucket_number;
         if (request->get_connection()->postPacket(dresp) == false) {
            delete dresp;
         }
      } else {
         log_debug("XXX do duplicate operation failed");
      }
      sendReturn = false;
      return ret;
   }

   int request_processor::process(request_mupdate *request,bool &send_return)
   {
      if (tair_mgr->is_working() == false) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      tair_operc_vector::iterator it;

      if (request->server_flag != TAIR_SERVERFLAG_MIGRATE) {
         log_warn("requestMUpdatePacket have no MIGRATE flag");
         return TAIR_RETURN_INVALID_ARGUMENT;
      }

      int rc = TAIR_RETURN_SUCCESS;
      if (request->key_and_values != NULL) {
         for (it = request->key_and_values->begin(); it != request->key_and_values->end(); ++it) {
            operation_record *oprec = (*it);
            if (oprec->operation_type == 1) {
               // put
               rc = tair_mgr->direct_put(*oprec->key, *oprec->value);
            } else if (oprec->operation_type == 2) {
               // remove
               rc = tair_mgr->direct_remove(*oprec->key);
            } else {
               rc = TAIR_RETURN_INVALID_ARGUMENT;
               break;
            }

            if (rc != TAIR_RETURN_SUCCESS) {
               log_debug("do migrate operation failed, rc: %d", rc);
               break;
            }
         }
      }

      return rc;
   }

   bool request_processor::do_proxy(uint64_t target_server_id, base_packet *proxy_packet, base_packet *packet)
   {
      proxy_packet->server_flag = TAIR_SERVERFLAG_PROXY;
      bool rc = connection_mgr->sendPacket(target_server_id, proxy_packet, NULL, packet);
      if (rc == false) {
         delete proxy_packet;
      }
      return rc;
   }


}
