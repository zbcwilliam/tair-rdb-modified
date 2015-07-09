/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * dataserver operations abstract layer impl
 *
 * Version: $Id: tair_manager.cpp 603 2012-03-08 03:28:19Z choutian.xmm@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifdef WITH_KDB
#include "kdb_manager.h"
#endif
#include "tair_manager.hpp"
#include "rdb_manager.h"
#include "mockdb_manager.h"
#include "migrate_manager.hpp"
#include "define.hpp"
#include "mdb_factory.hpp"
#include "fdb_manager.hpp"

namespace {
   const int TAIR_OPERATION_VERSION   = 1;
   const int TAIR_OPERATION_DUPLICATE = 2;
   const int TAIR_OPERATION_REMOTE    = 4;
   const int TAIR_DUPLICATE_BUSY_RETRY_COUNT = 10;
}

namespace tair {
#define STAT_KEY_VERIFY  do {                                           \
    if (status != STATUS_CAN_WORK)                                      \
    {                                                                   \
      return TAIR_RETURN_SERVER_CAN_NOT_WORK;                           \
    }                                                                   \
                                                                        \
    if (key.get_size () >= TAIR_MAX_KEY_SIZE || key.get_size () < 1)    \
    {                                                                   \
      return TAIR_RETURN_ITEMSIZE_ERROR;                                \
    }                                                                   \
}while(0)

   tair_manager::tair_manager() : migrate_done_set(0)
   {
      status = STATUS_NOT_INITED;
      storage_mgr = NULL;
      table_mgr = new table_manager();
      duplicator = NULL;
      migrate_mgr = NULL;
      migrate_log = NULL;
      dump_mgr = NULL;
   }

   tair_manager::~tair_manager()
   {
      if (migrate_mgr != NULL) {
         delete migrate_mgr;
         migrate_mgr = NULL;
      }

      if (duplicator != NULL) {
         delete duplicator;
         duplicator = NULL;
      }

      if (migrate_log != NULL) {
         migrate_log->close();
         migrate_log = NULL;
      }

      if (dump_mgr != NULL) {
         delete dump_mgr;
         dump_mgr = NULL;
      }

      if (storage_mgr != NULL) {
         delete storage_mgr;
         storage_mgr = NULL;
      }

      delete table_mgr;
      table_mgr = NULL;
   }

   bool tair_manager::initialize(tbnet::Transport *transport, tbnet::DefaultPacketStreamer *streamer)
   {
      if (status != STATUS_NOT_INITED) {
         return true;
      }

      const char *se_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_SENGINE, NULL);
      if (se_name == NULL || (strcmp(se_name, "mdb") == 0)) {
         // init mdb
         storage_mgr = mdb_factory::create_mdb_manager(false);
      } else if (strcmp(se_name, "fdb") == 0){
         // init fdb
         storage_mgr = new tair::storage::fdb::fdb_manager();
      }
#ifdef WITH_KDB
      else if (strcmp(se_name, "kdb") == 0){
         // init kdb
         storage_mgr = new tair::storage::kdb::kdb_manager();
      }
#endif
      else if (strcmp(se_name, "rdb") == 0){
         storage_mgr = new tair::storage::rdb::rdb_manager();
      }
      else if (strcmp(se_name, "mock") == 0){
         storage_mgr = new tair::storage::mockdb::mockdb_manager();
      }
      else {
         return false;
      }

      if (storage_mgr == NULL) {
         log_error("init storage engine failed, storage engine name: %s", se_name);
         return false;
      }

      // init the storage manager for stat helper
      TAIR_STAT.set_storage_manager(storage_mgr);

      // init dupicator
      int duplicate_thread_count = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, DUPLICATE_THREAD_NUM,
              DEFAULT_DUPLICATE_THREAD_NUM);
      if (duplicate_thread_count > 0) {
        duplicator = new duplicate_sender_manager(transport, streamer, table_mgr);
      }

      base_duplicator* dup;
      dup = duplicator;

      migrate_mgr = new migrate_manager(transport, streamer, dup, this, storage_mgr);

      dump_mgr = new tair::storage::dump_manager(storage_mgr);

      status = STATUS_INITED;

      localmode = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_LOCAL_MODE, 0);
      if (localmode) {
          table_mgr->set_table_for_localmode();
          status = STATUS_CAN_WORK;
      }

      return true;
   }

   /* common operator */
   int tair_manager::info (data_entry & info)
   {
      char buffer[1024];
      struct rusage self_ru;
      getrusage(RUSAGE_SELF, &self_ru);
      snprintf(buffer, 1024, "tair 2.3.4.9\r\n"
                             "used_cpu_sys: %f\r\n"
                             "used_cpu_user: %f\r\n"
                             "swap used %ld\r\n",
                             (float)self_ru.ru_stime.tv_sec+(float)self_ru.ru_stime.tv_usec/1000000,
                             (float)self_ru.ru_utime.tv_sec+(float)self_ru.ru_utime.tv_usec/1000000,
                             self_ru.ru_nswap);
      static int len = strlen(buffer);
      info.set_data(buffer, len);
      return TAIR_RETURN_SUCCESS;
   }

   int tair_manager::ttl (int area, data_entry & key, long long* time_remain)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area(area);

     int rc = storage_mgr->ttl(area, mkey, time_remain);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);

     return rc;
   }

   int tair_manager::type (int area, data_entry & key, long long* what_type)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area(area);

     int rc = storage_mgr->type(area, mkey, what_type);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);

     return rc;
   }

   int tair_manager::expire (int area, data_entry & key, int32_t expiretime)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area(area);

     int rc = storage_mgr->expire (area, mkey, expiretime);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);

     return rc;
   }

   int tair_manager::expireat (int area, data_entry & key, int32_t expiretime)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->expireat (area, mkey, expiretime);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::persist (int area, data_entry & key)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->persist (area, mkey);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::exists (int area, data_entry & key)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->exists (area, mkey);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::lazyclear(int area)
   {
      if (status != STATUS_CAN_WORK) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      return storage_mgr->lazyclear(area);
   }

   int tair_manager::dumparea(int area)
   {
      if (status != STATUS_CAN_WORK) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      return storage_mgr->dumparea(area);
   }

   int tair_manager::loadarea(int area)
   {
      if (status != STATUS_CAN_WORK) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      return storage_mgr->loadarea(area);
   }

   int tair_manager::addfilter (int area, data_entry & key,
           data_entry & field, data_entry & value)
   {
      if (status != STATUS_CAN_WORK) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      data_entry mkey = key;
      if (key.get_size() > 0) {
         mkey.merge_area (area);
      }

      return storage_mgr->addfilter (area, key, field, value);
   }

   int tair_manager::removefilter(int area, data_entry & key,
           data_entry & field, data_entry & value)
   {
      if (status != STATUS_CAN_WORK) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      data_entry mkey = key;
      if (key.get_size() > 0) {
         mkey.merge_area (area);
      }

      return storage_mgr->removefilter (area, key, field, value);
   }

   /* list operator */
   int tair_manager::lindex(int area, data_entry & key,
 			    int32_t index, data_entry & value)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area(area);

     int rc = storage_mgr->lindex(area, mkey, index, value);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::lrpush(int area, data_entry &key, vector<data_entry *> &value,
           uint32_t * oklen, uint32_t * list_len, int pcode, int max_count,
           uint16_t version, int expire_time)
   {
     STAT_KEY_VERIFY;

     if (value.size () == 0)
     {
       return TAIR_RETURN_SUCCESS;
     }

     for (size_t i = 0; i < value.size (); i++)
     {
       if (value[i]->get_size () >= TAIR_MAX_DATA_SIZE ||
               value[i]->get_size () < 1)
       {
     	 return TAIR_RETURN_ITEMSIZE_ERROR;
       }
     }

     data_entry mkey = key;	// key merged with area
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc;
     if (pcode == TAIR_REQ_LPUSH_PACKET || pcode == TAIR_REQ_LPUSH_LIMIT_PACKET)
     {
       rc = storage_mgr->lrpush (area, mkey, value,
 				oklen, list_len, IS_LEFT, IS_NOT_EXIST, max_count,
                 version_care, version, expire_time);
     }
     else if (pcode == TAIR_REQ_RPUSH_PACKET || pcode == TAIR_REQ_RPUSH_LIMIT_PACKET)
     {
       rc = storage_mgr->lrpush (area, mkey, value,
 				oklen, list_len, IS_RIGHT, IS_NOT_EXIST, max_count,
                 version_care, version, expire_time);
     }
     else if (pcode == TAIR_REQ_LPUSHX_PACKET || pcode == TAIR_REQ_LPUSHX_LIMIT_PACKET)
     {
       rc = storage_mgr->lrpush (area, mkey, value,
 				oklen, list_len, IS_LEFT, IS_EXIST, max_count,
                 version_care, version, expire_time);
     }
     else if (pcode == TAIR_REQ_RPUSHX_PACKET || pcode == TAIR_REQ_RPUSHX_LIMIT_PACKET)
     {
       rc = storage_mgr->lrpush (area, mkey, value,
 				oklen, list_len, IS_RIGHT, IS_EXIST, max_count,
                 version_care, version, expire_time);
     }

     TAIR_STAT.stat_put (area);

     return rc;
   }

   int tair_manager::lrpop (int area, data_entry & key,
 			   int popnum, vector<data_entry *>&value, int pcode,
                uint16_t version, int expire_time)
   {
     value.clear ();

     STAT_KEY_VERIFY;

     data_entry mkey = key;	// key merged with area
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     LeftOrRight lr = IS_LEFT;
     if (pcode == TAIR_REQ_LPOP_PACKET)
     {
       lr = IS_LEFT;
     }
     else if (pcode == TAIR_REQ_RPOP_PACKET)
     {
       lr = IS_RIGHT;
     }
     int rc = storage_mgr->lrpop (area, mkey, popnum, value, lr,
             version_care, version, expire_time);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_put (area);

     return rc;
   }


   int tair_manager::ltrim (int area, data_entry & key, int32_t start,
 			   int32_t end, uint16_t version, int expire_time)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc = storage_mgr->ltrim (area, mkey, start, end, version_care,
             version, expire_time);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);

     return rc;
   }

   int tair_manager::llen (int area, data_entry & key, long long* retlen)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->llen (area, mkey, retlen);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::lrem (int area, data_entry & key, int32_t count,
 			  data_entry & value, long long *retnum,
               uint16_t version, int expire_time)
   {
     STAT_KEY_VERIFY;

     if (value.get_size () >= TAIR_MAX_DATA_SIZE || value.get_size () < 1)
     {
       return TAIR_RETURN_ITEMSIZE_ERROR;
     }

     data_entry mkey = key;
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc = storage_mgr->lrem (area, mkey, count, value, retnum,
 				version_care, version, expire_time);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::lrange (int area, data_entry & key, int32_t start,
 			    int32_t end, vector < data_entry * >&values)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->lrange (area, mkey, start, end, values);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

 /* hset */
   int tair_manager::genericHgetall (int area, data_entry & key, vector<data_entry *>& field_val, int method)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc;
     if (method == METHOD_HGETALL) {
        rc = storage_mgr->hgetall(area, mkey, field_val);
     } else if (method == METHOD_HKEYS) {
        rc = storage_mgr->hkeys(area, mkey, field_val);
     } else if (method == METHOD_HVALS) {
        rc = storage_mgr->hvals(area, mkey, field_val);
     }

     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::hgetall (int area, data_entry & key,
           vector<data_entry *>& field_val)
   {
     return genericHgetall(area, key, field_val, METHOD_HGETALL);
   }

   int tair_manager::hvals (int area, data_entry & key,
 			   vector<data_entry * >& values)
   {
     return genericHgetall(area, key, values, METHOD_HVALS);
   }

   int tair_manager::hkeys (int area, data_entry & key,
 			   vector<data_entry * >& keys)
   {
     return genericHgetall(area, key, keys, METHOD_HKEYS);
   }

   int tair_manager::hincrby (int area, data_entry & key, data_entry & field,
 			     long long & value, long long *retnum,
                  uint16_t version, int expire_time)
   {
     STAT_KEY_VERIFY;

     if (field.get_size () >= TAIR_MAX_DATA_SIZE || field.get_size () < 1)
     {
       return TAIR_RETURN_ITEMSIZE_ERROR;
     }

     data_entry mkey = key;
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc = storage_mgr->hincrby (area, mkey, field, value, retnum,
 				   version_care, version, expire_time);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::hmset (int area, data_entry & key,
 			   vector<data_entry *>& field_val, int *retvalue,
                uint16_t version, int expire_time)
   {
     STAT_KEY_VERIFY;

     for (size_t i = 0; i < field_val.size (); i++)
     {
       if (field_val[i]->get_size () >= TAIR_MAX_DATA_SIZE ||
               field_val[i]->get_size () < 1)
       {
     	 return TAIR_RETURN_ITEMSIZE_ERROR;
       }
     }

     data_entry mkey = key;
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc = storage_mgr->hmset (area, mkey, field_val, retvalue, version_care,
               version, expire_time);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;

   }

   int tair_manager::hset (int area, data_entry & key,
 			  data_entry & field, data_entry & value,
               uint16_t version, int expire_time)
   {
     STAT_KEY_VERIFY;

     if (field.get_size () >= TAIR_MAX_DATA_SIZE || field.get_size () < 1)
     {
       return TAIR_RETURN_ITEMSIZE_ERROR;
     }

     if (value.get_size () >= TAIR_MAX_DATA_SIZE || value.get_size () < 1)
     {
       return TAIR_RETURN_ITEMSIZE_ERROR;
     }

     data_entry mkey = key;
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc = storage_mgr->hset (area, mkey, field, value, version_care,
               version, expire_time);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::hsetnx (int area, data_entry & key,
 			    data_entry & field, data_entry & value,
                 uint16_t version, int expire_time)
   {
     STAT_KEY_VERIFY;

     if (field.get_size () >= TAIR_MAX_DATA_SIZE || field.get_size () < 1)
     {
       return TAIR_RETURN_ITEMSIZE_ERROR;
     }

     if (value.get_size () >= TAIR_MAX_DATA_SIZE || value.get_size () < 1)
     {
       return TAIR_RETURN_ITEMSIZE_ERROR;
     }

     data_entry mkey = key;
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc = storage_mgr->hsetnx (area, mkey, field,
             value, version_care, version, expire_time);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::hexists (int area, data_entry & key,
              data_entry & field)
   {
     STAT_KEY_VERIFY;

     if (field.get_size () >= TAIR_MAX_DATA_SIZE || field.get_size () < 1)
     {
       return TAIR_RETURN_ITEMSIZE_ERROR;
     }

     data_entry mkey = key;
     mkey.merge_area(area);

     int rc = storage_mgr->hexists(area, mkey, field);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get(area, rc);
     return rc;
   }

   int tair_manager::hget (int area, data_entry & key, data_entry & field,
 			  data_entry & value)
   {
     STAT_KEY_VERIFY;

     if (field.get_size () >= TAIR_MAX_DATA_SIZE || field.get_size () < 1)
     {
       return TAIR_RETURN_ITEMSIZE_ERROR;
     }

     data_entry mkey = key;
     mkey.merge_area(area);

     int rc = storage_mgr->hget(area, mkey, field, value);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get(area, rc);
     return rc;
   }

   int tair_manager::hmget (int area, data_entry & key,
 			   vector < data_entry * >&fields, vector < data_entry * >&values)
   {
     STAT_KEY_VERIFY;

     for (size_t i = 0; i < fields.size (); i++)
     {
       if (fields[i]->get_size () >= TAIR_MAX_DATA_SIZE ||
 	  fields[i]->get_size () < 1)
       {
     	return TAIR_RETURN_ITEMSIZE_ERROR;
       }
     }

     for (size_t i = 0; i < values.size (); i++)
     {
       if (values[i]->get_size () >= TAIR_MAX_DATA_SIZE ||
               values[i]->get_size () < 1)
       {
     	return TAIR_RETURN_ITEMSIZE_ERROR;
       }
     }

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->hmget (area, mkey, fields, values);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::hdel (int area, data_entry & key, data_entry & field,
           uint16_t version, int expire_time)
   {
     STAT_KEY_VERIFY;

     if (field.get_size () >= TAIR_MAX_DATA_SIZE || key.get_size () < 1)
     {
       return TAIR_RETURN_ITEMSIZE_ERROR;
     }

     data_entry mkey = key;
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc = storage_mgr->hdel (area, mkey, field,
             version_care, version, expire_time);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::hlen(int area, data_entry & key, long long *retlen)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->hlen (area, mkey, retlen);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }
 /* set */
   int tair_manager::scard (int area, data_entry & key, long long * retnum)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->scard (area, mkey, retnum);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::smembers (int area, data_entry & key,
 			      vector < data_entry * >&values)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->smembers (area, mkey, values);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::sadd (int area, data_entry & key, data_entry & value,
           uint16_t version, int expire_time)
   {
     STAT_KEY_VERIFY;

     if (value.get_size () >= TAIR_MAX_DATA_SIZE || value.get_size () < 1)
     {
       return TAIR_RETURN_ITEMSIZE_ERROR;
     }

     data_entry mkey = key;
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc = storage_mgr->sadd (area, mkey, value, version_care,
             version, expire_time);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::srem (int area, data_entry & key, data_entry & value,
		   uint16_t version, int expire_time)
   {
     STAT_KEY_VERIFY;

     if (value.get_size () >= TAIR_MAX_DATA_SIZE || value.get_size () < 1)
     {
       return TAIR_RETURN_ITEMSIZE_ERROR;
     }

     data_entry mkey = key;
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc = storage_mgr->srem (area, mkey, value, version_care,
			 version, expire_time);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }
   int tair_manager::spop (int area, data_entry & key, data_entry & value,
           uint16_t version, int expire_time)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc = storage_mgr->spop (area, mkey, value, version_care,
             version, expire_time);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

 /* zset */
   int tair_manager::zscore (int area, data_entry & key,
 			    data_entry & value, double *score)
   {
     STAT_KEY_VERIFY;

     if (value.get_size () >= TAIR_MAX_DATA_SIZE || value.get_size () < 1)
     {
       return TAIR_RETURN_ITEMSIZE_ERROR;
     }

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->zscore (area, mkey, value, score);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::zrange (int area, data_entry & key,
 			    int32_t start, int end,
 			    vector<data_entry *> &values, vector<double> &scores, int withscore)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->zrange (area, mkey, start, end, values, scores, withscore);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::zrevrange (int area, data_entry & key, int32_t start,
 			       int32_t end, vector<data_entry *> &values, vector<double> &scores, int withscore)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->zrevrange (area, mkey, start, end, values, scores, withscore);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::zrangebyscore (int area, data_entry & key, double start,
 				   double end, vector<data_entry *> &values,
                   vector<double> &scores, int limit, int withscore)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->zrangebyscore(area, mkey, start, end,
             values, scores, limit, withscore);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get(area, rc);
     return rc;
   }

   int tair_manager::zrevrangebyscore (int area, data_entry & key, double start,
 				   double end, vector<data_entry *> &values,
                   vector<double> &scores, int limit, int withscore)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->zrevrangebyscore(area, mkey, start, end,
             values, scores, limit, withscore);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get(area, rc);
     return rc;
   }

   int tair_manager::zadd (int area, data_entry & key, double score,
 			  data_entry & value, uint16_t version, int expire_time)
   {
     STAT_KEY_VERIFY;

     if (value.get_size () >= TAIR_MAX_DATA_SIZE || value.get_size () < 1)
     {
       return TAIR_RETURN_ITEMSIZE_ERROR;
     }

     data_entry mkey = key;
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc = storage_mgr->zadd (area, mkey, score, value, version_care,
               version, expire_time);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::zrank (int area, data_entry & key, data_entry & value,
 			   long long * rank)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->zrank (area, mkey, value, rank);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::zrevrank (int area, data_entry & key, data_entry & value,
		   long long * rank)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->zrevrank (area, mkey, value, rank);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::zcount(int area, data_entry & key, double start, double end,
		   long long *retnum)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->zcount (area, mkey, start, end, retnum);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::zincrby (int area, data_entry & key, data_entry & value,
  		 double & addscore, double *retnum, uint16_t version, int expire_time)
   {
     STAT_KEY_VERIFY;

     if (value.get_size () >= TAIR_MAX_DATA_SIZE || value.get_size () < 1)
     {
       return TAIR_RETURN_ITEMSIZE_ERROR;
     }

     data_entry mkey = key;
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc = storage_mgr->zincrby (area, mkey, value, addscore, retnum,
 				   version_care, version, expire_time);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::zcard (int area, data_entry & key, long long * retnum)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int rc = storage_mgr->zcard (area, mkey, retnum);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::zrem (int area, data_entry & key, data_entry & value,
		   uint16_t version, int expire_time)
   {
     STAT_KEY_VERIFY;

     if (value.get_size () >= TAIR_MAX_DATA_SIZE || value.get_size () < 1)
     {
       return TAIR_RETURN_ITEMSIZE_ERROR;
     }

     data_entry mkey = key;
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc = storage_mgr->zrem (area, mkey, value, version_care,
			 version, expire_time);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::zremrangebyrank (int area, data_entry & key, int start,
		   int end, uint16_t version, int expire_time, long long *remnum)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc = storage_mgr->zremrangebyrank (area, mkey, start, end,
			 version_care, version, expire_time, remnum);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::zremrangebyscore (int area, data_entry & key, double start,
		   double end, uint16_t version, int expire_time, long long *remnum)
   {
     STAT_KEY_VERIFY;

     data_entry mkey = key;
     mkey.merge_area (area);

     int op_flag = get_op_flag (key.server_flag);
     bool version_care = op_flag & TAIR_OPERATION_VERSION;

     int rc = storage_mgr->zremrangebyscore (area, mkey, start, end,
			 version_care, version, expire_time, remnum);
     key.data_meta = mkey.data_meta;
     TAIR_STAT.stat_get (area, rc);
     return rc;
   }

   int tair_manager::putnx(int area, data_entry &key, data_entry &value, int expire_time)
   {
      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
         return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      if (value.get_size() >= TAIR_MAX_DATA_SIZE || value.get_size() < 1) {
         return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      data_entry mkey = key;
      mkey.merge_area(area);

      int op_flag = get_op_flag(key.server_flag);
      bool version_care = op_flag & TAIR_OPERATION_VERSION;

      int rc = storage_mgr->putnx(area, mkey, value, version_care, expire_time);
      TAIR_STAT.stat_put(area);

      return rc;
   }

   int tair_manager::put(int area, data_entry &key, data_entry &value, int expire_time)
   {
      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
         return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      if (value.get_size() >= TAIR_MAX_DATA_SIZE || value.get_size() < 1) {
         return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      data_entry mkey = key; // key merged with area
      mkey.merge_area(area);

      int op_flag = get_op_flag(key.server_flag);
      bool version_care = op_flag & TAIR_OPERATION_VERSION;

      int rc = storage_mgr->put(area, mkey, value, version_care, expire_time);
      TAIR_STAT.stat_put(area);

      return rc;
   }

   int tair_manager::direct_put(data_entry &key, data_entry &value)
   {
      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
         return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      if (value.get_size() >= TAIR_MAX_DATA_SIZE || value.get_size() < 1) {
         return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      data_entry akey = key;
      int area = akey.decode_area();
      key.area = area;

      int rc = storage_mgr->put(area, key, value, false, 0);

      TAIR_STAT.stat_put(area);

      return rc;
   }

   int tair_manager::direct_remove(data_entry &key)
   {
      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
         return TAIR_RETURN_ITEMSIZE_ERROR;
      }
      data_entry akey = key;
      akey.decode_area();

      int rc = storage_mgr->remove(akey.area, key, false);

      if (rc == TAIR_RETURN_DATA_NOT_EXIST) {
         // for migrate, return SUCCESS
         rc = TAIR_RETURN_SUCCESS;
      }

      TAIR_STAT.stat_remove(akey.area);

      return rc;
   }

   int tair_manager::add_count(int area, data_entry &key, int count, int init_value, int *result_value, int expire_time)
   {
      if (status != STATUS_CAN_WORK) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
         return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      //for rdb,rdb have no meta info,it's use his own incdecr
      data_entry mkey = key;
      mkey.merge_area (area);
      int op_flag = get_op_flag (key.server_flag);
      bool version_care = op_flag & TAIR_OPERATION_VERSION;
      int rc1 = storage_mgr->incdecr(area, mkey, init_value, count, result_value,
              version_care, 0, expire_time);
      if (rc1 != TAIR_RETURN_IS_NOT_IMPLEMENT) {
        //it's mean this is rdb module,
        key.data_meta = mkey.data_meta;
        TAIR_STAT.stat_get (area, rc1);
        return rc1;
      }

      //other storage engine
      tbsys::CThreadGuard guard(&counter_mutex[get_mutex_index(key)]);
      // get from storage engine
      data_entry old_value;
      int rc = get(area, key, old_value);
      log_debug("get result: %d, flag: %d", rc, key.data_meta.flag);
      key.data_meta.log_self();
      if (rc == TAIR_RETURN_SUCCESS && IS_ADDCOUNT_TYPE(key.data_meta.flag)) {
         // old value exist
         int32_t *v = (int32_t *)(old_value.get_data() + ITEM_HEAD_LENGTH);
         log_debug("old count: %d, new count: %d, init value: %d", (*v), count, init_value);
         *v += count;
         *result_value = *v;
      } else if(rc == TAIR_RETURN_SUCCESS){
         //exist,but is not add_count,return error;
         log_debug("cann't override old value");
         return TAIR_RETURN_CANNOT_OVERRIDE;
      }else {
         // old value not exist
         char fv[6]; // 2 + sizeof(int)
         *((short *)fv) = 0x1600; // for java header
         *result_value = init_value + count;
         *((int32_t *)(fv + 2)) = *result_value;
         old_value.set_data(fv, 6);
      }

      old_value.data_meta.flag |= TAIR_ITEM_FLAG_ADDCOUNT;
      log_debug("before put flag: %d", old_value.data_meta.flag);
      int result = put(area, key, old_value, expire_time);
      return result;
   }

   int tair_manager::get(int area, data_entry &key, data_entry &value)
   {
      if (status != STATUS_CAN_WORK) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
         return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      data_entry mkey = key;
      mkey.merge_area(area);

      int rc = storage_mgr->get(area, mkey, value);
      key.data_meta = mkey.data_meta;
      TAIR_STAT.stat_get(area, rc);
      return rc;
   }

   int tair_manager::getset(int area, data_entry & key, data_entry & value, data_entry & oldvalue,
           uint16_t version, int expire_time)
   {
      STAT_KEY_VERIFY;

      if (value.get_size () >= TAIR_MAX_DATA_SIZE || value.get_size () < 1)
      {
        return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      data_entry mkey = key;
      mkey.merge_area(area);

      int op_flag = get_op_flag (key.server_flag);
      bool version_care = op_flag & TAIR_OPERATION_VERSION;

      int rc = storage_mgr->getset (area, mkey, value, oldvalue,
              version_care, version, expire_time);
      key.data_meta = mkey.data_meta;
      TAIR_STAT.stat_get (area, rc);
      return rc;
   }

   int tair_manager::remove(int area, data_entry &key)
   {
      if (key.get_size() >= TAIR_MAX_KEY_SIZE || key.get_size() < 1) {
         return TAIR_RETURN_ITEMSIZE_ERROR;
      }

      data_entry mkey = key;
      mkey.merge_area(area);

      int op_flag = get_op_flag(key.server_flag);
      bool version_care =  op_flag & TAIR_OPERATION_VERSION;

      int rc = storage_mgr->remove(area, mkey, version_care);
      TAIR_STAT.stat_remove(area);

      return rc;
   }

   int tair_manager::clear(int area)
   {
      if (status != STATUS_CAN_WORK) {
         return TAIR_RETURN_SERVER_CAN_NOT_WORK;
      }

      return storage_mgr->clear(area);
   }

   void tair_manager::do_dump(set<dump_meta_info> dump_meta_infos)
   {
      log_debug("receive dump request, size: %d", dump_meta_infos.size());
      if (dump_meta_infos.size() == 0) return;

      // cancal all previous task
      dump_mgr->cancle_all();

      // make sure all task has been canceled
      while (dump_mgr->is_all_stoped() == false) usleep(100);

      set<dump_meta_info>::iterator it;
      set<tair::storage::dump_filter> dump_filters;

      // dump directory
      const string dir(TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_DUMP_DIR, TAIR_DEFAULT_DUMP_DIR));
      set<uint32_t> buckets;
      for (uint32_t bucket_no = 0; bucket_no < table_mgr->get_bucket_count(); bucket_no++) {
         if (table_mgr->is_master(bucket_no, TAIR_SERVERFLAG_CLIENT)) {
            buckets.insert(bucket_no);
         }
      }

      for (it=dump_meta_infos.begin(); it!=dump_meta_infos.end(); it++) {
         dump_meta_info info = *it;
         tair::storage::dump_filter filter;
         filter.set_parameter(info.start_time, info.end_time, info.area, dir);
         log_debug("add dump: {startTime:%u, endTime:%u, area:%d, dir:%s }",
                 info.start_time, info.end_time, info.area, dir.c_str());
         dump_filters.insert(filter);
      }

      dump_mgr->do_dump(dump_filters, buckets, time(NULL));
   }

   bool tair_manager::is_migrating()
   {
      return migrate_mgr->is_migrating();
   }

   bool tair_manager::should_proxy(uint32_t hashcode, uint64_t &target_server_id)
   {
      if (localmode) {
          return false;
      }

      int bucket_number = get_bucket_number(hashcode);
      bool is_migrated  = migrate_done_set.test(bucket_number);
      if (is_migrated) {
         target_server_id = table_mgr->get_migrate_target(bucket_number);
         if (target_server_id == local_server_ip::ip) // target is myself, do not proxy
            target_server_id = 0;
      }
      return is_migrated && target_server_id != 0;
   }

   bool tair_manager::should_proxy(data_entry &key, uint64_t &target_server_id)
   {
      if (key.server_flag == TAIR_SERVERFLAG_PROXY)
         return false; // if this is proxy, dont proxy

      if (localmode) {
          return false;
      }

      int bucket_number = get_bucket_number(key);
      bool is_migrated = migrate_done_set.test(bucket_number);
      if (is_migrated) {
         target_server_id = table_mgr->get_migrate_target(bucket_number);
         if (target_server_id == local_server_ip::ip) // target is myself, do not proxy
            target_server_id = 0;
      }
      return is_migrated && target_server_id != 0;
   }

   bool tair_manager::need_do_migrate_log(int bucket_number)
   {
      assert (migrate_mgr != NULL);
      return migrate_mgr->is_bucket_migrating(bucket_number);
   }

   void tair_manager::set_migrate_done(int bucket_number)
   {
      assert (migrate_mgr != NULL);
      migrate_done_set.set(bucket_number, true);
   }

   void tair_manager::update_server_table(uint64_t *server_table, int server_table_size, uint32_t server_table_version,
           int32_t data_need_remove, vector<uint64_t> &current_state_table, uint32_t copy_count, uint32_t bucket_count)
   {
      tbsys::CThreadGuard update_table_guard(&update_server_table_mutex);

      log_debug("updateServerTable, size: %d", server_table_size);
      table_mgr->do_update_table(server_table, server_table_size, server_table_version, copy_count, bucket_count);
      if (duplicator != NULL) {
        duplicator->set_max_queue_size((table_mgr->get_copy_count() - 1) * 3);
      }
      storage_mgr->set_bucket_count(table_mgr->get_bucket_count());

      migrate_done_set.resize(table_mgr->get_bucket_count());
      migrate_done_set.reset();
      if (status != STATUS_CAN_WORK) {
         if (data_need_remove == TAIR_DATA_NEED_MIGRATE)
            init_migrate_log();
         table_mgr->init_migrate_done_set(migrate_done_set, current_state_table);
         status = STATUS_CAN_WORK; // set inited status to true after init buckets
      }

      vector<int> release_buckets (table_mgr->get_release_buckets());
      if (release_buckets.empty() == false) {
         storage_mgr->close_buckets(release_buckets);
      }

      vector<int> holding_buckets (table_mgr->get_holding_buckets());

      if (holding_buckets.empty() == false) {
         // remove already migrated buckets
         vector<int>::reverse_iterator rit = holding_buckets.rbegin();
         while (rit != holding_buckets.rend()) {
            if (migrate_done_set.test(*rit))
               holding_buckets.erase((++rit).base());
            else
               ++rit;
         }
         storage_mgr->init_buckets(holding_buckets);
      }

      vector<int> padding_buckets (table_mgr->get_padding_buckets());
      if (padding_buckets.empty() == false) {
         storage_mgr->init_buckets(padding_buckets);
      }

      // clear dump task
      dump_mgr->cancle_all();
      migrate_mgr->do_server_list_changed();

      bucket_server_map migrates (table_mgr->get_migrates());

      if (migrates.empty() == false) {
         // remove already migrated buckets
         bucket_server_map::iterator it = migrates.begin();
         while (it != migrates.end()) {
            if (migrate_done_set.test((*it).first))
               migrates.erase(it++);
            else
               ++it;
         }
         migrate_mgr->set_migrate_server_list(migrates, table_mgr->get_version());
      }
      if (duplicator != NULL) {
        duplicator->do_hash_table_changed();
      }
   }

   // private methods
   uint32_t tair_manager::get_bucket_number(data_entry &key)
   {
      uint32_t hashcode = tair::util::string_util::mur_mur_hash(key.get_data(), key.get_size());
      log_debug("hashcode: %u, bucket count: %d", hashcode, table_mgr->get_bucket_count());
      return hashcode % table_mgr->get_bucket_count();
   }

   uint32_t tair_manager::get_bucket_number(uint32_t hashcode)
   {
      return hashcode % table_mgr->get_bucket_count();
   }

   bool tair_manager::should_write_local(int bucket_number, int server_flag, int op_flag, int &rc)
   {
      if (status != STATUS_CAN_WORK) {
         log_debug("server can not work now...");
         rc = TAIR_RETURN_SERVER_CAN_NOT_WORK;
         return false;
      }

      if (migrate_mgr->is_bucket_available(bucket_number) == false) {
         log_debug("bucket is migrating, request reject");
         rc = TAIR_RETURN_MIGRATE_BUSY;
         return false;
      }

      if ((server_flag == TAIR_SERVERFLAG_CLIENT || server_flag == TAIR_SERVERFLAG_PROXY)
          && migrate_done_set.test(bucket_number)
          && table_mgr->is_master(bucket_number, TAIR_SERVERFLAG_PROXY) == false) {
         rc = TAIR_RETURN_MIGRATE_BUSY;
         return false;
      }

      log_debug("bucket number: %d, serverFlag: %d, client const: %d",
              bucket_number, server_flag, TAIR_SERVERFLAG_CLIENT);
      if ((server_flag == TAIR_SERVERFLAG_CLIENT || server_flag == TAIR_SERVERFLAG_PROXY)
          && table_mgr->is_master(bucket_number, server_flag) == false) {
         log_debug("request rejected...");
         rc = TAIR_RETURN_WRITE_NOT_ON_MASTER;
         return false;
      }

      if ((duplicator != NULL) && (op_flag & TAIR_OPERATION_DUPLICATE)) {
         bool is_available = false;
         for (int i=0; i<TAIR_DUPLICATE_BUSY_RETRY_COUNT; ++i) {
            is_available = duplicator->is_bucket_available(bucket_number);
            if (is_available)
               break;

            usleep(1000);
         }

         if (is_available == false) {
            log_debug("bucket is not avaliable, reject request");
            rc = TAIR_RETURN_DUPLICATE_BUSY;
            return false;
         }
      }

      return true;
   }

   void tair_manager::init_migrate_log()
   {
      if (migrate_log != NULL) {
         log_info("migrateLog already inited, quit");
         return;
      }
      // init migrate update log
      const char *mlog_dir = TBSYS_CONFIG.getString(TAIRSERVER_SECTION, TAIR_ULOG_DIR, NULL);
      if (mlog_dir == NULL) {
         log_error("migrate log directory can not empty");
         exit(1);
      }
      if (!tbsys::CFileUtil::mkdirs((char *)mlog_dir)) {
         log_error("mkdir migrate log dir failed: %s", mlog_dir);
         exit(1);
      }

      const char *log_file_name = TBSYS_CONFIG.getString(TAIRSERVER_SECTION,
              TAIR_ULOG_MIGRATE_BASENAME, TAIR_ULOG_MIGRATE_DEFAULT_BASENAME);
      int32_t log_file_number = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_ULOG_FILENUM, TAIR_ULOG_DEFAULT_FILENUM);
      int32_t log_file_size = TBSYS_CONFIG.getInt(TAIRSERVER_SECTION, TAIR_ULOG_FILESIZE, TAIR_ULOG_DEFAULT_FILESIZE);
      log_file_size *= MB_SIZE;

      migrate_log = update_log::open(mlog_dir, log_file_name, log_file_number, true, log_file_size);
      log_info("migrate log opened: %s/%s", mlog_dir, log_file_name);
      migrate_mgr->set_log(migrate_log);

   }

   void tair_manager::get_proxying_buckets(vector<uint32_t> &buckets)
   {
      if (migrate_done_set.any()) {
         for (uint32_t i=0; i<migrate_done_set.size(); ++i) {
            if (migrate_done_set.test(i))
               buckets.push_back(i);
         }
      }
   }

   void tair_manager::set_solitary()
   {
      if (status == STATUS_CAN_WORK) {
         status = STATUS_INITED;
         sleep(1);
         table_mgr->clear_available_server();
         if (duplicator != NULL) {
            duplicator->do_hash_table_changed();
         }
         migrate_mgr->do_server_list_changed();
         log_warn("serverVersion is 1, set tairManager to solitary");
      }
   }

   bool tair_manager::is_working()
   {
      return status == STATUS_CAN_WORK;
   }

   int tair_manager::get_mutex_index(data_entry &key)
   {
      uint32_t hashcode = util::string_util::mur_mur_hash(key.get_data(), key.get_size());
      return hashcode % mutex_array_size;
   }

   void tair_manager::get_slaves(int server_flag, int bucket_number, vector<uint64_t> &slaves) {
      if (table_mgr->get_copy_count() == 1) return;

      if (server_flag == TAIR_SERVERFLAG_PROXY) {
         slaves = table_mgr->get_slaves(bucket_number, true);
      } else {
         slaves = (table_mgr->get_slaves(bucket_number, migrate_done_set.test(bucket_number)));
      }
   }

   int tair_manager::get_op_flag(int server_flag)
   {
      int flag = 0;
      if (server_flag == TAIR_SERVERFLAG_CLIENT ||
          server_flag == TAIR_SERVERFLAG_PROXY) {
         flag |= TAIR_OPERATION_VERSION;
         flag |= TAIR_OPERATION_DUPLICATE;
         flag |= TAIR_OPERATION_REMOTE;
      }

      return flag;
   }

   bool tair_manager::is_localmode() {
       return localmode;
   }
}
