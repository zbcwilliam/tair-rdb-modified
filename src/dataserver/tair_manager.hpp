/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * dataserver operations abstract layer
 *
 * Version: $Id: tair_manager.hpp 603 2012-03-08 03:28:19Z choutian.xmm@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_MANAGER_H
#define TAIR_MANAGER_H

#include <sys/time.h>
#include <sys/resource.h>

#include <tbsys.h>
#include <tbnet.h>
#include <boost/dynamic_bitset.hpp>
#include "define.hpp"
#include "dump_data_info.hpp"
#include "util.hpp"
#include "data_entry.hpp"
#include "storage_manager.hpp"
#include "dump_manager.hpp"
#include "dump_filter.hpp"
#include "table_manager.hpp"
#include "duplicate_manager.hpp"
#include "update_log.hpp"
#include "stat_helper.hpp"
#include "plugin_manager.hpp"

namespace tair {
   enum {
      ELEMENT_TYPE_INT = 0, //int 32bit
      ELEMENT_TYPE_LLONG,// long long 64 bit
      ELEMENT_TYPE_DOUBLE,//double
      ELEMENT_TYPE_STRING,//string
      ELEMENT_TYPE_INVALID,
   };
   using namespace tair::common;
   const int ITEM_HEAD_LENGTH = 2;
   class migrate_manager;
   class tair_manager {
      enum {
         STATUS_NOT_INITED,
         STATUS_INITED,
         STATUS_CAN_WORK,
      };

      const static int mutex_array_size = 1000;

#define METHOD_HGETALL  0
#define METHOD_HKEYS    1
#define METHOD_HVALS    2

   public:
      tair_manager();
      ~tair_manager();

      bool initialize(tbnet::Transport *transport, tbnet::DefaultPacketStreamer *streamer);

      /* common operator */
      int info (data_entry & info);
      int ttl (int area, data_entry & key, long long* time_remain);
      int type (int area, data_entry & key, long long* what_type);
      int expire (int area, data_entry & key, int32_t expiretime);
      int expireat (int area, data_entry & key, int32_t expiretime);
      int exists (int bucket_number, data_entry & key);
      int persist (int area, data_entry & key);
      int remove (int area, data_entry & key);
      int clear (int area);
      int lazyclear(int area);
      int dumparea(int area);
      int loadarea(int area);
      int addfilter (int area, data_entry & key, data_entry & field, data_entry & value);
      int removefilter(int area, data_entry & key, data_entry & field, data_entry & value);

      /* string operator */
      int put(int area, data_entry & key, data_entry & value, int expire_time);
      int putnx(int area, data_entry & key, data_entry & value, int expire_time);
      int add_count(int area, data_entry & key, int count, int init_value,
  		   int *result_value, int expire_time = 0);
      int get(int area, data_entry & key, data_entry & value);
      int getset(int area, data_entry & key, data_entry & value, data_entry & oldvalue,
              uint16_t version, int expire_time);

      /* list operator */
      int llen(int area, data_entry & key, long long *retlen);

      int lrpush(int area, data_entry& key, vector<data_entry *> &value,
  		uint32_t* oklen, uint32_t* list_len, int pcode, int max_count,
          uint16_t version, int expire_time);

      int lrpop (int area, data_entry & key, int popnum, vector<data_entry *>& value,
              int pcode, uint16_t version, int expire_time);

      int lindex (int area, data_entry & key, int32_t index,
  		data_entry & value);

      int ltrim (int area, data_entry & key, int32_t start, int32_t end,
              uint16_t version, int expire_time);

      int lrem (int area, data_entry & key, int32_t count, data_entry & val,
  	      long long *retnum, uint16_t version, int expire_time);

      int lrange (int area, data_entry & key, int32_t start, int32_t end,
  		vector<data_entry *>& values);

      /* hset */
      int hexists (int area, data_entry & key,
              data_entry & field);

      int hgetall (int area, data_entry & key,
  		 vector<data_entry *>& field_val);

      int hincrby (int area, data_entry & key, data_entry & field,
  		 long long & value, long long *retnum, uint16_t version, int expire_time);

      int hmset (int area, data_entry& key, vector<data_entry *>& field_val,
              int* retvalue, uint16_t version, int expire_time);

      int hset (int area, data_entry & key, data_entry & field,
  	      data_entry & val, uint16_t version, int expire_time);

      int hsetnx (int area, data_entry & key, data_entry & field,
  		data_entry & val, uint16_t version, int expire_time);

      int hget (int area, data_entry & key, data_entry & field,
  	      data_entry & val);

      int hmget (int area, data_entry & key, vector < data_entry * >&fielies,
  	       vector < data_entry * >&vals);

      int hkeys (int area, data_entry & key, vector < data_entry * >&vals);

      int hvals (int area, data_entry & key, vector < data_entry * >&vals);

      int hdel (int area, data_entry & key, data_entry & field,
              uint16_t version, int expire_time);

	  int hlen(int area, data_entry & key, long long *retlen);

      /* set */
      int scard (int area, data_entry & key, long long * retnum);

      int smembers (int area, data_entry & key, vector<data_entry *>& values);

      int sadd (int area, data_entry & key, data_entry & val,
              uint16_t version, int expire_time);

      int spop (int area, data_entry & key, data_entry & val,
              uint16_t version, int expire_time);

      int srem (int area, data_entry & key, data_entry & val, uint16_t version, int expire_time);

      /* zset */
      int zscore (int area, data_entry & key, data_entry & val, double *score);

      int zrange (int area, data_entry & key, int32_t start, int end,
  		vector<data_entry *> &vals, vector<double> &scores, int withscore);

      int zrevrange (int area, data_entry & key, int32_t start, int32_t end,
  		   vector <data_entry * > &vals, vector<double> &scores, int withscore);

      int zrangebyscore (int area, data_entry & key, double start, double end,
  		       vector <data_entry *> &vals, vector<double> &scores, int limit, int withscore);

      int zrevrangebyscore (int area, data_entry & key, double start, double end,
  		       vector <data_entry *> &vals, vector<double> &scores, int limit, int withscore);

      int zadd (int area, data_entry & key, double score, data_entry & val,
              uint16_t version, int expire_time);

      int zrank (int area, data_entry & key, data_entry & val, long long * rank);

	  int zrevrank (int area, data_entry & key, data_entry & val, long long * rank);

	  int zcount(int area, data_entry & key, double start, double end, long long *retnum);

      int zincrby (int area, data_entry & key, data_entry & value,
  		 double & addscore, double *retnum, uint16_t version, int expire_time);

      int zcard (int area, data_entry & key, long long * retnum);

      int zrem (int area, data_entry & key, data_entry & val, uint16_t version, int expire_time);

	  int zremrangebyrank (int area, data_entry & key, int start, int end,
			  uint16_t version, int expire_time, long long *remnum);

	  int zremrangebyscore (int area, data_entry & key, double start, double end,
			  uint16_t version, int expire_time, long long *remnum);

      int direct_put(data_entry &key, data_entry &value);
      int direct_remove(data_entry &key);


      bool is_migrating();
      bool should_proxy(data_entry &key, uint64_t &targetServerId);
      bool should_proxy(uint32_t hashcode, uint64_t &target_server_id);
      void set_migrate_done(int bucketNumber);
      void set_area_quota(int area,uint64_t quota) {
         storage_mgr->set_area_quota(area,quota);
      }
      void get_slaves(int server_flag, int bucket_number, vector<uint64_t> &slaves);

      void set_solitary(); // make this data server waitting for next direction
      bool is_working();

      void update_server_table(uint64_t *server_table, int server_table_size, uint32_t server_table_version,
              int32_t data_need_remove, vector<uint64_t> &current_state_table, uint32_t copy_count, uint32_t bucket_count);

      void get_proxying_buckets(vector<uint32_t> &buckets);

      void do_dump(set<dump_meta_info> dump_meta_infos);

      bool is_localmode();

   public:
      plugin::plugins_manager plugins_manager;
   private:
      int genericHgetall (int area, data_entry & key, vector<data_entry *>& field_val, int method);
   private:
      tair::storage::storage_manager *get_storage_manager(data_entry &key);
      uint32_t get_bucket_number(data_entry &key);
      uint32_t get_bucket_number(uint32_t hashcode);
      bool should_write_local(int bucket_number, int server_flag, int op_flag, int &rc);
      bool need_do_migrate_log(int bucket_number);
      int get_op_flag(int server_flag);
      void init_migrate_log();
      int get_mutex_index(data_entry &key);

   private:
      bool localmode;
      int status;
      tbsys::CThreadMutex counter_mutex[mutex_array_size];
      tbsys::CThreadMutex item_mutex[mutex_array_size];

      tbsys::CThreadMutex update_server_table_mutex;
      tair::storage::storage_manager *storage_mgr;
      table_manager *table_mgr;
      duplicate_sender_manager *duplicator;
      migrate_manager *migrate_mgr;
      boost::dynamic_bitset<> migrate_done_set;
      update_log *migrate_log;
      tair::storage::dump_manager *dump_mgr;
   };
}
#endif
