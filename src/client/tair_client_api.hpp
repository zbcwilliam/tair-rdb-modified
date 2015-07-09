/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tair_client_api.hpp 562 2012-02-08 09:44:00Z choutian.xmm@taobao.com $
 *
 * Authors:
 *   MaoQi <maoqi@taobao.com>
 *
 */
#ifndef TAIR_CLIENT_API
#define TAIR_CLIENT_API

#include <vector>
#include <map>
#include "data_entry.hpp"
#include "util.hpp"

namespace tair {

  class tair_client_impl;
  using namespace std;
  using namespace tair::common;

  class tair_client_api
  {
    public:
      tair_client_api();
      ~tair_client_api();

      static const int MAX_ITEMS = 65535;
      static const int ALL_ITEMS = MAX_ITEMS + 1;

      /**
       * @brief connect to tair cluster
       *
       * @param master_addr     master configserver addr [ip:port],default port is 5198
       * @param slave_addr      slave configserver addr [ip:port]
       * @param group_name      group_name
       *
       * @return true -- success,false -- fail
       */
      bool startup(const char *master_addr,const char *slave_addr,const char *group_name);

      /**
       * @brief connect to tair cluster
       *
       * @param master_cfgsvr master configserver ip:port
       * @param slave_cfgsvr  master configserver ip:port
       * @param group_name    group name
       *
       * @return  true -- success,false -- fail
       */
      // bool startup(uint64_t master_cfgsvr,uint64_t slave_cfgsvr,const char *group_name);

      /**
       * @brief close tairclient,release resource
       */
      void close();

      /**
       * @brief put data to tair
       *
       * @param area     namespace
       * @param key      key
       * @param data     data
       * @param expire   expire time,realtive time
       * @param version  version,if you don't care the version,set it to 0
       *
       * @return 0 -- success, otherwise fail,you can use get_error_msg(ret) to get more information.
       */
      int put(int area,
          const data_entry &key,
          const data_entry &data,
          int expire,
          int version);

      /**
       * @brief get data from tair cluster
       *
       * @param area    namespace
       * @param key     key
       * @param data    data,a pointer,the caller must release the memory.
       *
       * @return 0 -- success, otherwise fail.
       */
      int get(int area,
          const data_entry &key,
          data_entry*& data);

      /**
       * @brief get multi  data from tair cluster
       *
       * @param area    namespace
       * @param keys    keys list, vector type
       * @param data    data,data hash map,the caller must release the memory.
       *
       * @return 0 -- success, TAIR_RETURN_PARTIAL_SUCCESS -- partly success, otherwise fail.
       */
      int mget(int area,
          vector<data_entry *> &keys,
          tair_keyvalue_map& data);

      /**
       * @brief  remove data from tair cluster
       *
       * @param area    namespace
       * @param key     key
       *
       * @return  0 -- success, otherwise fail.
       */
      int remove(int area,
          const data_entry &key);

      int remove_area(int area);

      /**
       * @brief remove multi  data from tair cluster
       *
       * @param area    namespace
       * @param keys    key list, vector type
       *
       * @return  0 -- success, TAIR_RETURN_PARTIAL_SUCCESS partly success, otherwise fail.
       */
      int mdelete(int area,
          vector<data_entry*> &key);
      /**
       * @brief invalid multi  data from tair cluster
       *
       * @param area    namespace
       * @param keys    key list, vector type
       *
       * @return  0 -- success, TAIR_RETURN_PARTIAL_SUCCESS partly success, otherwise fail.
       */
      int minvalid(int area,
          vector<data_entry*> &key);

      //lpush
      int lpush(int area, data_entry &key, vector<data_entry*> &values,
              int expire, int version, long &successlen);
      //rpush
      int rpush(int area, data_entry &key, vector<data_entry*> &values,
              int expire, int version, long &successlen);
      //lpop
      int lpop(int area, data_entry &key, int count, vector<data_entry*> &values,
              int expire, int version);
      //rpop
      int rpop(int area, data_entry &key, int count, vector<data_entry*> &values,
              int expire, int version);
      //lindex
      int lindex(int area, data_entry &key, int index, data_entry &value);


      //scard
      int scard(const int area, const data_entry &key, long long &retnum);//added 6.30

	  //sadd
      int sadd(const int area, const data_entry &key, const data_entry &value,
              const int expire, const int version);
      //smembers
      int smembers(const int area, const data_entry &key, vector<data_entry*> &values);
      //srem
      int srem(const int area, const data_entry &key, const data_entry &value,
              const int expire, const int version);


      //hset
      int hset(const int area, const data_entry &key, const data_entry &field,
              const data_entry &value, const int expire, const int version);
      //hget
      int hget(const int area, const data_entry &key, const data_entry &field, data_entry &value);
      //hmset
      int hmset(const int area, const data_entry &key, const map<data_entry*, data_entry*> &field_values,
        map<data_entry*, data_entry*> &field_values_success, const int expire, const int version);
      //hgetall
      int hgetall(const int area, const data_entry &key, map<data_entry*, data_entry*> &field_values);
      //hmget
      int hmget(const int area, const data_entry &key, const vector<data_entry*> &fields, vector<data_entry*> &values);
      //hdel
      int hdel(const int area, const data_entry &key, const data_entry &field, const int expire, const int version);


      /*add by caster , sorted set operation*/
	  int zcard(const int area, const data_entry &key, long long &retnum);//added 6.29
	  
      int zadd(const int area, const data_entry &key,const double score , const data_entry &value,
              const int expire, const int version);
	  
	  //zrem added at 6.30
	  int zrem(const int area, const data_entry &key, const data_entry &value,
              const int expire, const int version);
	  //zrevrange added at 6.30
	  int zrevrange (const int area, const data_entry & key, int32_t start, int32_t end,
          vector <data_entry *> &values, vector<double> &scores,int32_t withscore);
	  
	  //zrange added by zbcwilliam
	  int zrange (const int area, const data_entry & key, int32_t start, int32_t end,
          vector <data_entry *> &values, vector<double> &scores,int32_t withscore); 

      int zrangebyscore (const int area, const data_entry & key, const double start,const double end,
              vector <data_entry *> &vals, vector<double> &scores,const int limit,const int withscore);


      /*end of adding by caster, sorted set operation*/

      /**
       * @brief incr
       *
       * @param area            namespace
       * @param key             key
       * @param count           add (count) to original value.
       * @param retCount                the latest value
       * @param expire          expire time
       * @param initValue               initialize value
       *
       * @return 0 -- success, otherwise fail.
       */
      int incr(int area,
          const data_entry& key,
          int count,
          int *ret_count,
          int init_value = 0,
          int expire = 0);
      // opposite to incr
      int decr(int area,
          const data_entry& key,
          int count,
          int *ret_count,
          int init_value = 0,
          int expire = 0);

      /**
       *
       * ******this method is deprecated,don't use******
       *
       * @brief   add count
       *
       * @param area            namespace
       * @param key             key
       * @param count           add (count) to original value.
       * @param retCount                the latest value
       * @param initValue               the initialize value
       *
       * @return  0 -- success ,otherwise fail.
       */
      int add_count(int area,
          const data_entry &key,
          int count,
          int *ret_count,
          int init_value = 0);

      /**
       *
       * items :  just support four types: int32_t int64_t double string
       *
       * @brief  add items
       *
       * @param area            namespace
       * @param key             key
       * @param data            data,vector<type>
       * @param version         version,the same as put
       * @param expired         expire time ,the same as put
       * @param max_count               how many items do you want to save
       *
       * @return  0 -- success, otherwise fail
       */
#define ADD_ITEMS(type)                                                 \
      int add_items(int area,const data_entry &key,type data,int version,int expired,int max_count=500)

      ADD_ITEMS(std::vector<int>&);
      ADD_ITEMS(std::vector<int64_t>&);
      ADD_ITEMS(std::vector<double>&);
      ADD_ITEMS(std::vector<std::string>&);


      /**
       * @brief get items
       *
       * @param area            namespace
       * @param key             key
       * @param offset          item's offset
       * @param count           the item's count you want to get,must > 0,
       *                                iff you want to get all items,set count to ALL_ITEMS;
       * @param std::vector       result,you should know the item's type
       *
       * @return  0 -- success, otherwise fail
       */
#define GET_ITEMS_FUNC(FuncName,type)                                   \
      int FuncName(int area, const data_entry &key, int offset, int count, type result)

      GET_ITEMS_FUNC(get_items,std::vector<int>&);
      GET_ITEMS_FUNC(get_items,std::vector<int64_t>&);
      GET_ITEMS_FUNC(get_items,std::vector<double>&);
      GET_ITEMS_FUNC(get_items,std::vector<std::string>&);

      /**
       * @brief get and remove items
       *
       * @param area            namespace
       * @param key             key
       * @param offset          item's offset
       * @param count           the item's count you want to get
       *                                if count >= really items count,this key will be deleted.
       * @param std::vector       result,you should know the item's type
       *
       * @return  0 -- success, otherwise fail
       */

      GET_ITEMS_FUNC(get_and_remove,std::vector<int>&);
      GET_ITEMS_FUNC(get_and_remove,std::vector<int64_t>&);
      GET_ITEMS_FUNC(get_and_remove,std::vector<double>&);
      GET_ITEMS_FUNC(get_and_remove,std::vector<std::string>&);

      /**
       * @brief remove items
       *
       * @param area            namespace
       * @param key             key
       * @param offset          the offset of item
       * @param count           the count of item that you want to remove
       *
       * @return  0 -- success, otherwise -- fail
       */
      int remove_items(int area,
          const data_entry &key,
          int offset,
          int count);

      /**
       * @brief get item count
       *
       * @param area            namespace
       * @param key             key
       *
       * @return  if ret > 0,ret is item's count,else failed.
       */
      int get_items_count(int area,const data_entry& key);

      /**
       * @brief set timout of each method
       *
       * @param timeout
       */
      void set_timeout(int timeout);

      /**
       * @brief get bucket count of tair cluster
       *
       * @return  bucket count
       */
      uint32_t get_bucket_count() const;

      /**
       * @brief get copy count of tair cluster
       *
       * @return  copy count
       */
      uint32_t get_copy_count() const;

      const char *get_error_msg(int ret);

      void get_server_with_key(const data_entry& key,std::vector<std::string>& servers) const;

    private:

      tair_client_impl *impl;

  };
}
#endif
