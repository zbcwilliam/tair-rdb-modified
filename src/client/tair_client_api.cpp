/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tair_client_api.cpp 562 2012-02-08 09:44:00Z choutian.xmm@taobao.com $
 *
 * Authors:
 *   MaoQi <maoqi@taobao.com>
 *
 */
#include "tair_client_api.hpp"
#include "tair_client_api_impl.hpp"
#include <iterator>
namespace tair {

  /*-----------------------------------------------------------------------------
   *  tair_client_api
   *-----------------------------------------------------------------------------*/

  tair_client_api::tair_client_api()
  {
    impl = new tair_client_impl();
  }

  tair_client_api::~tair_client_api()
  {
    delete impl;
  }

  bool tair_client_api::startup(const char *master_addr,const char *slave_addr,const char *group_name)
  {
    return impl->startup(master_addr,slave_addr,group_name);
  }

  void tair_client_api::close()
  {
    impl->close();
  }

  int tair_client_api::remove_area(int area)
  {
    return impl->remove_area(area, true);
  }

  int tair_client_api::put(int area,
      const data_entry &key,
      const data_entry &data,
      int expire,
      int version)
  {
    return impl->put(area,key,data,expire,version);
  }

  int tair_client_api::get(int area,
      const data_entry &key,
      data_entry*& data)
  {
    return impl->get(area,key,data);
  }

  int tair_client_api::mget(int area,
      vector<data_entry *> &keys,
      tair_keyvalue_map& data)
  {
    return impl->mget(area,keys,data);
  }

  int tair_client_api::remove(int area,
      const data_entry &key)
  {
    return impl->remove(area,key);
  }

  int tair_client_api::mdelete(int area,
      vector<data_entry *> &keys)
  {
    return impl->mdelete(area,keys);
  }

  int tair_client_api::minvalid(int area,
      vector<data_entry *> &keys)
  {
    return impl->mdelete(area,keys);
  }


  int tair_client_api::lpush(int area, data_entry &key, vector<data_entry*> &values,
          int expire, int version, long &successlen)
  {
    return impl->lpush(area, key, values, expire, version, successlen);
  }

  int tair_client_api::rpush(int area, data_entry &key, vector<data_entry*> &values,
          int expire, int version, long &successlen)
  {
    return impl->rpush(area, key, values, expire, version, successlen);
  }

  int tair_client_api::lpop(int area, data_entry &key, int count, vector<data_entry*> &values,
          int expire, int version)
  {
    return impl->lpop(area, key, count, values, expire, version);
  }

  int tair_client_api::rpop(int area, data_entry &key, int count, vector<data_entry*> &values,
          int expire, int version)
  {
    return impl->rpop(area, key, count, values, expire, version);
  }

  int tair_client_api::lindex(int area, data_entry &key, int index, data_entry &value)
  {
    return impl->lindex(area, key, index, value);
  }

  int tair_client_api::scard(const int area,const data_entry & key,long long & retnum) 
  {
    return impl->zcard(area, key, retnum);
  }
  int tair_client_api::sadd(const int area, const data_entry &key, const data_entry &value,
          const int expire, const int version) {
    return impl->sadd(area, key, value, expire, version);
  }

  int tair_client_api::smembers(const int area, const data_entry &key, vector<data_entry*> &values) {
    return impl->smembers(area, key, values);
  }

  int tair_client_api::srem(const int area, const data_entry &key, const data_entry &value,
          const int expire, const int version) {
    return impl->srem(area, key, value, expire, version);
  }


  int tair_client_api::hset(const int area, const data_entry &key, const data_entry &field,
          const data_entry &value, const int expire, const int version) {
    return impl->hset(area, key, field, value, expire, version);
  }

  int tair_client_api::hget(const int area, const data_entry &key, const data_entry &field,
          data_entry &value) {
    return impl->hget(area, key, field, value);
  }

  int tair_client_api::hmset(const int area, const data_entry &key,
          const map<data_entry*, data_entry*> &field_values, map<data_entry*, data_entry*> &field_values_success,
          const int expire, const int version) {
    return impl->hmset(area, key, field_values, field_values_success, expire, version);
  }

  int tair_client_api::hgetall(const int area, const data_entry &key, map<data_entry*, data_entry*> &field_values) {
    return impl->hgetall(area, key, field_values);
  }

  int tair_client_api::hmget(const int area, const data_entry &key, const vector<data_entry*> &fields,
          vector<data_entry*> &values) {
    return impl->hmget(area, key, fields, values);
  }

  int tair_client_api::hdel(const int area, const data_entry &key, const data_entry &field,
          const int expire, const int version) {
    return impl->hdel(area, key, field, expire, version);
  }
	//zcard added at 6.29
   int tair_client_api::zcard(const int area, const data_entry &key, long long &retnum) {
    return impl->zcard(area, key, retnum);
  }

  int tair_client_api::zadd(const int area, const data_entry &key, const double score, const data_entry &value,
          const int expire, const int version) {
    return impl->zadd(area, key, score, value, expire, version);
  }
  //zrem added at 6.30
  int tair_client_api::zrem(const int area, const data_entry &key, const data_entry &value,
          const int expire, const int version) {
    return impl->zrem(area, key, value, expire, version);
  }

  //zrevrange added at 6.30
  int tair_client_api::zrevrange (const int area, const data_entry & key, int32_t start,int32_t end,
          vector <data_entry *> &values, vector<double> &scores,int32_t withscore) {
    return impl->zrevrange(area, key, start, end, values, scores,withscore);
  }
  
  //zrange added by zbcwilliam
  int tair_client_api::zrange (const int area, const data_entry & key, int32_t start,int32_t end,
          vector <data_entry *> &values, vector<double> &scores,int32_t withscore) {
    return impl->zrange(area, key, start, end, values, scores,withscore);
  } 
  int tair_client_api::zrangebyscore (const int area, const data_entry & key, const double start,const double end,
          vector <data_entry *> &vals, vector<double> &scores,const int limit,const int withscore) {
    return impl->zrangebyscore(area, key, start, end, vals, scores,limit,withscore);
  }

  int tair_client_api::incr(int area,
      const data_entry& key,
      int count,
      int *ret_count,
      int init_value/* = 0*/,
      int expire /*= 0*/)
  {
    if(area < 0 || count < 0 || expire < 0){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    return impl->add_count(area,key,count,ret_count,init_value,expire);

  }
  int tair_client_api::decr(int area,
      const data_entry& key,
      int count,
      int *ret_count,
      int init_value/* = 0*/,
      int expire /*= 0*/)
  {
    if(area < 0 || count < 0 || expire < 0){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    return impl->add_count(area,key,-count,ret_count,init_value,expire);

  }



  int tair_client_api::add_count(int area,
      const data_entry &key,
      int count,
      int *ret_count,
      int init_value /*= 0*/)
  {

    return impl->add_count(area,key,count,ret_count,init_value);

  }

  void tair_client_api::set_timeout(int timeout)
  {
    impl->set_timeout(timeout);
  }

  const char *tair_client_api::get_error_msg(int ret)
  {
    return impl->get_error_msg(ret);
  }

  uint32_t tair_client_api::get_bucket_count() const
  {
    return impl->get_bucket_count();
  }
  uint32_t tair_client_api::get_copy_count() const
  {
    return impl->get_copy_count();
  }
  void tair_client_api::get_server_with_key(const data_entry& key,std::vector<std::string>& servers) const
  {
    return impl->get_server_with_key(key,servers);
  }
} /* tair */
