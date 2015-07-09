/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * kyotocabinet storage engine
 *
 * Version: $Id$
 *
 * Authors:
 *   yexiang <yexiang.ych@taobao.com>
 *
 */

#ifndef RDB_MANAGER_H
#define RDB_MANAGER_H

#include "storage/storage_manager.hpp"
#include "common/data_entry.hpp"
#include "common/stat_info.hpp"
#include "redis_define.h"

struct redisConfig;

namespace tair
{
namespace storage
{
namespace rdb
{

class redis_db;

class rdb_manager : public tair::storage::storage_manager
{
#define METHOD_HGETALL    0
#define METHOD_HKEYS      1
#define METHOD_HVALS      2
public:
    rdb_manager();
    virtual ~rdb_manager();

    //commin operator
    int expire(int bucket_number, data_entry & key, int expiretime);

    int expireat(int bucket_number, data_entry & key, int expiretime);

    int persist(int bucket_number, data_entry & key);

    int remove(int bucket_number, data_entry & key, bool version_care);

    int ttl(int bucket_number, data_entry & key, long long* time_remain);

    int type(int bucket_number, data_entry & key, long long* what_type);

    int exists(int bucket_number, data_entry & key);

    int addfilter(int bucket_number, data_entry & key, data_entry & field, data_entry & value);

    int removefilter(int bucket_number, data_entry & key, data_entry & field, data_entry & value);

    int lazyclear(int area);

    int dumparea(int area);

    int loadarea(int area);

    int clear(int area)
    {
        return 0;
    }

    //string operator
    int put(int bucket_number, data_entry & key, data_entry & value,
              bool version_care, int expire_time);

    int putnx(int bucket_number, data_entry & key, data_entry & value,
              bool version_care, int expire_time);

    int get(int bucket_number, data_entry & key, data_entry & value);

    int getset(int bucket_number, data_entry & key, data_entry & value,
                data_entry & oldvalue, bool version_care, uint16_t version, int expire_time);

    int incdecr(int bucket_number, data_entry & key, int init_value, int addvalue, int * retvalue,
                bool version_care, uint16_t version, int expire_time);
    //list operator
    int lrpop(int bucket_number, data_entry & key, int count, std::vector<data_entry *>& values,
            tair::LeftOrRight lr, bool version_care, uint16_t version, int expire_time);

    int lrpush(int bucket_number, data_entry & key, const std::vector<data_entry* >& values,
            uint32_t* oklen, uint32_t* list_len, tair::LeftOrRight lr, tair::ExistOrNot en,
            int max_count, bool version_care, uint16_t version, int expire_time);

    int lindex(int bucket_number, data_entry &key, int32_t index, data_entry& value);

    int lrange(int bucket_number, data_entry &key, int start, int end,
              std::vector<data_entry *>& values);

    int lrem(int bucket_number, data_entry &key, int count, data_entry& value,
            long long *retnum, bool version_care, uint16_t version, int expire_time);

    int llen(int bucket_number, data_entry &key, long long* len);

    int linsert(int bucket_number, data_entry &key, data_entry &value_in_list,
            data_entry &value_insert, tair::BeforeOrAfter ba,
            bool version_care, uint16_t version, int expire_time);

    int lset(int bucket_number, data_entry &key, int index, data_entry &value,
            bool version_care, uint16_t version, int expire_time);

    int ltrim(int bucket_number, data_entry &key, int start, int end,
            bool version_care, uint16_t version, int expire_time);

    //hset
    int hgetall(int bucket_number, data_entry & key, std::vector<data_entry*> & values);

    int hincrby(int bucket_number, data_entry & key, data_entry & field,
            long long& value, long long *retvalue,
            bool version_care, uint16_t version, int expire_time);

    int hmset(int bucket_number, data_entry & key, std::vector<data_entry*> & field_value, int* retvalue,
            bool version_care, uint16_t version, int expire_time);

    int hset(int bucket_number, data_entry & key, data_entry & field,
            data_entry & value, bool version_care, uint16_t version, int expire_time);

    int hsetnx(int bucket_number, data_entry & key, data_entry & field,
            data_entry & value, bool version_care, uint16_t version, int expire_time);

    int hexists(int bucket_number, data_entry & key, data_entry & field);

    int hget(int bucket_number, data_entry & key, data_entry & field,
            data_entry & value);

    int hmget(int bucket_number, data_entry & key, std::vector<data_entry*> & field,
            std::vector<data_entry*> & values);

    int hkeys(int bucket_number, data_entry & key, std::vector<data_entry*> & values);

    int hvals(int bucket_number, data_entry & key, std::vector<data_entry*> & values);

    int hdel(int bucket_number, data_entry & key, data_entry & field,
            bool version_care, uint16_t version, int expire_time);

	int hlen(int bucket_number, data_entry &key, long long* len);

    //set
    int scard(int bucket_number, data_entry & key, long long* retnum);

    int smembers(int bucket_number, data_entry & key, std::vector<data_entry*> & values);

    int sadd(int bucket_number, data_entry & key, data_entry & value,
            bool version_care, uint16_t version, int expire_time);

    int srem(int bucket_number, data_entry & key, data_entry & value,
            bool version_care, uint16_t version, int expire_time);

    int spop(int bucket_number, data_entry & key, data_entry & value,
            bool version_care, uint16_t version, int expire_time);

    //zset
    int zscore(int bucket_number, data_entry & key, data_entry & value, double* retstore);

    int zrange(int bucket_number, data_entry & key, int start, int end,
            std::vector<data_entry*> & values, std::vector<double> & scores, int withscore);

    int zrevrange(int bucket_number, data_entry & key, int start, int end,
            std::vector<data_entry*> & values, std::vector<double> & scores, int withscore);

    int zrangebyscore(int bucket_number, data_entry & key, double start, double end,
            std::vector<data_entry*> & values, std::vector<double> & scores, int limit, int withscore);

    int zrevrangebyscore(int bucket_number, data_entry & key, double start, double end,
            std::vector<data_entry*> & values, std::vector<double> & scores, int limit, int withscore);

    int zadd(int bucket_number, data_entry & key, double score, data_entry & value,
            bool version_care, uint16_t version, int expire_time);

    int zrank(int bucket_number, data_entry & key, data_entry & value, long long* rank);

	int zrevrank(int bucket_number, data_entry & key, data_entry & value, long long* rank);

	int zcount(int bucket_number, data_entry & key, double start, double end, long long* retnum);

    int zincrby(int bucket_number, data_entry & key, data_entry & value,
            double& addscore, double *retvalue, bool version_care, uint16_t version, int expire_time);

    int zcard(int bucket_number, data_entry & key, long long* retnum);

	int zrem(int bucket_number, data_entry & key, data_entry & value, bool version_care,
			uint16_t version, int expire_time);

	int zremrangebyrank(int bucket_number, data_entry & key, int start, int end,
		   bool version_care, uint16_t version, int expire_time, long long* remnum);

	int zremrangebyscore(int bucket_number, data_entry & key, double start, double end,
		   bool version_care, uint16_t version, int expire_time, long long* remnum);

    bool init_buckets(const std::vector <int>&buckets);
    void close_buckets(const std::vector <int>&buckets);

    void begin_scan(md_info & info) {}
    void end_scan(md_info & info) {}

    bool get_next_items(md_info & info, std::vector <item_data_info *>&list)
    {
        return false;
    }

    void set_area_quota(int area, uint64_t quota);
    void set_area_quota(std::map<int, uint64_t> &quota_map);

    void get_stats(tair_stat * stat);

private:
    int genericHgetall(int bucket_number, data_entry & key, std::vector<data_entry*> & values, int method);
    int genericZrangebyscore(int bucket_number, data_entry & key, double start, double end,
            std::vector<data_entry*> & values, std::vector<double> & scores, int limit, int withscore, bool reverse);

private:
	void get_redis_config(redisConfig* config);

    long getAbsTime(int expire_time)
    {
        long now = time(NULL);
        long exp = expire_time;
        if (exp <= now) exp += now;
        return exp;
    }

    redis_db *get_redis_instance()
    {
        return redis_instance;
    }
private:
    redis_db * redis_instance;
};


}; // end namespace rdb
}; // end namespace storage
}; // end namespace tari


#endif

