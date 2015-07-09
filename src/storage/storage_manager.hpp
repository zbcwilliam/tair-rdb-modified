/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * storage engine interface
 *
 * Version: $Id: storage_manager.hpp 603 2012-03-08 03:28:19Z choutian.xmm@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_STORAGE_MANAGER_H
#define TAIR_STORAGE_MANAGER_H

#include <stdint.h>
#include <map>
#include "util.hpp"
#include "data_entry.hpp"
#include "define.hpp"
#include "stat_info.hpp"

namespace tair {

enum LeftOrRight
{
    IS_LEFT, IS_RIGHT
};

enum ExistOrNot
{
    IS_NOT_EXIST = 0, //nx
    IS_EXIST = 1,
    IS_NOT_EXIST_AND_EXPIRE = 2,
    IS_EXIST_AND_EXPIRE = 3
};

enum BeforeOrAfter
{
    IS_BEFORE, IS_AFTER
};

typedef struct _migrate_dump_index
{
    uint32_t hash_index;
    uint32_t db_id;
    int is_migrate;
} md_info;

namespace storage
{
using namespace tair::util;
using namespace tair::common;
class storage_manager  {
    public:
        storage_manager():bucket_count(0){}
        virtual ~ storage_manager(){}

        //common operator
        virtual int expire(int bucket_number, data_entry & key, int expiretime)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int expireat(int bucket_number, data_entry & key, int expiretime)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int persist(int bucket_number, data_entry & key)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int ttl(int bucket_number, data_entry & key, long long* time_remain)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int type(int bucket_number, data_entry & key, long long* what_type)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }


        virtual int exists(int bucket_number, data_entry & key)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int lazyclear(int area)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int dumparea(int area)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int loadarea(int area)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int addfilter(int area, data_entry & key, data_entry & field,
                data_entry & value)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int removefilter(int area, data_entry & key, data_entry & field,
                data_entry & value)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int remove(int bucket_number, data_entry & key, bool version_care) = 0;

        //string operator
        virtual int putnx(int bucket_number, data_entry & key, data_entry & value,
                bool version_care, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int put(int bucket_number, data_entry & key, data_entry & value,
                bool version_care, int expire_time) = 0;

        virtual int get(int bucket_number, data_entry & key,
                data_entry & value) = 0;

        virtual int getset(int bucket_number, data_entry & key, data_entry & value,
                data_entry & oldvalue, bool version_care, uint16_t version, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int incdecr(int bucket_number,data_entry & key,
                int init_value, int addvalue, int * retvalue,
                bool version_care, uint16_t version, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        //list operator
        virtual int lrpop(int bucket_number, data_entry &key, int count,
                std::vector<data_entry *>& values, LeftOrRight lr,
                bool version_care, uint16_t version, int expire_time) {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }
        virtual int lrpush(int bucket_number, data_entry &key, const std::vector<data_entry *>& values,
                uint32_t* oklen, uint32_t* list_len, LeftOrRight lr, ExistOrNot en,
                int max_count, bool version_care, uint16_t version, int expire_time) {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }
        virtual int lindex(int bucket_number, data_entry &key, int32_t index, data_entry& value)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }
        virtual int lrange(int bucket_number, data_entry &key, int start, int end,
                std::vector<data_entry *>& values)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }
        virtual int lrem(int bucket_number, data_entry &key, int count,
                data_entry& value, long long* retnum,
                bool version_care, uint16_t version, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }
        virtual int llen(int bucket_number, data_entry &key, long long *len)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }
        virtual int linsert(int bucket_number, data_entry &key, data_entry &value_in_list,
                data_entry &value_insert, BeforeOrAfter ba,
                bool version_care, uint16_t version, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int lset(int bucket_number, data_entry &key, int index,
                data_entry &value, bool version_care, uint16_t version, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int ltrim(int bucket_number, data_entry &key, int start, int end,
                bool version_care, uint16_t version, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        //hset
        virtual int hgetall(int bucket_number, data_entry & key, std::vector<data_entry*> & values)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int hincrby(int bucket_number, data_entry & key, data_entry & field,
                long long & value, long long *retvalue,
                bool version_care, uint16_t version, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int hmset(int bucket_number, data_entry & key, std::vector<data_entry*> & field_value,
                int* retvalue, bool version_care, uint16_t version, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int hset(int bucket_number, data_entry & key, data_entry & field,
                data_entry & value, bool version_care, uint16_t version, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int hsetnx(int bucket_number, data_entry & key, data_entry & field,
                data_entry & value, bool version_care, uint16_t version, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int hexists(int bucket_number, data_entry & key, data_entry & field)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int hget(int bucket_number, data_entry & key, data_entry & field,
                data_entry & value)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int hmget(int bucket_number, data_entry & key, std::vector<data_entry*> & field,
                std::vector<data_entry*> & values) {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int hkeys(int bucket_number, data_entry & key, std::vector<data_entry*> & values)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int hvals(int bucket_number, data_entry & key, std::vector<data_entry*> & values)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        //should need version and flag
        virtual int hdel(int bucket_number, data_entry & key, data_entry & field,
                bool version_care, uint16_t version, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

		virtual int hlen(int bucket_number, data_entry &key, long long* len)
		{
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
		}

        //set
        virtual int scard(int bucket_number, data_entry & key, long long* retnum)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int smembers(int bucket_number, data_entry & key, std::vector<data_entry*> & values)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int sadd(int bucket_number, data_entry & key, data_entry & value,
                bool version_care, uint16_t version, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int spop(int bucket_number, data_entry & key, data_entry & valuei,
                bool version_care, uint16_t version, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int srem(int bucket_number, data_entry & key, data_entry & value,
                bool version_care, uint16_t version, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        //zset
        virtual int zscore(int bucket_number, data_entry & key, data_entry & value, double* retstore)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int zrange(int bucket_number, data_entry & key, int start, int end,
                std::vector<data_entry*> & values, std::vector<double> &scores, int withscore)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int zrevrange(int bucket_number, data_entry & key, int start, int end,
                std::vector<data_entry*> & values, std::vector<double> &scores, int withscore)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int zrangebyscore(int bucket_number, data_entry & key, double start, double end,
                std::vector<data_entry*> & values, std::vector<double> & scores, int limit, int withscore)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int zrevrangebyscore(int bucket_number, data_entry & key, double start, double end,
                std::vector<data_entry*> & values, std::vector<double> & scores, int limit, int withscore)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int zadd(int bucket_number, data_entry & key, double score, data_entry & value,
                bool version_care, uint16_t version, int expire_time)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

        virtual int zrank(int bucket_number, data_entry & key, data_entry & value, long long* rank)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

		virtual int zrevrank(int bucket_number, data_entry & key, data_entry & value, long long* rank)
		{
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
		}

	    virtual int zcount(int bucket_number, data_entry & key, double start, double end, long long* retnum)
		{
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
		}

        virtual int zincrby(int bucket_number, data_entry & key, data_entry & value,
				double& addscore, double *retvalue, bool version_care, uint16_t version, int expire_time)
		{
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
		}

        virtual int zcard(int bucket_number, data_entry & key, long long* retnum)
        {
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
        }

		virtual int zrem(int bucket_number, data_entry & key, data_entry & value,
				bool version_care, uint16_t version, int expire_time)
		{
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
		}

		virtual int zremrangebyrank(int bucket_number, data_entry & key, int start,
			   	int end, bool version_care, uint16_t version, int expire_time, long long* remnum)
		{
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
		}

		virtual int zremrangebyscore(int bucket_number, data_entry & key, double start,
			   	double end, bool version_care, uint16_t version, int expire_time, long long* remnum)
		{
            return TAIR_RETURN_IS_NOT_IMPLEMENT;
		}

        virtual int clear(int area) = 0;

        virtual bool init_buckets(const std::vector<int> &buckets) = 0;

        virtual void close_buckets(const std::vector<int> &buckets) = 0;
        virtual bool get_next_items(md_info & info,
                std::vector<item_data_info *> &list) = 0;
        virtual void begin_scan(md_info & info) = 0;
        virtual void end_scan(md_info & info) = 0;

        virtual void get_stats(tair_stat * stat) = 0;

        virtual void set_area_quota(int area, uint64_t quota) = 0;
        virtual void set_area_quota(std::map<int, uint64_t> &quota_map) = 0;

        void set_bucket_count(uint32_t bucket_count)
        {
            if(this->bucket_count != 0)
                return;                //can not rest bucket count
            this->bucket_count = bucket_count;
            return;
        }
          protected:
              uint32_t bucket_count;

      };
  }
}

#endif
