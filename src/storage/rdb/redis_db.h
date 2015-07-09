#ifndef REDIS_DB_H
#define REDIS_DB_H

#include "redis_define.h"
#include "redis_db_context.h"
#include "redis_db_session.h"

BEGIN_NS
/**
 * one context can hold multi session
 * in the case one to one
 */
class redis_db
{
#define REDIS_DB_KEY_VERIFY do {        \
    if(key_len <= 0)                    \
    {                                   \
        return REDIS_ERR_LENGTHZERO;    \
    }                                   \
}while(0)

#define REDIS_DB_KEY_ITEM_VERIFY do {   \
    if(item == NULL ||                  \
        item->data_len <= 0 ||          \
        key_len <= 0)                   \
	{                                   \
        return REDIS_ERR_LENGTHZERO;    \
    }                                   \
}while(0)

#define REDIS_DB_KEY_VALUE_VERIFY do {  \
    if(key_len <= 0 ||                  \
        value == NULL ||                \
        value->data_len <= 0)           \
    {                                   \
        return REDIS_ERR_LENGTHZERO;    \
    }                                   \
}while(0)

#define REDIS_DB_KEY_VALUE_S_VERIFY do { \
    if(key_len <= 0 || value_len <= 0)   \
    {                                    \
        return REDIS_ERR_LENGTHZERO;     \
    }                                    \
}while(0)

#define REDIS_DB_KEY_ITEMS_VERIFY do {  \
    if(key_len <= 0 || item_num <= 0)   \
    {                                   \
        return REDIS_ERR_LENGTHZERO;    \
    }                                   \
}while(0)

#define REDIS_DB_KEY_FIELD_S_VERIFY do { \
    if(key_len <= 0 || field_len <= 0)   \
    {                                    \
        return REDIS_ERR_LENGTHZERO;     \
    }                                    \
}while(0)

#define REDIS_DB_KEY_FIELD_VERIFY do {   \
    if(key_len <= 0 || field == NULL ||  \
            field->data_len <= 0)        \
    {                                    \
        return REDIS_ERR_LENGTHZERO;     \
    }                                    \
}while(0)

#define REDIS_DB_KEY_FIELDS_VERIFY do {  \
    if(key_len <= 0 || field_len <= 0)  \
    {                                   \
        return REDIS_ERR_LENGTHZERO;    \
    }                                   \
}while(0)

#define get_redis_db(_area, _hashcode) ((_area) * context->get_unit_num() + (_hashcode) % context->get_unit_num())

public:
    redis_db()
    {
        context = new redis_db_context();
        context->start();
        session = new redis_db_session(*context);
    }

	redis_db(redisConfig &config)
	{
		context = new redis_db_context(config);
		context->start();
		session = new redis_db_session(*context);
	}

    virtual ~redis_db()
    {
        context->stop(); //block current thread
        delete session;
        session = NULL;
        delete context;
        context = NULL;
    }

/*common*/

    virtual int ttl(MKEY, long long* time_remain)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->ttl(IKEY, time_remain);
    }

    virtual int type(MKEY, long long* what_type)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->type(IKEY, what_type);
    }

    virtual int expire(MKEY, long expiretime)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->expire(IKEY, expiretime);
    }

    virtual int persist(MKEY)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->persist(IKEY);
    }

    virtual int exists(MKEY)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->exists(IKEY);
    }

    virtual int remove(MKEY, int version_care)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->del(IKEY, version_care);
    }

    virtual int lazyflushdb(uint32_t area) {
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for(uint32_t i = start; i < end; i++) {
            session->lazyflushdb(i);
        }
        return REDIS_OK;
    }

    virtual int dumparea(uint32_t area) {
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for(uint32_t i = start; i < end; i++) {
            session->dumpDb(i);
        }
        return REDIS_OK;
    }

    virtual int loadarea(uint32_t area) {
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for(uint32_t i = start; i < end; i++) {
            session->loadDb(i);
        }
        return REDIS_OK;
    }

    virtual int addfilter(int area, value_item* key_pattern, value_item* field_pattern,
            value_item* value_pattern) {
        if (key_pattern == NULL && field_pattern == NULL && value_pattern == NULL) {
            return REDIS_ERR_LENGTHZERO;
        }

        char* key_data = (key_pattern == NULL ? NULL : key_pattern->data);
        int key_data_len = (key_pattern == NULL ? 0 : key_pattern->data_len);
        char* field_data = (field_pattern == NULL ? NULL : field_pattern->data);
        int field_data_len = (field_pattern == NULL ? 0 : field_pattern->data_len);
        char* val_data = (value_pattern == NULL ? NULL : value_pattern->data);
        int val_data_len = (value_pattern == NULL ? 0 : value_pattern->data_len);

        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for(uint32_t i = start; i < end; i++) {
            session->addfilter(i, key_data, key_data_len,
                    field_data, field_data_len,
                    val_data, val_data_len);
        }
        return REDIS_OK;
    }

    virtual int removefilter(int area, value_item* key_pattern, value_item* field_pattern,
            value_item* value_pattern) {
        if (key_pattern == NULL && field_pattern == NULL && value_pattern == NULL) {
            return REDIS_ERR_LENGTHZERO;
        }

        char* key_data = (key_pattern == NULL ? NULL : key_pattern->data);
        int key_data_len = (key_pattern == NULL ? 0 : key_pattern->data_len);
        char* field_data = (field_pattern == NULL ? NULL : field_pattern->data);
        int field_data_len = (field_pattern == NULL ? 0 : field_pattern->data_len);
        char* val_data = (value_pattern == NULL ? NULL : value_pattern->data);
        int val_data_len = (value_pattern == NULL ? 0 : value_pattern->data_len);

        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for(uint32_t i = start; i < end; i++) {
            session->removefilter(i, key_data, key_data_len,
                    field_data, field_data_len,
                    val_data, val_data_len);
        }
        return REDIS_OK;
    }

/*string*/
    virtual int put(MKEY, MDETA, value_item* item, EORN)
    {
        REDIS_DB_KEY_ITEM_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->set(IKEY, IDETA, item, en);
    }

    virtual int get(MKEY, OITEMVSN)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->get(IKEY, BITEMVSN);
	}

    virtual int getset(MKEY, MDETA, value_item* new_item, OITEMVSN)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->getset(IKEY, IDETA, new_item, BITEMVSN);
    }

    virtual int incdecr(MKEY, MDETA, int init_value, int addvalue, int *retvalue)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->incdecr(IKEY, IDETA, init_value, addvalue, retvalue);
    }

/*list*/
    virtual int lrpush(MKEY, MDETA, MITEMSVNS, LORR, EORN, int max_count, push_return_value *prv)
    {
        REDIS_DB_KEY_ITEMS_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->lrpush(IKEY, IDETA, IITEMSVNS, lr, en, max_count, prv);
    }

    virtual int lrpop(MKEY, MDETA, int count, LORR, OITEMSVSN)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->lrpop(IKEY, IDETA, count, lr, BITEMSVSN);
    }

    virtual int lindex(MKEY, int index, OITEMVSN)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->lindex(IKEY, index, BITEMVSN);
    }

    virtual int lrange(MKEY, int start, int end, OITEMSVSN)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->lrange(IKEY, start, end, BITEMSVSN);
    }

    virtual int lrem(MKEY, MDETA, int count, value_item* value, long long* remlen)
    {
        REDIS_DB_KEY_VALUE_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->lrem(IKEY, IDETA, count, value, remlen);
    }

    virtual int llen(MKEY, long long *len)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->llen(IKEY, len);
    }

    virtual int linsert(MKEY, MDETA, BORA, char* value_in_list, size_t value_in_list_len,
            value_item* value_insert)
    {
      if(key_len <= 0 || value_in_list_len <= 0 || value_insert == NULL
              || value_insert->data_len <= 0)
      {
          return REDIS_ERR_LENGTHZERO;
      }

      area = get_redis_db(area, generic_hash(key,key_len));
      return session->linsert(IKEY, IDETA, ba, value_in_list, value_in_list_len,
              value_insert);
    }

    virtual int lset(MKEY, MDETA, int index, value_item* value)
    {
        REDIS_DB_KEY_VALUE_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->lset(IKEY, IDETA, index, value);
    }

    virtual int ltrim(MKEY, MDETA, int start, int end)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->ltrim(IKEY, IDETA, start,end);
    }

/*hset*/
    virtual int hgetall(MKEY, OITEMSVSN)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->hgetall(IKEY, BITEMSVSN);
    }

    virtual int hkeys(MKEY, OITEMSVSN)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->hkeys(IKEY, BITEMSVSN);
    }

    virtual int hexists(MKEY, CFIELD)
    {
        REDIS_DB_KEY_FIELD_S_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->hexists(IKEY, IFIELD);
    }

    virtual int hget(MKEY, CFIELD, OITEMVSN)
    {
        REDIS_DB_KEY_FIELD_S_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->hget(IKEY, IFIELD, BITEMVSN);
    }

    virtual int hincrby(MKEY, MDETA, value_item* field, int addvalue, long long * retvalue)
    {
        REDIS_DB_KEY_FIELD_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->hincrby(IKEY, IDETA, field, addvalue, retvalue);
    }

    virtual int hmget(MKEY, MFIELD, OITEMSVSN)
    {
        REDIS_DB_KEY_FIELDS_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->hmget(IKEY, IFIELD, BITEMSVSN);
    }

    virtual int hset(MKEY, MDETA, CFIELD, value_item* item)
    {
        if(key_len <= 0 || field_len <= 0 || item == NULL || item->data_len <= 0)
        {
            return REDIS_ERR_LENGTHZERO;
        }
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->hset(IKEY, IDETA, IFIELD, item);
    }

    virtual int hmset(MKEY, MDETA, value_item* field_value, int field_val_len, int* retvalue)
    {
        //field_val_len % 2 = 0
        if(key_len <= 0 || field_val_len <= 0 || field_val_len & 1 == 1
                || field_value == NULL || field_value->data_len <= 0)
        {
            return REDIS_ERR_LENGTHZERO;
        }
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->hmset(IKEY, IDETA, field_value, field_val_len, retvalue);
    }

    virtual int hsetnx(MKEY, MDETA, CFIELD, value_item* item)
    {
        if(key_len <= 0 || field_len <= 0 || item == NULL || item->data_len <= 0)
        {
            return REDIS_ERR_LENGTHZERO;
        }
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->hsetnx(IKEY, IDETA, IFIELD, item);
    }

    virtual int hvals(MKEY, OITEMSVSN) {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->hvals(IKEY, BITEMSVSN);
    }

    virtual int hdel(MKEY, MDETA, CFIELD)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->hdel(IKEY, IDETA, IFIELD);
    }

    virtual int hlen(MKEY, long long *len)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->hlen(IKEY, len);
    }
/*set*/
    virtual int scard(MKEY, long long* retnum)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->scard(IKEY, retnum);
    }

    virtual int smembers(MKEY, OITEMSVSN)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->smembers(IKEY, BITEMSVSN);
    }

    virtual int sadd(MKEY, MDETA, value_item* item)
    {
        REDIS_DB_KEY_ITEM_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->sadd(IKEY, IDETA, item);
    }

    virtual int srem(MKEY, MDETA, value_item* item)
    {
        REDIS_DB_KEY_ITEM_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->srem(IKEY, IDETA, item);
    }

    virtual int spop(MKEY, MDETA, OITEMVSN)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->spop(IKEY, IDETA, BITEMVSN);
    }
/*zset*/
    virtual int zscore(MKEY, char* value, size_t value_len, double* retscore)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->zscore(IKEY, value, value_len, retscore);
    }

    virtual int zrevrange(MKEY, int start, int end, OITEMSVSN,
            double** scores_items, int* scores_items_len, int withscore)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->zrevrange(IKEY, start, end, BITEMSVSN, scores_items, scores_items_len, withscore);
    }

    virtual int zrange(MKEY, int start, int end, OITEMSVSN,
           double** scores_items, int* scores_items_len, int withscore)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->zrange(IKEY, start, end, BITEMSVSN, scores_items, scores_items_len, withscore);
    }

    virtual int genericZrangebyscore(MKEY, double start, double end, OITEMSVSN,
            double** scores_items, int* scores_items_len, int limit, int withscore, bool reverse)
    {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->genericZrangebyscore(IKEY, start, end, BITEMSVSN,
                scores_items, scores_items_len, limit, withscore, reverse);
    }

    virtual int zadd(MKEY, MDETA, double score, value_item* item)
    {
        REDIS_DB_KEY_ITEM_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->zadd(IKEY, IDETA, score, item);
    }

    virtual int zrank(MKEY, char* value, size_t value_len, long long* rank)
    {
        REDIS_DB_KEY_VALUE_S_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->zrank(IKEY, value, value_len, rank);
    }

    virtual int zrevrank(MKEY, char* value, size_t value_len, long long* rank)
    {
        REDIS_DB_KEY_VALUE_S_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->zrevrank(IKEY, value, value_len, rank);
    }

    virtual int zcount(MKEY, double start, double end, long long *retnum)
	{
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->zcount(IKEY, start, end, retnum);
	}

	virtual int zincrby(MKEY, MDETA, value_item* value, double addscore, double *retvalue)
	{
		REDIS_DB_KEY_VALUE_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
		return session->zincrby(IKEY, IDETA, value, addscore, retvalue);
	}

	virtual int zrem(MKEY, MDETA, value_item* item)
    {
        REDIS_DB_KEY_ITEM_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->zrem(IKEY, IDETA, item);
    }

	virtual int zremrangebyrank(MKEY, MDETA, int start, int end, long long* remnum)
	{
		REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
		return session->zremrangebyrank(IKEY, IDETA, start, end, remnum);
	}

	virtual int zremrangebyscore(MKEY, MDETA, double start, double end, long long* remnum)
	{
		REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
		return session->zremrangebyscore(IKEY, IDETA, start, end, remnum);
	}

    virtual int zcard(MKEY, long long* retnum) {
        REDIS_DB_KEY_VERIFY;
        area = get_redis_db(area, generic_hash(key,key_len));
        return session->zcard(IKEY,retnum);
    }

//other operator
    size_t item_count(uint32_t area)
    {
        size_t count = 0;
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for (uint32_t i = start; i < end; i++) {
            count += context->item_count(i);
        }
        return count;
    }

    size_t autoremove_count(uint32_t area)
    {
        size_t count = 0;
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for (uint32_t i = start; i < end; i++) {
            count += context->autoremove_count(i);
        }
        return count;
    }

    void reset_autoremove_count(uint32_t area)
    {
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for (uint32_t i = start; i < end; i++) {
            context->reset_autoremove_count(i);
        }
    }

    uint64_t get_maxmemory() {
        return context->get_maxmemory();
    }

    virtual void set_maxmemory(uint32_t area, uint64_t quota)
    {
        uint64_t per_quota = quota / context->get_unit_num();
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for (uint32_t i = start; i < end; i++) {
            context->set_db_maxmemory(i, per_quota);
        }
    }

    uint64_t get_db_used_maxmemory(uint32_t area)
    {
        uint64_t max_memory = 0;
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for (uint32_t i = start; i < end; i++) {
            max_memory += redis_db_context::get_db_used_maxmemory(i);
        }

        return max_memory;
    }

    int get_read_count(uint32_t area) {
        size_t count = 0;
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for (uint32_t i = start; i < end; i++) {
            count += context->get_read_count(i);
        }
        return count;
    }

    void reset_read_count(uint32_t area) {
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for (uint32_t i = start; i < end; i++) {
            context->reset_read_count(i);
        }
    }

    int get_write_count(uint32_t area) {
        size_t count = 0;
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for (uint32_t i = start; i < end; i++) {
            count += context->get_write_count(i);
        }
        return count;
    }

    void reset_write_count(uint32_t area) {
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for (uint32_t i = start; i < end; i++) {
            context->reset_write_count(i);
        }
    }

    int get_hit_count(uint32_t area) {
        size_t count = 0;
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for (uint32_t i = start; i < end; i++) {
            count += context->get_hit_count(i);
        }
        return count;
    }

    void reset_hit_count(uint32_t area) {
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for (uint32_t i = start; i < end; i++) {
            context->reset_hit_count(i);
        }
    }

    int get_remove_count(uint32_t area) {
        size_t count = 0;
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for (uint32_t i = start; i < end; i++) {
            count += context->get_remove_count(i);
        }
        return count;
    }

    void reset_remove_count(uint32_t area) {
        uint32_t start = area * context->get_unit_num();
        uint32_t end = start + context->get_unit_num();
        for (uint32_t i = start; i < end; i++) {
            context->reset_remove_count(i);
        }
    }

    uint16_t get_area_num() {
        return context->get_area_num();
    }
private:
    unsigned int generic_hash(char *buffer, int len) {
        const unsigned char* buf = (const unsigned char*)buffer;
        unsigned int hash = 5381;

        while (len--)
            hash = ((hash << 5) + hash) + (*buf++); /* hash * 33 + c */
        return hash;
    }
private:
    redis_db_context *context;
    redis_db_session *session;
};


END_NS


#endif

