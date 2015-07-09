#ifndef REDIS_DB_SESSION_H
#define REDIS_DB_SESSION_H
#include <tbsys.h>
#include <Mutex.h>

#include "storage/storage_manager.hpp"
#include "redis/redislib.h"
#include "log.hpp"
#include "redis_define.h"

BEGIN_NS

class redis_db_context;


#pragma pack(1)
typedef struct value_item
{
    size_t data_len;    //8
    char* data;
};

#define MDETA           uint16_t version, int version_care, long expiretime
#define IDETA           version, version_care, expiretime
#define MKEY            uint32_t area, char* key, size_t key_len
#define IKEY            area, key, key_len
#define MITEMSVNS       value_item* item, int item_num
#define MITEMVSN        value_item* item
#define IITEMSVNS       item, item_num
#define OITEMVSN        value_item** item, uint16_t *ret_version
#define BITEMVSN        item, ret_version
#define OITEMSVSN       value_item** item, int *item_num, uint16_t *ret_version
#define BITEMSVSN       item, item_num, ret_version
#define CFIELD          char* field, size_t field_len
#define MFIELD          value_item* field, int field_len
#define IFIELD          field, field_len
#define LORR            LeftOrRight lr
#define EORN            ExistOrNot  en
#define BORA            BeforeOrAfter ba

class redis_db_session
{
public:
    redis_db_session(const redis_db_context &context);
    virtual ~redis_db_session();
    static void freeValue_Item(value_item* item, int item_num);

    void exec_command(const std::string& cmd, std::string &res);

    //common
    int lazyflushdb(int area);
    int dumpDb(int area);
    int loadDb(int area);
    int expire(MKEY, long expiretime);
    int persist(MKEY);
    int exists(MKEY);
    int ttl(MKEY, long long* time_remain);
    int type(MKEY, long long* what_type);
    int rename(MKEY, char* target_key, size_t target_key_len, int nx);
    int addfilter(int area, char* key_pattern, int kplen, char* field_pattern, int fplen,
            char* val_pattern, int vplen);
    int removefilter(int area, char* key_pattern, int kplen, char* field_pattern, int fplen,
            char* val_pattern, int vplen);
    //string
	int set(MKEY, MDETA, value_item* item, EORN);
    int get(MKEY, OITEMVSN);
    int getset(MKEY, MDETA, value_item* new_item, OITEMVSN);
    int del(MKEY, int version_care);
    int incdecr(MKEY, MDETA, int init_value, int addvalue, int *retvalue);
    //list
    int lrpush(MKEY, MDETA, MITEMSVNS, LORR, EORN, int max_count, push_return_value *prv);
    int lrpop(MKEY, MDETA, int count, LORR, OITEMSVSN);
    int lindex(MKEY, int index, OITEMVSN);
    int lrange(MKEY, int start, int end, OITEMSVSN);
    int lrem(MKEY, MDETA, int count, value_item* value, long long* remlen);
    int llen(MKEY, long long* len);
    int ltrim(MKEY, MDETA, int start, int end);
    int linsert(MKEY, MDETA, BORA, char* value_in_list, size_t value_in_list_len,
            value_item* value_insert);
    int lset(MKEY, MDETA, int index, value_item* value);
    //hset
    int hgetall(MKEY, OITEMSVSN);
    int hkeys(MKEY, OITEMSVSN);
    int hvals(MKEY, OITEMSVSN);
    int hget(MKEY, CFIELD, OITEMVSN);
    int hmget(MKEY, MFIELD, OITEMSVSN);
    int hincrby(MKEY, MDETA, value_item* field, long long addvalue, long long *retvalue);
    int hset(MKEY, MDETA, CFIELD, value_item* item);
    int hsetnx(MKEY, MDETA, CFIELD, value_item* item);
    int hmset(MKEY, MDETA, value_item* field_value, int field_val_len, int *retvalue);
    int hdel(MKEY, MDETA, CFIELD);
    int hlen(MKEY, long long* len);
    int hexists(MKEY, CFIELD);
    //set
    int scard(MKEY, long long* retnum);
    int smembers(MKEY, OITEMSVSN);
    int sadd(MKEY, MDETA, value_item* item);
    int spop(MKEY, MDETA, OITEMVSN);
    int srem(MKEY, MDETA, value_item* item);

    //zset
    int zscore(MKEY, char* value, size_t value_len, double* retscore);
    int zrevrange(MKEY, int start, int end, OITEMSVSN, double** scores_items, int* scores_items_len, int withscore);
    int zrange(MKEY, int start, int end, OITEMSVSN, double** scores_items, int* scores_items_len, int withscore);
    int genericZrangebyscore(MKEY, double start, double end,
        OITEMSVSN, double** scores_items, int* scores_items_len, int limit, int withscore, bool reverse);
    int zadd(MKEY, MDETA, double score, value_item* item);
    int zrank(MKEY, char* value, size_t value_len, long long* rank);
    int zrevrank(MKEY, char* value, size_t value_len, long long* rank);
    int zcount(MKEY, double start, double end, long long *retnum);
	int zincrby(MKEY, MDETA, value_item* value, double addscore, double *retvalue);
	int zrem(MKEY, MDETA, value_item* item);
	int zremrangebyrank(MKEY, MDETA, int start, int end, long long* remnum);
	int zremrangebyscore(MKEY, MDETA, double start, double end, long long* remnum);
    int zcard(MKEY, long long* retnum);
private:
    int filter_field(redisServer* server, int dbnum, char* key, size_t key_len);
    int ready_client(redisClient *client, size_t arglen);
    int get_redis_client(redisClient **client, const uint32_t dbnum, const int argc);
    const redis_db_context &context;
    //const tbutil::Mutex session_mutex;
    friend class redis_db_context;

};

END_NS

#endif


