#include <cstdlib>
#include <cmath>
#include <tbsys.h>
#include "rdb_manager.h"
#include "redis_db.h"

using namespace tair;
using namespace tair::storage::rdb;

#define NOT_USED(V) ((void)V)

#define CHECK_DATA_ENTRY_SIZE(value) do {                   \
    if(value->get_size() > DATA_ENTRY_MAX_SIZE) {           \
        delete []item;                                      \
        item = NULL;                                        \
        return TAIR_RETURN_DATA_LEN_LIMITED;                \
    }                                                       \
} while(0)

#define DATA_ENTRY_MAX_SIZE             1048576

#define GET_VERSION_EXPIRE(version, exp, en) do {                           \
    version = version > 0 ? version : 0;                                    \
    if(expire_time > 0) {                                                   \
        if (en == IS_NOT_EXIST) {                                           \
            en = IS_NOT_EXIST_AND_EXPIRE;                                   \
        } else {                                                            \
            en = IS_EXIST_AND_EXPIRE;                                       \
        }                                                                   \
    	exp = getAbsTime(expire_time);                                      \
	} else if(expire_time < 0) {											\
        exp = expire_time;                                                  \
    }                                                                       \
}while(0)


#define VERIFY_NAMESPACE(_area) if ((_area) >= db->get_area_num() || (_area) < 0) return TAIR_RETURN_NAMESPACE_ERROR;


#include <sstream>
#include <iomanip>

std::string hexStr(char *data, int len)
{
    std::stringstream ss;
    ss<<std::hex;
    ss<<std::setfill('0')<<std::setw(2);
    for(int i(0);i<len;++i)
        ss<<"\\x"<<(unsigned int)data[i];
    return ss.str();
}

inline int redis_tair_code(int ret)
{
    switch(ret) {
        case REDIS_OK:
            return TAIR_RETURN_SUCCESS;
        case REDIS_OK_BUT_CZERO:
            return TAIR_RETURN_COUNT_ZERO;
        case REDIS_OK_NOT_EXIST:
            return TAIR_RETURN_DATA_NOT_EXIST;
        case REDIS_ERR_VERSION_ERROR:
            return TAIR_RETURN_VERSION_ERROR;
        case REDIS_ERR_WRONG_TYPE_ERROR:
        case REDIS_ERR_EXPIRE_TIME_OUT:
            return TAIR_RETURN_TYPE_NOT_MATCH;
        case REDIS_ERR_OUT_OF_RANGE:
            return TAIR_RETURN_OUT_OF_RANGE;
        case REDIS_ERR_IS_NOT_NUMBER:
        	return TAIR_RETURN_IS_NOT_NUMBER;
        case REDIS_ERR_IS_NOT_DOUBLE:
        	return TAIR_RETURN_IS_NOT_DOUBLE;
        case REDIS_ERR_IS_NOT_INTEGER:
        	return TAIR_RETURN_IS_NOT_INTEGER;
        case REDIS_OK_BUT_ALREADY_EXIST:
        	return TAIR_RETURN_ALREADY_EXIST;
        case REDIS_OK_RANGE_HAVE_NONE:
        	return TAIR_RETURN_RANGE_HAVE_NONE;
        case REDIS_ERR_NAMESPACE_ERROR:
            return TAIR_RETURN_NAMESPACE_ERROR;
        case REDIS_ERR_INCDECR_OVERFLOW:
            return TAIR_RETURN_INCDECR_OVERFLOW;
        case REDIS_ERR_DATA_LEN_LIMITED:
            return TAIR_RETURN_DATA_LEN_LIMITED;
    }
    log_warn("redis tair code = %d", ret);
    return TAIR_RETURN_FAILED;
}

rdb_manager::rdb_manager()
{
	redisConfig config;
	get_redis_config(&config);
    redis_instance = new redis_db(config);
}

rdb_manager::~rdb_manager()
{
    if (redis_instance) {
        delete redis_instance;
        redis_instance = NULL;
    }
}

//common operator
int rdb_manager::ttl(int bucket_number, data_entry & key, long long* time_remain)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);
    int ret = db->ttl(key.area, key.get_data(), key.get_size(), time_remain);
    return redis_tair_code(ret);
}

int rdb_manager::type(int bucket_number, data_entry & key, long long* what_type)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);
    int ret = db->type(key.area, key.get_data(), key.get_size(), what_type);
    return redis_tair_code(ret);
}

int rdb_manager::remove(int bucket_number, data_entry & key, bool version_care)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);
    int ret = db->remove(key.area, key.get_data(), key.get_size(), (int)version_care);
    return redis_tair_code(ret);
}

int rdb_manager::expire(int bucket_number, data_entry & key, int expiretime)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);
    int ret = db->expire(key.area, key.get_data(), key.get_size(), expiretime);
    return redis_tair_code(ret);
}

//expiretime is time point for second
int rdb_manager::expireat(int bucket_number, data_entry & key, int expiretime)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);
    int ret = db->expire(key.area, key.get_data(), key.get_size(), expiretime);
    return redis_tair_code(ret);
}

int rdb_manager::persist(int bucket_number, data_entry & key)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);
    int ret = db->persist(key.area, key.get_data(), key.get_size());
    return redis_tair_code(ret);
}

int rdb_manager::exists(int bucket_number, data_entry & key)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);
    int ret = db->exists(key.area, key.get_data(), key.get_size());
    return redis_tair_code(ret);
}

int rdb_manager::lazyclear(int area)
{
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(area);
    int ret = db->lazyflushdb(area);
    return redis_tair_code(ret);
}

int rdb_manager::dumparea(int area)
{
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(area);
    int ret = db->dumparea(area);
    return redis_tair_code(ret);
}

int rdb_manager::loadarea(int area)
{
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(area);
    int ret = db->loadarea(area);
    return redis_tair_code(ret);
}

int rdb_manager::addfilter(int area, data_entry & key, data_entry & field, data_entry & value)
{
    redis_db *db = get_redis_instance();

    VERIFY_NAMESPACE(area);

    value_item ikey;
    ikey.data = key.get_data();
    ikey.data_len = key.get_size();

    value_item ifield;
    ifield.data = field.get_data();
    ifield.data_len = field.get_size();

    value_item ivalue;
    ivalue.data = value.get_data();
    ivalue.data_len = value.get_size();

    int ret = db->addfilter(area, &ikey, &ifield, &ivalue);

    return redis_tair_code(ret);
}

int rdb_manager::removefilter(int area, data_entry & key, data_entry & field, data_entry & value)
{
    redis_db *db = get_redis_instance();

    VERIFY_NAMESPACE(area);

    value_item ikey;
    ikey.data = key.get_data();
    ikey.data_len = key.get_size();

    value_item ifield;
    ifield.data = field.get_data();
    ifield.data_len = field.get_size();

    value_item ivalue;
    ivalue.data = value.get_data();
    ivalue.data_len = value.get_size();

    int ret = db->removefilter(area, &ikey, &ifield, &ivalue);

    return redis_tair_code(ret);
}

//string operator
int rdb_manager::incdecr(int bucket_number, data_entry & key, int init_value, int addvalue, int * retvalue,
                bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->incdecr(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            init_value, addvalue, retvalue);

    return redis_tair_code(ret);
}

int rdb_manager::putnx(int bucket_number, data_entry & key, data_entry & value,
        bool version_care, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item item;

    if(value.get_size() > DATA_ENTRY_MAX_SIZE) {
        return TAIR_RETURN_DATA_LEN_LIMITED;
    }
    item.data       = value.get_data();
    item.data_len   = value.get_size();

    uint16_t version = key.data_meta.version;
    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->put(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            &item, en);

    return redis_tair_code(ret);
}

int rdb_manager::put(int bucket_number, data_entry & key, data_entry & value,
        bool version_care, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);
    
    log_debug("rdb_manager::put key=%s,size=%d,hex=%s,value=%s"
        ,key.get_data()+2,key.get_size(),hexStr(key.get_data(),key.get_size()).c_str(),hexStr(value.get_data(),value.get_size()).c_str() );
    value_item item;

    if(value.get_size() > DATA_ENTRY_MAX_SIZE) {
        return TAIR_RETURN_DATA_LEN_LIMITED;
    }
    item.data       = value.get_data();
    item.data_len   = value.get_size();

    uint16_t version = key.data_meta.version;
    long exp = 0;
    ExistOrNot en = IS_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->put(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            &item, en);

    return redis_tair_code(ret);
}

int rdb_manager::get(int bucket_number, data_entry & key, data_entry & value)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item *item = NULL;
    uint16_t ret_version = 0;

    int ret = db->get(key.area, key.get_data(), key.get_size(),
            &item, &ret_version);

    if (ret == REDIS_OK)
    {
        if (item == NULL)
            return redis_tair_code(ret);

        assert(item->data != NULL);

        key.set_version(ret_version);
        value.set_alloced_data(item->data, item->data_len);
        value.set_version(ret_version);
        value.data_meta.keysize = key.data_meta.keysize;
        value.data_meta.valsize = item->data_len;
    }

    if(item != NULL)
    {
        delete item;
        item = NULL;
    }

    return redis_tair_code(ret);
}

int rdb_manager::getset(int bucket_number, data_entry & key, data_entry & value,
        data_entry & oldvalue, bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item *item = NULL;
    uint16_t ret_version = 0;

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    value_item new_item;
    new_item.data = value.get_data();
    new_item.data_len = value.get_size();
    int ret = db->getset(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            &new_item, &item, &ret_version);

    if (item == NULL)
        return redis_tair_code(ret);

    if (item->data_len > 0) {
        key.set_version(ret_version);
        oldvalue.set_alloced_data(item->data, item->data_len);
        oldvalue.set_version(ret_version);
        oldvalue.data_meta.keysize = key.data_meta.keysize;
        oldvalue.data_meta.valsize = item->data_len;
    }

    delete item;

    return redis_tair_code(ret);
}

//list operator
int rdb_manager::lrpop(int bucket_number, data_entry &key, int count,
        std::vector<data_entry *>& values, LeftOrRight lr,
        bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    values.clear();
    if(count == 0)
        return redis_tair_code(REDIS_OK);

    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    value_item *items   = NULL;
    int items_len = 0;
    uint16_t ret_version = 0;
    int ret = db->lrpop(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            count, lr, &items, &items_len, &ret_version);

    if (items == NULL || ret != REDIS_OK)
    {
        if (items != NULL)
        {
            delete []items;
        }
        return redis_tair_code(ret);
    }

    key.set_version(ret_version);
    for(int i = 0; i < items_len; ++i)
    {
        data_entry *entry = new data_entry();
        entry->set_alloced_data(items[i].data, items[i].data_len);
        entry->set_version(ret_version);
        entry->data_meta.keysize = key.data_meta.keysize;
        entry->data_meta.valsize = items[i].data_len;
        values.push_back(entry);
    }

    if (items != NULL)
    {
        delete []items;
        items = NULL;
    }

    return redis_tair_code(ret);
}

int rdb_manager::lrpush(int bucket_number, data_entry & key, const std::vector<data_entry* >& values,
        uint32_t* oklen, uint32_t* list_len, tair::LeftOrRight lr, tair::ExistOrNot en,
        int max_count, bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item *item = NULL;
    if(values.size() == 0) {
        item = NULL;
    } else {
        item = new value_item[values.size()];
    }
    for(size_t i = 0; i < values.size(); i++)
    {
        CHECK_DATA_ENTRY_SIZE(values[i]);
        item[i].data = values[i]->get_data();
        item[i].data_len = values[i]->get_size();
    }

    long exp = 0;
    ExistOrNot en2 = IS_NOT_EXIST;
    push_return_value prv;
    GET_VERSION_EXPIRE(version, exp, en2);

    int ret = db->lrpush(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            item, values.size(), lr, en, max_count, &prv);

    *oklen = prv.pushed_num;
    *list_len = prv.list_len;

    if(item != NULL)
    {
        delete []item;
        item = NULL;
    }

    return redis_tair_code(ret);
}

int rdb_manager::lindex(int bucket_number, data_entry &key, int32_t index, data_entry& value) {
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item *item = NULL;
    uint16_t ret_version = 0;
    int ret = db->lindex(key.area, key.get_data(), key.get_size(),
            index, &item, &ret_version);
    if (ret == REDIS_OK)
    {
        if (item == NULL) {
            if (item != NULL)
            {
                delete item;
                item = NULL;
            }
            return TAIR_RETURN_DATA_NOT_EXIST;
        }

        key.set_version(ret_version);
        value.set_alloced_data(item->data, item->data_len);
        value.set_version(ret_version);
        value.data_meta.keysize = key.data_meta.keysize;
        value.data_meta.valsize = item->data_len;
    }

    if(item != NULL)
    {
        delete item;
        item = NULL;
    }

    return redis_tair_code(ret);
}

int rdb_manager::lrange(int bucket_number, data_entry &key, int start, int end,
              std::vector<data_entry *>& values)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item *items = NULL;
    int items_len = 0;
    uint16_t ret_version = 0;

    int ret = db->lrange(key.area, key.get_data(), key.get_size(),
            start, end, &items, &items_len, &ret_version);

    if (items == NULL || ret != REDIS_OK)
    {
        if (items != NULL)
        {
            delete []items;
        }
        return redis_tair_code(ret);
    }

    key.set_version(ret_version);
    for(int i = 0; i < items_len; ++i)
    {
        data_entry *entry = new data_entry();
        entry->set_alloced_data(items[i].data, items[i].data_len);
        entry->set_version(ret_version);
        entry->data_meta.keysize = key.data_meta.keysize;
        entry->data_meta.valsize = items[i].data_len;
        values.push_back(entry);
    }

    if (items != NULL)
    {
        delete []items;
        items = NULL;
    }

    return redis_tair_code(ret);
}

int rdb_manager::lrem(int bucket_number, data_entry &key, int count,
        data_entry& value, long long *retnum,
        bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item item;
    item.data       = value.get_data();
    item.data_len   = value.get_size();

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->lrem(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            count, &item, retnum);
    return redis_tair_code(ret);
}

int rdb_manager::llen(int bucket_number, data_entry &key, long long *retlen)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);
    int ret = db->llen(key.area, key.get_data(), key.get_size(), retlen);
    return redis_tair_code(ret);
}

int rdb_manager::linsert(int bucket_number, data_entry &key, data_entry &value_in_list,
        data_entry &value_insert, tair::BeforeOrAfter ba,
        bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item item;
    item.data       = value_insert.get_data();
    item.data_len   = value_insert.get_size();

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->linsert(key.area, key.get_data(), key.get_size(),
            version, version_care, exp, ba,
            value_in_list.get_data(), value_in_list.get_size(), &item);

    return redis_tair_code(ret);
}

int rdb_manager::lset(int bucket_number, data_entry &key, int index,
        data_entry &value, bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item item;
    item.data       = value.get_data();
    item.data_len   = value.get_size();

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->lset(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            index, &item);

    return redis_tair_code(ret);
}

int rdb_manager::ltrim(int bucket_number, data_entry &key, int start, int end,
        bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->ltrim(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            start, end);

    return redis_tair_code(ret);
}

bool rdb_manager::init_buckets(const std::vector <int>&buckets)
{
    return true;
}

void rdb_manager::close_buckets(const std::vector <int>&buckets)
{
    return ;
}

void rdb_manager::set_area_quota(int area, uint64_t quota)
{
    redis_instance->set_maxmemory(area, quota);
}

void rdb_manager::set_area_quota(std::map<int, uint64_t> &quota_map)
{
    std::map<int, uint64_t>::iterator iter = quota_map.begin();
    for (; iter != quota_map.end(); ++iter)
    {
        set_area_quota(iter->first, iter->second);
    }
}

void rdb_manager::get_stats(tair_stat *stat)
{
    int evict_count, put_count, get_count, hit_count, remove_count;
    int max_area = redis_instance->get_area_num();
    for(int i = 0; i < max_area; ++i) {
      stat[i].data_size_value = redis_instance->get_db_used_maxmemory(i);
      stat[i].item_count_value = redis_instance->item_count(i);
      stat[i].use_size_value = stat[i].data_size_value;

      evict_count = redis_instance->autoremove_count(i);
      redis_instance->reset_autoremove_count(i);

      put_count = redis_instance->get_write_count(i);
      redis_instance->reset_write_count(i);

      get_count = redis_instance->get_read_count(i);
      redis_instance->reset_read_count(i);

      hit_count = redis_instance->get_hit_count(i);
      redis_instance->reset_hit_count(i);

      remove_count = redis_instance->get_remove_count(i);
      redis_instance->reset_remove_count(i);

      stat[i].set_evict_count(evict_count);
      stat[i].set_put_count(put_count);
      stat[i].set_get_count(get_count);
      stat[i].set_remove_count(remove_count);
      stat[i].set_hit_count(hit_count);
      /*
      log_debug(
          "area:%d dsize=usize:%ld count:%ld ecount:%ld put_count %d get_count %d remove_count %d hit_count %d",
          i,
          stat[i].data_size_value,
          stat[i].item_count_value,
          stat[i].evict_count(),
          stat[i].put_count(),
          stat[i].get_count(),
          stat[i].remove_count(),
          stat[i].hit_count());*/
    }
}

//hset
int rdb_manager::genericHgetall(int bucket_number, data_entry & key,
        std::vector<data_entry*> & values, int method) {
    NOT_USED(bucket_number);
    values.clear();

    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item *items = NULL;
    int items_len = 0;
    uint16_t ret_version = 0;
    int ret;

    if (method == METHOD_HGETALL) {
        ret = db->hgetall(key.area, key.get_data(), key.get_size(),
                &items, &items_len, &ret_version);
    } else if (method == METHOD_HVALS) {
        ret = db->hvals(key.area, key.get_data(), key.get_size(),
                &items, &items_len, &ret_version);
    } else if (method == METHOD_HKEYS) {
        ret = db->hkeys(key.area, key.get_data(), key.get_size(),
                &items, &items_len, &ret_version);
    }

    if (items == NULL || ret != REDIS_OK)
    {
        if (items != NULL)
        {
            delete []items;
        }
        return redis_tair_code(ret);
    }

    key.set_version(ret_version);
    for(int i = 0; i < items_len; ++i)
    {
        data_entry *entry = new data_entry();
        entry->set_alloced_data(items[i].data, items[i].data_len);
        entry->data_meta.keysize = key.data_meta.keysize;
        entry->data_meta.valsize = items[i].data_len;
        values.push_back(entry);
    }

    if(items != NULL)
    {
        delete []items;
        items = NULL;
    }

    return redis_tair_code(ret);
}

int rdb_manager::hgetall(int bucket_number, data_entry & key, std::vector<data_entry*> & values) {
    return genericHgetall(bucket_number, key, values, METHOD_HGETALL);
}

int rdb_manager::hkeys(int bucket_number, data_entry & key, std::vector<data_entry*> & values) {
    return genericHgetall(bucket_number, key, values, METHOD_HKEYS);
}

int rdb_manager::hvals(int bucket_number, data_entry & key, std::vector<data_entry*> & values) {
    return genericHgetall(bucket_number, key, values, METHOD_HVALS);
}

int rdb_manager::hincrby(int bucket_number, data_entry & key, data_entry & field,
        long long & addvalue, long long *retvalue,
        bool version_care, uint16_t version, int expire_time) {
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item item;
    item.data       = field.get_data();
    item.data_len   = field.get_size();

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->hincrby(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            &item, addvalue, retvalue);

    return redis_tair_code(ret);
}

int rdb_manager::hmset(int bucket_number, data_entry & key, std::vector<data_entry*> & fields_value,
        int* retvalue, bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item *item = NULL;
    if(fields_value.size() == 0) {
        item = NULL;
    } else {
        item = new value_item[fields_value.size()];
    }
    for(size_t i = 0; i < fields_value.size(); i++) {
        item[i].data = fields_value[i]->get_data();
        item[i].data_len = fields_value[i]->get_size();
    }

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->hmset(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            item, fields_value.size(), retvalue);

    if (item != NULL) {
        delete []item;
        item = NULL;
    }

    return redis_tair_code(ret);
}

int rdb_manager::hset(int bucket_number, data_entry & key, data_entry & field,
        data_entry & value, bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item item;
    item.data = value.get_data();
    item.data_len = value.get_size();

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->hset(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            field.get_data(), field.get_size(), &item);

    return redis_tair_code(ret);
}

int rdb_manager::hsetnx(int bucket_number, data_entry & key, data_entry & field,
        data_entry & value, bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item item;
    item.data = value.get_data();
    item.data_len = value.get_size();

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->hsetnx(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            field.get_data(), field.get_size(), &item);

    return redis_tair_code(ret);
}

int rdb_manager::hexists(int bucket_number, data_entry & key, data_entry & field)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    int ret = db->hexists(key.area, key.get_data(), key.get_size(),
            field.get_data(), field.get_size());
    return redis_tair_code(ret);
}

int rdb_manager::hget(int bucket_number, data_entry & key, data_entry & field,
        data_entry & value)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item *item = NULL;
    uint16_t ret_version = 0;
    int ret = db->hget(key.area, key.get_data(), key.get_size(),
            field.get_data(), field.get_size(), &item, &ret_version);
    if (ret == REDIS_OK)
    {
        if (item == NULL)
        {
            if(item != NULL)
            {
                delete item;
                item = NULL;
            }
            return redis_tair_code(ret);
        }

        key.set_version(ret_version);
        value.set_alloced_data(item->data, item->data_len);
        value.set_version(ret_version);
        value.data_meta.keysize = key.data_meta.keysize;
        value.data_meta.valsize = item->data_len;
    }

    if(item != NULL)
    {
        delete item;
        item = NULL;
    }

    return redis_tair_code(ret);
}

int rdb_manager::hmget(int bucket_number, data_entry & key, std::vector<data_entry*> & fields,
        std::vector<data_entry*> & values)
{
    NOT_USED(bucket_number);
    values.clear();

    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item *vals_item   = NULL;
    value_item *fields_item  = NULL;
    int vals_len = 0;
    int fields_len = 0;

    fields_len = fields.size();
    if(fields_len == 0) {
        fields_item = NULL;
    } else {
        fields_item = new value_item[fields_len];
    }

    for(int i = 0; i < fields_len; i++) {
        fields_item[i].data = fields[i]->get_data();
        fields_item[i].data_len = fields[i]->get_size();
    }

    uint16_t ret_version = 0;

    int ret = db->hmget(key.area, key.get_data(), key.get_size(),
            fields_item, fields_len, &vals_item, &vals_len, &ret_version);

    if (vals_item == NULL || ret != REDIS_OK)
    {
        if (vals_item != NULL)
        {
            delete []vals_item;
        }
        if (fields_item != NULL)
        {
            delete []fields_item;
        }
        return redis_tair_code(ret);
    }

    key.set_version(ret_version);
    for(int i = 0; i < vals_len; ++i)
    {
        data_entry *entry = new data_entry();
        entry->set_alloced_data(vals_item[i].data, vals_item[i].data_len);
        entry->set_version(ret_version);
        entry->data_meta.keysize = key.data_meta.keysize;
        entry->data_meta.valsize = vals_item[i].data_len;
        values.push_back(entry);
    }

    if (vals_item != NULL)
    {
        delete []vals_item;
        vals_item = NULL;
    }
    if (fields_item != NULL)
    {
        delete []fields_item;
        fields_item = NULL;
    }
    return redis_tair_code(ret);
}

//should need version
int rdb_manager::hdel(int bucket_number, data_entry & key, data_entry & field,
        bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->hdel(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            field.get_data(), field.get_size());

    return redis_tair_code(ret);

}

int rdb_manager::hlen(int bucket_number, data_entry &key, long long* retlen)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);
    int ret = db->hlen(key.area, key.get_data(), key.get_size(), retlen);
    return redis_tair_code(ret);
}

//set
int rdb_manager::scard(int bucket_number, data_entry & key, long long* retnum)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);
    int ret = db->scard(key.area, key.get_data(), key.get_size(), retnum);
    return redis_tair_code(ret);
}

int rdb_manager::smembers(int bucket_number, data_entry & key, std::vector<data_entry*> & values)
{
    NOT_USED(bucket_number);
    values.clear();

    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item *items   = NULL;
    int items_len = 0;
    uint16_t ret_version = 0;

    int ret = db->smembers(key.area, key.get_data(), key.get_size(),
            &items, &items_len, &ret_version);

    if (items == NULL || ret != REDIS_OK)
    {
        if (items != NULL)
        {
            delete []items;
        }
        return redis_tair_code(ret);
    }

    key.set_version(ret_version);
    for(int i = 0; i < items_len; ++i)
    {
        data_entry *entry = new data_entry();
        entry->set_alloced_data(items[i].data, items[i].data_len);
        entry->set_version(ret_version);
        entry->data_meta.keysize = key.data_meta.keysize;
        entry->data_meta.valsize = items[i].data_len;
        values.push_back(entry);
    }

    if (items != NULL)
    {
        delete []items;
    }
    return redis_tair_code(ret);
}

int rdb_manager::sadd(int bucket_number, data_entry & key, data_entry & value,
        bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item item;
    item.data       = value.get_data();
    item.data_len   = value.get_size();

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->sadd(key.area, key.get_data(), key.get_size(),
            version, version_care, exp, &item);

    return redis_tair_code(ret);
}

int rdb_manager::srem(int bucket_number, data_entry & key, data_entry & value,
        bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item item;
    item.data       = value.get_data();
    item.data_len   = value.get_size();

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->srem(key.area, key.get_data(), key.get_size(),
            version, version_care, exp, &item);

    return redis_tair_code(ret);
}

int rdb_manager::spop(int bucket_number, data_entry & key, data_entry & value,
        bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item *item = NULL;
    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    uint16_t ret_version = 0;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->spop(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            &item, &ret_version);

    if (ret == REDIS_OK)
    {
        if (item == NULL)
        {
            if (item != NULL)
            {
                delete item;
            }
            return redis_tair_code(ret);
        }

        key.set_version(ret_version);
        value.set_alloced_data(item->data, item->data_len);
        value.set_version(ret_version);
        value.data_meta.keysize = key.data_meta.keysize;
        value.data_meta.valsize = item->data_len;
    }

    if(item != NULL)
    {
        delete item;
        item = NULL;
    }

    return redis_tair_code(ret);

}

//zset
int rdb_manager::zscore(int bucket_number, data_entry & key,
        data_entry & value, double* retstore)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    int ret = db->zscore(key.area, key.get_data(), key.get_size(), value.get_data(),
            value.get_size(), retstore);

    return redis_tair_code(ret);
}

int rdb_manager::zrange(int bucket_number, data_entry & key, int start, int end,
            std::vector<data_entry*> & values, std::vector<double> & scores, int withscore)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item *items = NULL;
    int items_len = 0;
    uint16_t ret_version = 0;
    double *scores_items = NULL;
    int scores_items_len = 0;

    int ret = db->zrange(key.area, key.get_data(), key.get_size(),
            start, end, &items, &items_len, &ret_version,
            &scores_items, &scores_items_len, withscore);

    if (items == NULL || ret != REDIS_OK)
    {
        if (items != NULL)
        {
            delete []items;
        }
        if (scores_items != NULL)
        {
            delete []scores_items;
        }
        return redis_tair_code(ret);
    }

    key.set_version(ret_version);
    for(int i = 0; i < items_len; ++i)
    {
        data_entry *entry = new data_entry();
        entry->set_alloced_data(items[i].data, items[i].data_len);
        entry->set_version(ret_version);
        entry->data_meta.keysize = key.data_meta.keysize;
        entry->data_meta.valsize = items[i].data_len;
        values.push_back(entry);
    }

    for(int i = 0; i < scores_items_len; i++)
    {
        scores.push_back(scores_items[i]);
    }

    if (items != NULL)
    {
        delete []items;
        items = NULL;
    }

    if (scores_items != NULL)
    {
        delete []scores_items;
        scores_items = NULL;
    }

    return redis_tair_code(ret);
}

int rdb_manager::zrevrange(int bucket_number, data_entry & key, int start, int end,
            std::vector<data_entry*> & values, std::vector<double> & scores, int withscore)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item *items = NULL;
    int items_len = 0;
    uint16_t ret_version = 0;
    double *scores_items = NULL;
    int scores_items_len = 0;

    int ret = db->zrevrange(key.area, key.get_data(), key.get_size(),
            start, end, &items, &items_len, &ret_version, &scores_items,
            &scores_items_len, withscore);

    if (items == NULL || ret != REDIS_OK)
    {
        if (items != NULL)
        {
            delete []items;
        }
        if (scores_items != NULL)
        {
            delete []scores_items;
        }
        return redis_tair_code(ret);
    }

    key.set_version(ret_version);
    for(int i = 0; i < items_len; ++i)
    {
        data_entry *entry = new data_entry();
        entry->set_alloced_data(items[i].data, items[i].data_len);
        entry->set_version(ret_version);
        entry->data_meta.keysize = key.data_meta.keysize;
        entry->data_meta.valsize = items[i].data_len;
        values.push_back(entry);
    }

    for(int i = 0; i < scores_items_len; i++)
    {
        scores.push_back(scores_items[i]);
    }

    if (items != NULL)
    {
        delete []items;
        items = NULL;
    }

    if (scores_items != NULL)
    {
        delete []scores_items;
        scores_items = NULL;
    }

    return redis_tair_code(ret);
}

int rdb_manager::genericZrangebyscore(int bucket_number, data_entry & key, double start, double end,
        std::vector<data_entry*> & values, std::vector<double> & scores, int limit, int withscore, bool reverse)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    log_debug("rdb_manager::genericZrangebyscore key=%s,size=%d,hex=%s",key.get_data()+2,key.get_size(),hexStr(key.get_data(),key.get_size()).c_str());
    value_item *items = NULL;
    int items_len = 0;
    double *scores_items = NULL;
    int scores_items_len = 0;
    uint16_t ret_version = 0;

    int ret = db->genericZrangebyscore(key.area, key.get_data(), key.get_size(),
            start, end, &items, &items_len, &ret_version, &scores_items, &scores_items_len,
            limit, withscore, reverse);

    if (items == NULL || ret != REDIS_OK)
    {
        if (items != NULL)
        {
            delete []items;
        }
        return redis_tair_code(ret);
    }

    key.set_version(ret_version);
    for(int i = 0; i < items_len; ++i)
    {
        data_entry *entry = new data_entry();
        entry->set_alloced_data(items[i].data, items[i].data_len);
        entry->set_version(ret_version);
        entry->data_meta.keysize = key.data_meta.keysize;
        entry->data_meta.valsize = items[i].data_len;
        values.push_back(entry);
    }

    for(int i = 0; i < scores_items_len; i++)
    {
        scores.push_back(scores_items[i]);
    }

    if (items != NULL)
    {
        delete []items;
        items = NULL;
    }

    if (scores_items != NULL)
    {
        delete []scores_items;
        scores_items = NULL;
    }

    return redis_tair_code(ret);
}

int rdb_manager::zrangebyscore(int bucket_number, data_entry & key, double start, double end,
        std::vector<data_entry*> & values, std::vector<double> & scores, int limit, int withscore)
{
    return genericZrangebyscore(bucket_number, key, start, end, values, scores,
            limit, withscore, false);
}

int rdb_manager::zrevrangebyscore(int bucket_number, data_entry & key, double start, double end,
        std::vector<data_entry*> & values, std::vector<double> & scores, int limit, int withscore)
{
    return genericZrangebyscore(bucket_number, key, start, end, values, scores,
            limit, withscore, true);
}

int rdb_manager::zadd(int bucket_number, data_entry & key, double score, data_entry & value,
            bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);
    
    log_debug("rdb_manager::zadd key=%s,size=%d,hex=%s,value=%s",key.get_data()+2,key.get_size(),
      hexStr(key.get_data(),key.get_size()).c_str(),hexStr(value.get_data(),value.get_size()).c_str());

    value_item item;
    item.data       = value.get_data();
    item.data_len   = value.get_size();

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->zadd(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            score, &item);

    return redis_tair_code(ret);
}

int rdb_manager::zrank(int bucket_number, data_entry & key, data_entry & value, long long* rank)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    int ret = db->zrank(key.area, key.get_data(), key.get_size(),
            value.get_data(), value.get_size(), rank);

    return redis_tair_code(ret);
}

int rdb_manager::zrevrank(int bucket_number, data_entry & key, data_entry & value, long long* rank)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    int ret = db->zrevrank(key.area, key.get_data(), key.get_size(),
            value.get_data(), value.get_size(), rank);

    return redis_tair_code(ret);
}

int rdb_manager::zcount(int bucket_number, data_entry & key, double start, double end, long long* retnum)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);
    int ret = db->zcount(key.area, key.get_data(), key.get_size(), start, end, retnum);
    return redis_tair_code(ret);
}

int rdb_manager::zincrby(int bucket_number, data_entry & key, data_entry & value,
	   	double& addscore, double *retvalue, bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item item;
    item.data       = value.get_data();
    item.data_len   = value.get_size();

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->zincrby(key.area, key.get_data(), key.get_size(),
            version, version_care, exp,
            &item, addscore, retvalue);

    return redis_tair_code(ret);
}

int rdb_manager::zcard(int bucket_number, data_entry & key, long long* retnum)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);
    int ret = db->zcard(key.area, key.get_data(), key.get_size(), retnum);
    return redis_tair_code(ret);
}

int rdb_manager::zrem(int bucket_number, data_entry & key, data_entry & value,
		bool version_care, uint16_t version, int expire_time)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    value_item item;
    item.data       = value.get_data();
    item.data_len   = value.get_size();

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->zrem(key.area, key.get_data(), key.get_size(),
            version, version_care, exp, &item);
    return redis_tair_code(ret);
}

int rdb_manager::zremrangebyrank(int bucket_number, data_entry & key, int start,
	   	int end, bool version_care, uint16_t version, int expire_time, long long* remnum)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->zremrangebyrank(key.area, key.get_data(), key.get_size(),
            version, version_care, exp, start, end, remnum);
    return redis_tair_code(ret);
}

int rdb_manager::zremrangebyscore(int bucket_number, data_entry & key, double start,
	   	double end, bool version_care, uint16_t version, int expire_time, long long* remnum)
{
    NOT_USED(bucket_number);
    redis_db *db = get_redis_instance();
    VERIFY_NAMESPACE(key.area);

    long exp = 0;
    ExistOrNot en = IS_NOT_EXIST;
    GET_VERSION_EXPIRE(version, exp, en);

    int ret = db->zremrangebyscore(key.area, key.get_data(), key.get_size(),
            version, version_care, exp, start, end, remnum);
    return redis_tair_code(ret);
}

void rdb_manager::get_redis_config(redisConfig* config)
{
  config->verbosity = 4 - TBSYS_LOGGER._level;
  config->unit_num = TBSYS_CONFIG.getInt(TAIRRDB_SECTION, RDB_UNIT_NUM, RDB_DEFAULT_UNIT_NUM);
  config->area_group_num = TBSYS_CONFIG.getInt(TAIRRDB_SECTION, RDB_AREA_GROUP_NUM, RDB_DEFAULT_AREA_GROUP_NUM);
  config->maxmemory = TBSYS_CONFIG.getString(TAIRRDB_SECTION, RDB_MAXMEMORY, RDB_DEFAULT_MAXMEMORY);
  config->db_maxmemory = TBSYS_CONFIG.getString(TAIRRDB_SECTION, RDB_AREA_MAXMEMORY, RDB_DEFAULT_AREA_MAXMEMORY);
  config->maxmemory_policy = TBSYS_CONFIG.getInt(TAIRRDB_SECTION, RDB_MAXMEMORY_POLICY,
                                                 REDIS_MAXMEMORY_ALLKEYS_LRU);
  config->maxmemory_samples = TBSYS_CONFIG.getInt(TAIRRDB_SECTION, RDB_MAXMEMORY_SAMPLES, 3);

  config->list_max_size = TBSYS_CONFIG.getInt(TAIRRDB_SECTION, RDB_LIST_MAX_SIZE, 8192);
  config->hash_max_size = TBSYS_CONFIG.getInt(TAIRRDB_SECTION, RDB_HASH_MAX_SIZE, 8192);
  config->set_max_size = TBSYS_CONFIG.getInt(TAIRRDB_SECTION, RDB_ZSET_MAX_SIZE, 8192);
  config->zset_max_size = TBSYS_CONFIG.getInt(TAIRRDB_SECTION, RDB_SET_MAX_SIZE, 8192);
}
