/*
 *Notes: for making redis_db_session more simple and clear,
 *redis-lib must ensure
 *  if ret == REDIS_OK
        may
 *          client->retrun_value != NULL,
 *          redis_db_session should free it's memory
        or
            client->return_value == NUL
                    or
                        client->return_value == NULLL
 *  else
 *      client->return_value == NULL,
 *      redis_db_session shounldn't free it's memory,
 *      so redis-lib should free it's memory
 */
#include <string>
#include "redis_db_session.h"
#include "redis_db_context.h"
#include "redis_define.h"
#include "scope_lock.h"
#include "redis/command.h"
#include <tbsys.h>

USE_NS

#define BUFFER_SIZE 32
#define DB_CONTEXT_LOCK(index) scope_lock lock(&(context.server->db_mutexs[(index)]))

redis_db_session::redis_db_session(const redis_db_context &context)
    : context(context)
{
}

redis_db_session::~redis_db_session()
{
}

int redis_db_session::get_redis_client(redisClient **client, const uint32_t dbnum, const int argc)
{
    *client = selectClient(context.server, dbnum);
    if (client == NULL) {
        return REDIS_ERR_NAMESPACE_ERROR;
    }

    ready_client(*client, argc);

    set_malloc_dbnum(dbnum);
    (*client)->old_dbnum = dbnum;
    return REDIS_OK;
}

//common
int redis_db_session::lazyflushdb(int area)
{
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();

    redisDb *db = context.server->db + area;
    db->logiclock++;
    db->need_remove_key = dbSize(db);
    return REDIS_OK;
}

int redis_db_session::dumpDb(int area)
{
    char filename[256];
    snprintf(filename, 256, "rdb-%d", area);
    dumpThreadInfo* info = create_dump_thread_info(context.server, filename, strlen(filename), area);
    if (info == NULL) {
        return REDIS_ERR_MEMORY_ALLOCATE_ERROR;
    }

    pthread_t tid;
    int flag = pthread_create(&tid, NULL, dump_db_thread, (void*)info);
    if (flag != 0) {
        return REDIS_ERR;
    }
    return REDIS_OK;
}

int redis_db_session::loadDb(int area)
{
    char filename[256];
    snprintf(filename, 256, "rdb-%d", area);
    loadThreadInfo* info = create_load_thread_info(context.server, filename, strlen(filename), area);
    if (info == NULL) {
        return REDIS_ERR_MEMORY_ALLOCATE_ERROR;
    }

    pthread_t tid;
    int flag = pthread_create(&tid, NULL, load_db_thread, (void*)info);
    if (flag != 0) {
        return REDIS_ERR;
    }
    return REDIS_OK;
}

int redis_db_session::addfilter(int area, char* key_pattern,
        int kplen, char* field_pattern, int fplen, char* val_pattern, int vplen)
{
    /* notes: now not process key pattern, and only support simple pattern,
     * greater version at future*/
    UNUSED(key_pattern);
    UNUSED(kplen);

    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();

    redisDb *db = context.server->db + area;

    int8_t type;
    int len;
    char* pattern;
    if (fplen) {
        type = FILTER_TYPE_FIELD;
        len  = fplen;
        pattern = field_pattern;
    } else {
        type = FILTER_TYPE_VALUE;
        len  = vplen;
        pattern = val_pattern;
    }

    int ret = add_filter_node(&(db->filter_list), type, len, pattern);
    if (ret) {
        return REDIS_OK;
    }
    return REDIS_ERR_MEMORY_ALLOCATE_ERROR;
}

int redis_db_session::removefilter(int area, char* key_pattern,
        int kplen, char* field_pattern, int fplen, char* val_pattern, int vplen)
{
    /* notes: now not process key pattern, and only support simple pattern,
     * greater version at future*/
    UNUSED(key_pattern);
    UNUSED(kplen);

    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();

    redisDb *db = context.server->db + area;

    int8_t type;
    int len;
    char* pattern;
    if (fplen) {
        type = FILTER_TYPE_FIELD;
        len  = fplen;
        pattern = field_pattern;
    } else {
        type = FILTER_TYPE_VALUE;
        len  = vplen;
        pattern = val_pattern;
    }

    int ret = remove_filter_node(&(db->filter_list), type, len, pattern);
    if (ret) {
        return REDIS_OK;
    }
    return REDIS_ERR_MEMORY_ALLOCATE_ERROR;
}

int redis_db_session::expire(MKEY, long expiretime) {
    int ret;
    redisClient* client;
    char str_expiretime[BUFFER_SIZE];
    snprintf(str_expiretime,BUFFER_SIZE-1,"%ld",expiretime);
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(EXPIRE_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->cmd = getCommand(EXPIRE_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    client->argv[client->argc++] = createStringObject(str_expiretime,
            strlen(str_expiretime),0,0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    return client->returncode;
}

int redis_db_session::persist(MKEY) {
    int ret;
    redisClient* client;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(PERSIST_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->cmd = getCommand(PERSIST_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    return client->returncode;
}

int redis_db_session::exists(MKEY) {
    int ret;
    redisClient *client;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(EXISTS_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(EXISTS_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }
    return client->returncode;
}

int redis_db_session::rename(MKEY, char* target_key, size_t target_key_len, int nx)
{
    int ret;
    redisClient *client;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(RENAME_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    if (nx) {
        client->cmd = getCommand(RENAMENX_COMMAND);
    } else {
        client->cmd = getCommand(RENAME_COMMAND);
    }
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    client->argv[client->argc++] = createStringObject(target_key, target_key_len, client->db->logiclock, 0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }

    return client->returncode;
}

int redis_db_session::del(MKEY, int version_care)
{
    int ret;
    redisClient *client;
    PROFILER_BEGIN("db context lock");
	DB_CONTEXT_LOCK(area);
    PROFILER_END();

    ret = get_redis_client(&client,area,getCommand(DEL_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->remove_count++;

    client->version_care = version_care;
    client->cmd = getCommand(DEL_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }

    return client->returncode;
}

int redis_db_session::ttl(MKEY, long long* time_remain)
{
    int ret;
    redisClient *client;
    *time_remain = -1;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(TTL_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(TTL_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }

    *time_remain = client->retvalue.llnum;

    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }
    return client->returncode;
}

int redis_db_session::type(MKEY, long long* what_type)
{
    int ret;
    redisClient *client;
    *what_type = REDIS_UNKNOWN;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(TYPE_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;
    client->db->read_count++;

    client->cmd = getCommand(TYPE_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }

    *what_type = client->retvalue.llnum;

    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }
    return client->returncode;
}

//string
int redis_db_session::set(MKEY, MDETA, value_item* item, EORN)
{
    int ret;
    redisClient *client;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(SETNX_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    if(en == IS_NOT_EXIST) {
        client->cmd = getCommand(SETNX_COMMAND);
        client->argv[client->argc++] = NULL;
        client->argv[client->argc++] = createStringObject(key, key_len,
                client->db->logiclock, version);
        client->argv[client->argc++] = createStringObject(item->data,
                item->data_len, 0, 0);
    } else if(en == IS_EXIST) {
        client->cmd = getCommand(SET_COMMAND);
        client->argv[client->argc++] = NULL;
        client->argv[client->argc++] = createStringObject(key, key_len,
                client->db->logiclock, version);
        client->argv[client->argc++] = createStringObject(item->data,
                item->data_len, 0, 0);
    } else if(en == IS_EXIST_AND_EXPIRE) {
        char str_expire[BUFFER_SIZE];
        snprintf(str_expire,BUFFER_SIZE-1,"%ld",expiretime);
        client->cmd = getCommand(SETEX_COMMAND);
        client->argv[client->argc++] = NULL;
        client->argv[client->argc++] = createStringObject(key, key_len,
                client->db->logiclock, version);
        client->argv[client->argc++] = createStringObject(str_expire,
                strlen(str_expire), 0, 0);
        client->argv[client->argc++] = createStringObject(item->data,
                item->data_len, 0, 0);
    } else if(en == IS_NOT_EXIST_AND_EXPIRE) {
        char str_expire[BUFFER_SIZE];
        snprintf(str_expire,BUFFER_SIZE-1,"%ld",expiretime);
        client->cmd = getCommand(SETNXEX_COMMAND);
        client->argv[client->argc++] = NULL;
        client->argv[client->argc++] = createStringObject(key, key_len,
                client->db->logiclock, version);
        client->argv[client->argc++] = createStringObject(str_expire,
                strlen(str_expire), 0, 0);
        client->argv[client->argc++] = createStringObject(item->data,
                item->data_len, 0, 0);
    }

    ret = processCommand(client);
    if(ret == REDIS_OK) {
        ret = client->returncode;
    }
    return ret;
}


int redis_db_session::incdecr(MKEY, MDETA, int init_value, int addvalue, int *retvalue) {
    /*we only use incrby do (incr,decr,incrby,decrby)*/
    int ret;
    redisClient *client;
    *retvalue = 0;
    char str_init_value[BUFFER_SIZE];
    snprintf(str_init_value, BUFFER_SIZE-1,"%d",init_value);
    char str_addvalue[BUFFER_SIZE];
    snprintf(str_addvalue,BUFFER_SIZE-1,"%d",addvalue);

    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(INCRBY_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(INCRBY_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, version);
    client->argv[client->argc++] = createStringObject(str_init_value,
            strlen(str_init_value), 0, 0);
    client->argv[client->argc++] = createStringObject(str_addvalue,
            strlen(str_addvalue), 0, 0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }

    *retvalue = (int)(client->retvalue.llnum);
    return client->returncode;
}

int redis_db_session::get(MKEY, OITEMVSN)
{
    int ret;
    redisClient *client;
    void* client_return_value = NULL;
    *item = NULL;

    //lock in this scope
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(GET_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(GET_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);

    ret = processCommand(client);
    if(ret != REDIS_OK) {
        return ret;
    }

    ret = client->returncode;
    *ret_version = client->version;
    client_return_value = client->return_value;
    client->return_value = NULL;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }

    if(ret != REDIS_OK || client_return_value == NULL) {
        return ret;
    }

    value_item_list* vlist = (value_item_list*)(client_return_value);
    if(vlist->len == 0) {
        freeValueItemList(vlist);
        return ret;
    }

    *item = new value_item;
    value_item_iterator *it = createValueItemIterator(vlist);
    value_item_node *node = nextValueItemNode(&it);

    if(node->type == NODE_TYPE_ROBJ) {
        robj* obj = (robj*)(node->obj.obj);
        (*item)->data_len = sdslen((char*)(obj->ptr));
        (*item)->data = new char[(*item)->data_len];
        memcpy((*item)->data, obj->ptr, sdslen((char*)obj->ptr));
    } else if(node->type == NODE_TYPE_LONGLONG) {
        (*item)->data = new char[BUFFER_SIZE];
        snprintf((*item)->data,BUFFER_SIZE-1,"%lld",node->obj.llnum);
        (*item)->data_len = strlen((*item)->data);
    }

    freeValueItemIterator(&it);
    freeValueItemList(vlist);

    return ret;
}

int redis_db_session::getset(MKEY, MDETA, value_item* new_item, OITEMVSN) {
    int ret;
    redisClient *client;
    *item = NULL;

    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(GETSET_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(GETSET_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, version);
    client->argv[client->argc++] = createStringObject(new_item->data, new_item->data_len, 0, 0);
    ret = processCommand(client);
    if(ret != REDIS_OK) {
        return ret;
    }

    ret = client->returncode;
    *ret_version = client->version;

    if(client->return_value == NULL) {
        return ret;
    }

    value_item_list* vlist = (value_item_list*)(client->return_value);
    if(vlist->len == 0) {
        freeValueItemList(vlist);
        return ret;
    }

    *item = new value_item;
    value_item_iterator *it = createValueItemIterator(vlist);
    value_item_node *node = nextValueItemNode(&it);

    if(node->type == NODE_TYPE_ROBJ) {
        robj* obj = (robj*)(node->obj.obj);
        (*item)->data_len = sdslen((char*)(obj->ptr));
        (*item)->data = new char[(*item)->data_len];
        memcpy((*item)->data, obj->ptr, sdslen((char*)obj->ptr));
    } else if(node->type == NODE_TYPE_LONGLONG) {
        (*item)->data = new char[BUFFER_SIZE];
        snprintf((*item)->data,BUFFER_SIZE-1,"%lld",node->obj.llnum);
        (*item)->data_len = strlen((*item)->data);
    }

    freeValueItemIterator(&it);
    freeValueItemList(vlist);

    return ret;
}


//may will change this function,it make something more complex
//and argument is not firendly
int redis_db_session::lrpush(MKEY, MDETA, value_item* item, int item_num, LORR,
        EORN, int max_count, push_return_value *prv)
{
    int ret;
    redisClient *client;
    prv->pushed_num = 0;
    prv->list_len = 0;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(LPUSH_COMMAND)->argc + item_num - 1);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    /* max_count <= 0, mean we don't use max_count to limit length */
    client->max_count = max_count;

    if(en == IS_NOT_EXIST) {
        if(lr == IS_LEFT) {
            client->cmd = getCommand(LPUSH_COMMAND);
            client->argv[client->argc++] = NULL;
        } else {
            client->cmd = getCommand(RPUSH_COMMAND);
            client->argv[client->argc++] = NULL;
        }
    } else if(en == IS_EXIST) {
        if(lr == IS_LEFT) {
            client->cmd = getCommand(LPUSHX_COMMAND);
            client->argv[client->argc++] = NULL;
        } else {
            client->cmd = getCommand(RPUSHX_COMMAND);
            client->argv[client->argc++] = NULL;
        }
    }

    client->argv[client->argc++] =
        createStringObject(key, key_len, client->db->logiclock, version);
    for(int i = 0; i < item_num; i++) {
        client->argv[client->argc++] =
            createStringObject(item[i].data, item[i].data_len, 0, 0);
    }

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;

    if(client->return_value == NULL) {
        return ret;
    }

    prv->pushed_num = ((push_return_value*)(client->return_value))->pushed_num;
    prv->list_len = ((push_return_value*)(client->return_value))->list_len;
    zfree(client->return_value);
    client->return_value = NULL;

    return ret;
}


int redis_db_session::lrpop(MKEY, MDETA, int count, LORR, OITEMSVSN)
{
    int ret;
    redisClient *client;
    void* client_return_value = NULL;
    char str_count[BUFFER_SIZE];
    *item = NULL;
    *item_num = 0;
    snprintf(str_count,BUFFER_SIZE-1,"%d",count);

    //lock in this scope
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(LPOP_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    if(lr == IS_LEFT) {
        client->cmd = getCommand(LPOP_COMMAND);
        client->argv[client->argc++] = NULL;
    } else {
        client->cmd = getCommand(RPOP_COMMAND);
        client->argv[client->argc++] = NULL;
    }
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, version);
    client->argv[client->argc++] = createStringObject(str_count, strlen(str_count), 0, 0);

    ret = processCommand(client);
    if(ret != REDIS_OK) {
        return ret;
    }

    ret = client->returncode;
    client_return_value = client->return_value;
    *ret_version = client->version;
    client->return_value = NULL;

    if(ret != REDIS_OK || client_return_value == NULL) {
        return ret;
    }

    value_item_list* vlist = (value_item_list*)(client_return_value);
    if(vlist->len == 0) {
        freeValueItemList(vlist);
        return ret;
    }

    *item_num = vlist->len;
    *item = new value_item[*item_num];

    value_item_iterator *it = createValueItemIterator(vlist);
    value_item_node *node = NULL;
    int now = 0;
    while((node = nextValueItemNode(&it)) != NULL)
    {
        if(node->type == NODE_TYPE_ROBJ) {
            robj* obj = (robj*)(node->obj.obj);
            (*item)[now].data_len = sdslen((char*)(obj->ptr));
            (*item)[now].data = new char[(*item)[now].data_len];
            memcpy((*item)[now].data, obj->ptr, sdslen((char*)obj->ptr));
        } else if(node->type == NODE_TYPE_BUFFER) {
            (*item)[now].data_len = node->size;
            (*item)[now].data = new char[(*item)[now].data_len];
            memcpy((*item)[now].data, node->obj.obj, node->size);
        }
        ++now;
    }
    freeValueItemIterator(&it);
    freeValueItemList(vlist);

    return ret;
}

int redis_db_session::lindex(MKEY, int index, OITEMVSN)
{
    int ret;
    redisClient *client;
    char str_index[BUFFER_SIZE];
    void* client_return_value = NULL;
    *item = NULL;
    snprintf(str_index,BUFFER_SIZE-1,"%d",index);

    //lock in this scope
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(LINDEX_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(LINDEX_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key,key_len,0,0);
    client->argv[client->argc++] = createStringObject(str_index,strlen(str_index),0,0);

    ret = processCommand(client);
    if(ret != REDIS_OK) {
        return ret;
    }

    ret = client->returncode;
    *ret_version = client->version;
    client_return_value = client->return_value;
    client->return_value = NULL;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }

    if(ret != REDIS_OK || client_return_value == NULL) {
        return ret;
    }

    value_item_list* vlist = (value_item_list*)(client_return_value);
    if(vlist->len == 0) {
        freeValueItemList(vlist);
        return ret;
    }

    *item = new value_item;
    value_item_iterator *it = createValueItemIterator(vlist);
    value_item_node *node = nextValueItemNode(&it);

    if(node->type == NODE_TYPE_ROBJ) {
        robj* obj = (robj*)(node->obj.obj);
        (*item)->data_len = sdslen((char*)(obj->ptr));
        (*item)->data = new char[(*item)->data_len];
        memcpy((*item)->data, obj->ptr, sdslen((char*)obj->ptr));
    } else if(node->type == NODE_TYPE_BUFFER) {
        (*item)->data_len = node->size;
        (*item)->data = new char[(*item)->data_len];
        memcpy((*item)->data, node->obj.obj, node->size);
    }

    freeValueItemIterator(&it);
    freeValueItemList(vlist);

    return ret;
}


int redis_db_session::lrange(MKEY, int start, int end, OITEMSVSN)
{
    int ret;
    redisClient *client;
    void* client_return_value = NULL;
    *item = NULL;
    *item_num = 0;
    char str_start[BUFFER_SIZE];
    char str_end[BUFFER_SIZE];
    snprintf(str_start,BUFFER_SIZE-1,"%d",start);
    snprintf(str_end,BUFFER_SIZE-1,"%d",end);

    //lock in this scope
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(LRANGE_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(LRANGE_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    client->argv[client->argc++] = createStringObject(str_start, strlen(str_start), 0, 0);
    client->argv[client->argc++] = createStringObject(str_end, strlen(str_end), 0, 0);

    ret = processCommand(client);
    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    *ret_version = client->version;
    client_return_value = client->return_value;
    client->return_value = NULL;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }

    if(ret != REDIS_OK || client_return_value == NULL) {
        return ret;
    }

    value_item_list* vlist = (value_item_list*)(client_return_value);
    *item_num = vlist->len;
    if((*item_num) == 0) {
        freeValueItemList(vlist);
        return ret;
    }

    *item = new value_item[*item_num];
	if((*item) == NULL) {
		log_warn("value_item too large");
		return REDIS_ERR_MEMORY_ALLOCATE_ERROR;
	}

    value_item_iterator *it = createValueItemIterator(vlist);
    value_item_node *node = NULL;
    int now = 0;
    while((node = nextValueItemNode(&it)) != NULL)
    {
        if(node->type == NODE_TYPE_ROBJ) {
            robj* obj = (robj*)(node->obj.obj);
            (*item)[now].data_len = sdslen((char*)(obj->ptr));
            (*item)[now].data = new char[(*item)[now].data_len];
            memcpy((*item)[now].data, obj->ptr, (*item)[now].data_len);
        } else if(node->type == NODE_TYPE_BUFFER) {
            (*item)[now].data_len = node->size;
            (*item)[now].data = new char[(*item)[now].data_len];
            memcpy((*item)[now].data, node->obj.obj, node->size);
        }
        ++now;
    }
    freeValueItemIterator(&it);
    freeValueItemList(vlist);

    return ret;
}

int redis_db_session::lrem(MKEY, MDETA, int count, value_item* value, long long* remlen)
{
    int ret;
    redisClient *client;
    *remlen = 0;
    char str_count[BUFFER_SIZE];
    snprintf(str_count,BUFFER_SIZE-1,"%d",count);
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(LREM_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(LREM_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, version);
    client->argv[client->argc++] = createStringObject(str_count, strlen(str_count), 0, 0);
    client->argv[client->argc++] = createStringObject(value->data, value->data_len, 0, 0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }

    *remlen = client->retvalue.llnum;
    return client->returncode;
}

int redis_db_session::llen(MKEY, long long* len)
{
    int ret;
    redisClient *client;
    *len = 0;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(LLEN_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(LLEN_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }

    *len = client->retvalue.llnum;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }
    return client->returncode;
}

int redis_db_session::linsert(MKEY, MDETA, BORA, char* value_in_list,
            size_t value_in_list_len, value_item* value_insert)
{
    int ret;
    redisClient *client;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(LINDEX_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(LINSERT_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, version);
    if(ba == IS_BEFORE) {
        client->argv[client->argc++] = createStringObject(STRING_BEFORE,
                STRING_BEFORE_LEN, 0, 0);
    } else {
        client->argv[client->argc++] = createStringObject(STRING_AFTER,
                STRING_AFTER_LEN, 0, 0);
    }
    client->argv[client->argc++] = createStringObject(value_in_list,
            value_in_list_len, 0, 0);
    client->argv[client->argc++] = createStringObject(value_insert->data,
            value_insert->data_len, 0, 0);

    ret = processCommand(client);
    if(client->return_value != NULL) {
        zfree(client->return_value);
    }
    if(ret == REDIS_OK) {
        ret = client->returncode;
    }
    return ret;
}

int redis_db_session::lset(MKEY, MDETA, int index, value_item *item)
{
    int ret;
    redisClient *client;
    char str_index[BUFFER_SIZE];
    snprintf(str_index,BUFFER_SIZE-1,"%d",index);

    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(LSET_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(LSET_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len,
            client->db->logiclock, version);
    client->argv[client->argc++] = createStringObject(str_index,
            strlen(str_index), 0, 0);
    client->argv[client->argc++] = createStringObject(item->data,
            item->data_len, 0, 0);

    ret = processCommand(client);
    if(ret == REDIS_OK) {
        ret = client->returncode;
    }
    return ret;
}

int redis_db_session::ltrim(MKEY, MDETA, int start, int end)
{
    int ret;
    redisClient *client;
    char str_start[BUFFER_SIZE];
    char str_end[BUFFER_SIZE];
    snprintf(str_start,BUFFER_SIZE-1,"%d",start);
    snprintf(str_end,BUFFER_SIZE-1,"%d",end);
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(LTRIM_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(LTRIM_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, version);
    client->argv[client->argc++] = createStringObject(str_start,strlen(str_start),0,0);
    client->argv[client->argc++] = createStringObject(str_end,strlen(str_end),0,0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    return client->returncode;
}

int redis_db_session::ready_client(redisClient *client, size_t arglen)
{
    set_malloc_dbnum(client->old_dbnum);
    if(client->oldargc < client->argc) {
        client->oldargc = client->argc;
    }
    resetClient(client);
    if(((int)arglen) > client->oldargc) {
        set_malloc_dbnum(0);
        if(client->argv != NULL) {
            zfree(client->argv);
            client->argv = NULL;
            client->argc = 0;
        }
        client->argv = (robj**)zmalloc(sizeof(robj*) * arglen);

        if(client->argv == NULL) {
            return REDIS_ERR_MEMORY_ALLOCATE_ERROR;
        }
    }
    client->argc = 0;
    client->version_care = 0;
    client->version = 0;
    client->expiretime = 0;
    client->returncode = REDIS_OK;
    client->return_value = NULL;
    return REDIS_OK;
}

//hset
int redis_db_session::hgetall(MKEY, OITEMSVSN) {
    int ret;
    redisClient *client;
    void* client_return_value = NULL;
    *item = NULL;
    *item_num = 0;

    //lock in this scope
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();

    /* to filter */
    filter_field(context.server, area, key, key_len);

    ret = get_redis_client(&client,area,getCommand(HGETALL_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(HGETALL_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);

    ret = processCommand(client);
    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    *ret_version = client->version;
    client_return_value = client->return_value;
    client->return_value = NULL;

    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }

    if(ret != REDIS_OK || client_return_value == NULL) {
        return ret;
    }

    value_item_list* vlist = (value_item_list*)(client_return_value);
    if(vlist->len == 0) {
        freeValueItemList(vlist);
        return ret;
    }
    *item_num = vlist->len;
    *item = new value_item[*item_num];

    value_item_iterator *it = createValueItemIterator(vlist);
    value_item_node *node = NULL;
    int now = 0;
    while((node = nextValueItemNode(&it)) != NULL)
    {
        if(node->type == NODE_TYPE_ROBJ) {
            robj* obj = (robj*)(node->obj.obj);
            (*item)[now].data_len = sdslen((char*)(obj->ptr));
            (*item)[now].data = new char[(*item)[now].data_len];
            memcpy((*item)[now].data, obj->ptr, sdslen((char*)obj->ptr));
        } else if(node->type == NODE_TYPE_BUFFER) {
            (*item)[now].data_len = node->size;
            (*item)[now].data = new char[(*item)[now].data_len];
            memcpy((*item)[now].data, node->obj.obj, node->size);
        } else if(node->type == NODE_TYPE_LONGLONG) {
            (*item)[now].data = new char[BUFFER_SIZE];
            snprintf((*item)[now].data,BUFFER_SIZE-1,"%lld",node->obj.llnum);
            (*item)[now].data_len = strlen((*item)[now].data);
        }
        ++now;
    }
    freeValueItemIterator(&it);
    freeValueItemList(vlist);

    return ret;
}

int redis_db_session::hkeys(MKEY, OITEMSVSN) {
    int ret;
    redisClient *client;
    void* client_return_value = NULL;
    *item = NULL;
    *item_num = 0;

    //lock in this scope
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();

    /* to filter */
    filter_field(context.server, area, key, key_len);

    ret = get_redis_client(&client,area,getCommand(HKEYS_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(HKEYS_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);

    ret = processCommand(client);
    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    *ret_version = client->version;
    client_return_value = client->return_value;
    client->return_value = NULL;

    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }

    if(ret != REDIS_OK || client_return_value == NULL) {
        return ret;
    }

    value_item_list* vlist = (value_item_list*)(client_return_value);
    if(vlist->len == 0) {
        freeValueItemList(vlist);
        return ret;
    }
    *item_num = vlist->len;
    *item = new value_item[*item_num];

    value_item_iterator *it = createValueItemIterator(vlist);
    value_item_node *node = NULL;
    int now = 0;
    while((node = nextValueItemNode(&it)) != NULL)
    {
        if(node->type == NODE_TYPE_ROBJ) {
            robj* obj = (robj*)(node->obj.obj);
            (*item)[now].data_len = sdslen((char*)(obj->ptr));
            (*item)[now].data = new char[(*item)[now].data_len];
            memcpy((*item)[now].data, obj->ptr, sdslen((char*)obj->ptr));
        } else if(node->type == NODE_TYPE_BUFFER) {
            (*item)[now].data_len = node->size;
            (*item)[now].data = new char[(*item)[now].data_len];
            memcpy((*item)[now].data, node->obj.obj, node->size);
        } else if(node->type == NODE_TYPE_LONGLONG) {
            (*item)[now].data = new char[BUFFER_SIZE];
            snprintf((*item)[now].data,BUFFER_SIZE-1,"%lld",node->obj.llnum);
            (*item)[now].data_len = strlen((*item)[now].data);
        }
        ++now;
    }
    freeValueItemIterator(&it);
    freeValueItemList(vlist);

    return ret;
}

int redis_db_session::hget(MKEY, CFIELD, OITEMVSN)
{
    int ret;
    redisClient *client;
    void* client_return_value = NULL;
    *item = NULL;

    //lock this scope
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(HGET_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    unsigned int timestamp = search_filter_node(&(client->db->filter_list),
            FILTER_TYPE_FIELD, field_len, field);

    client->db->read_count++;

    client->cmd = getCommand(HGET_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    client->argv[client->argc++] = createStringObject(field,field_len, timestamp, 0);

    ret = processCommand(client);
    if(ret != REDIS_OK) {
        return ret;
    }

    ret = client->returncode;
    *ret_version = client->version;
    client_return_value = client->return_value;
    client->return_value = NULL;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }

    if(ret != REDIS_OK || client_return_value == NULL) {
        return ret;
    }

    value_item_list* vlist = (value_item_list*)(client_return_value);
    if(vlist->len == 0) {
        freeValueItemList(vlist);
        return ret;
    }

    *item = new value_item;
    value_item_iterator *it = createValueItemIterator(vlist);
    value_item_node *node = nextValueItemNode(&it);

    if(node->type == NODE_TYPE_ROBJ) {
        robj* obj = (robj*)(node->obj.obj);
        (*item)->data_len = sdslen((char*)(obj->ptr));
        (*item)->data = new char[(*item)->data_len];
        memcpy((*item)->data, obj->ptr, sdslen((char*)obj->ptr));
    } else if(node->type == NODE_TYPE_BUFFER) {
        (*item)->data_len = node->size;
        (*item)->data = new char[(*item)->data_len];
        memcpy((*item)->data, node->obj.obj, node->size);
    } else if(node->type == NODE_TYPE_LONGLONG) {
        (*item)->data = new char[BUFFER_SIZE];
        snprintf((*item)->data,BUFFER_SIZE-1,"%lld",node->obj.llnum);
        (*item)->data_len = strlen((*item)->data);
    }

    freeValueItemIterator(&it);
    freeValueItemList(vlist);

    return ret;
}

int redis_db_session::hincrby(MKEY, MDETA, value_item* field, long long addvalue, long long *retvalue) {
    int ret;
    redisClient *client;
    *retvalue = 0;
    char str_addvalue[BUFFER_SIZE];
    snprintf(str_addvalue,BUFFER_SIZE-1,"%lld",addvalue);
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(HINCRBY_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    unsigned int timestamp = search_filter_node(&(client->db->filter_list),
            FILTER_TYPE_FIELD, field->data_len, field->data);

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(HINCRBY_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, version);
    client->argv[client->argc++] = createStringObject(field->data,
            field->data_len, timestamp, 0);
    client->argv[client->argc++] = createStringObject(str_addvalue,
            strlen(str_addvalue), 0, 0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    if(ret != REDIS_OK) {
        return ret;
    }

    *retvalue = client->retvalue.llnum;

    return ret;
}

int redis_db_session::hmget(MKEY, MFIELD, OITEMSVSN)
{
    int ret;
    redisClient *client;
    void* client_return_value = NULL;
    *item = NULL;
    *item_num = 0;

    //lock in this scope
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(HMGET_COMMAND)->argc + field_len - 1);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(HMGET_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    for(int i = 0; i < field_len; i++) {
        unsigned int timestamp = search_filter_node(&(client->db->filter_list),
            FILTER_TYPE_FIELD, field[i].data_len, field[i].data);
        client->argv[client->argc++] = createStringObject(field[i].data,
                field[i].data_len,timestamp,0);
    }

    ret = processCommand(client);
    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    *ret_version = client->version;
    client_return_value = client->return_value;
    client->return_value = NULL;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }

    if(ret != REDIS_OK || client_return_value == NULL) {
        return ret;
    }

    value_item_list* vlist = (value_item_list*)(client_return_value);
    if(vlist->len == 0) {
        freeValueItemList(vlist);
        return ret;
    }
    *item_num = vlist->len;
    *item = new value_item[*item_num];

    value_item_iterator *it = createValueItemIterator(vlist);
    value_item_node *node = NULL;
    int now = 0;
    while((node = nextValueItemNode(&it)) != NULL)
    {
        if(node->type == NODE_TYPE_NULL) {
            (*item)[now].data_len = 0;
            (*item)[now].data = NULL;
        }else if(node->type == NODE_TYPE_ROBJ) {
            robj* obj = (robj*)(node->obj.obj);
            (*item)[now].data_len = sdslen((char*)(obj->ptr));
            (*item)[now].data = new char[(*item)[now].data_len];
            memcpy((*item)[now].data, obj->ptr, sdslen((char*)obj->ptr));
        } else if(node->type == NODE_TYPE_BUFFER) {
            (*item)[now].data_len = node->size;
            (*item)[now].data = new char[(*item)[now].data_len];
            memcpy((*item)[now].data, node->obj.obj, node->size);
        } else if(node->type == NODE_TYPE_LONGLONG) {
            (*item)[now].data = new char[BUFFER_SIZE];
            snprintf((*item)[now].data,BUFFER_SIZE-1,"%lld",node->obj.llnum);
            (*item)[now].data_len = strlen((*item)[now].data);
        }
        ++now;
    }
    freeValueItemIterator(&it);
    freeValueItemList(vlist);

    return ret;
}

int redis_db_session::hset(MKEY, MDETA, CFIELD, value_item* item)
{
    int ret;
    redisClient *client;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(HSET_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    unsigned int timestamp = search_filter_node(&(client->db->filter_list),
            FILTER_TYPE_FIELD, field_len, field);

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(HSET_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len,
            client->db->logiclock, version);
    client->argv[client->argc++] = createStringObject(field,field_len,timestamp,0);
    client->argv[client->argc++] = createStringObject(item->data,
            item->data_len,0,0);

    ret = processCommand(client);
    if(ret == REDIS_OK) {
        ret = client->returncode;
    }
    return ret;
}

int redis_db_session::hmset(MKEY, MDETA, value_item* field_value, int field_val_len, int* retvalue)
{
    int ret;
    redisClient *client;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,2+field_val_len);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(HMSET_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, version);
    for(int i = 0; i < field_val_len; i += 2) {
        unsigned int timestamp = search_filter_node(&(client->db->filter_list),
            FILTER_TYPE_FIELD, field_value[i].data_len, field_value[i].data);
        client->argv[client->argc++] = createStringObject(field_value[i].data,
                field_value[i].data_len,timestamp,0);
        client->argv[client->argc++] = createStringObject(field_value[i+1].data,
                field_value[i+1].data_len,0,0);
    }

    ret = processCommand(client);
    if(ret == REDIS_OK) {
        ret = client->returncode;
    }

    *retvalue = (int)(client->retvalue.llnum);
    return ret;

}

int redis_db_session::hsetnx(MKEY, MDETA, CFIELD, value_item* item)
{
    int ret;
    redisClient *client;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(HSETNX_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    unsigned int timestamp = search_filter_node(&(client->db->filter_list),
            FILTER_TYPE_FIELD, field_len, field);

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(HSETNX_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len,
            client->db->logiclock, version);
    client->argv[client->argc++] = createStringObject(field,field_len,timestamp,0);
    client->argv[client->argc++] = createStringObject(item->data,
            item->data_len,0,0);

    ret = processCommand(client);
    if(ret == REDIS_OK) {
        ret = client->returncode;
    }
    return ret;
}

int redis_db_session::filter_field(redisServer* server, int dbnum, char* key, size_t key_len) {
    int ret;
    redisClient *client = selectClient(server, dbnum);
    if (client->db->filter_list.head) {
        filterListIterator* iter =
            create_filter_list_iterator(&(client->db->filter_list), FILTER_TYPE_FIELD);
        struct filterNode* node = NULL;
        while((node = next_filter_node(iter)) != NULL) {
            ret = get_redis_client(&client,dbnum,getCommand(FILTER_COMMAND)->argc);
            if (ret != REDIS_OK) {
                return ret;
            }
            client->cmd = getCommand(FILTER_COMMAND);
            client->argv[client->argc++] = NULL;
            client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
            client->argv[client->argc++] = createStringObject(node->buff, node->len, node->timestamp, 0);
            processCommand(client);
        }
        free_filter_list_iterator(iter);
    }
    return REDIS_OK;
}

int redis_db_session::hvals(MKEY, OITEMSVSN) {
    int ret;
    redisClient *client;
    void* client_return_value = NULL;
    *item = NULL;
    *item_num = 0;

    //lock in this scope
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();

    /* to filter */
    filter_field(context.server, area, key, key_len);

    ret = get_redis_client(&client,area,getCommand(HVALS_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(HVALS_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);

    ret = processCommand(client);
    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    *ret_version = client->version;
    client_return_value = client->return_value;
    client->return_value = NULL;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }

    if(ret != REDIS_OK || client_return_value == NULL) {
        return ret;
    }

    value_item_list* vlist = (value_item_list*)(client_return_value);
    if(vlist->len == 0) {
        freeValueItemList(vlist);
        return ret;
    }
    *item_num = vlist->len;
    *item = new value_item[*item_num];

    value_item_iterator *it = createValueItemIterator(vlist);
    value_item_node *node = NULL;
    int now = 0;
    while((node = nextValueItemNode(&it)) != NULL)
    {
        if(node->type == NODE_TYPE_ROBJ) {
            robj* obj = (robj*)(node->obj.obj);
            (*item)[now].data_len = sdslen((char*)(obj->ptr));
            (*item)[now].data = new char[(*item)[now].data_len];
            memcpy((*item)[now].data, obj->ptr, sdslen((char*)obj->ptr));
        } else if(node->type == NODE_TYPE_BUFFER) {
            (*item)[now].data_len = node->size;
            (*item)[now].data = new char[(*item)[now].data_len];
            memcpy((*item)[now].data, node->obj.obj, node->size);
        } else if(node->type == NODE_TYPE_LONGLONG) {
            (*item)[now].data = new char[BUFFER_SIZE];
            snprintf((*item)[now].data,BUFFER_SIZE-1,"%lld",node->obj.llnum);
            (*item)[now].data_len = strlen((*item)[now].data);
        }
        ++now;
    }
    freeValueItemIterator(&it);
    freeValueItemList(vlist);

    return ret;
}

int redis_db_session::hdel(MKEY, MDETA, CFIELD) {
    int ret;
    redisClient *client;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(HDEL_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(HDEL_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, version);
    client->argv[client->argc++] = createStringObject(field, field_len,0,0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    return client->returncode;
}

int redis_db_session::hlen(MKEY, long long* len) {
	int ret;
    redisClient *client;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(HLEN_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(HLEN_COMMAND);
	client->argv[client->argc++] = NULL;
	client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);

	ret = processCommand(client);

	if(ret != REDIS_OK) {
		return ret;
	}
	*len = client->retvalue.llnum;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }
	return client->returncode;
}
//set
int redis_db_session::scard(MKEY, long long* retnum) {
    int ret;
    redisClient *client;
    *retnum = 0;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(SCARD_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(SCARD_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    if(ret != REDIS_OK) {
        return ret;
    }

    *retnum = client->retvalue.llnum;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }
    return ret;
}

int redis_db_session::smembers(MKEY, OITEMSVSN) {
    int ret;
    redisClient *client;
    void* client_return_value = NULL;
    *item = NULL;
    *item_num = 0;

    //lock in this scope
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(SMEMBERS_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(SMEMBERS_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);

    ret = processCommand(client);
    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    *ret_version = client->version;
    client_return_value = client->return_value;
    client->return_value = NULL;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }

    if(ret != REDIS_OK || client_return_value == NULL) {
        return ret;
    }

    value_item_list* vlist = (value_item_list*)(client_return_value);
    if(vlist->len == 0) {
        freeValueItemList(vlist);
        return ret;
    }
    *item_num = vlist->len;
    *item = new value_item[*item_num];

    value_item_iterator *it = createValueItemIterator(vlist);
    value_item_node *node = NULL;
    int now = 0;
    while((node = nextValueItemNode(&it)) != NULL)
    {
        if(node->type == NODE_TYPE_ROBJ) {
            robj* obj = (robj*)(node->obj.obj);
            (*item)[now].data_len = sdslen((char*)(obj->ptr));
            (*item)[now].data = new char[(*item)[now].data_len];
            memcpy((*item)[now].data, obj->ptr, sdslen((char*)obj->ptr));
        } else if(node->type == NODE_TYPE_BUFFER) {
            (*item)[now].data_len = node->size;
            (*item)[now].data = new char[(*item)[now].data_len];
            memcpy((*item)[now].data, node->obj.obj, node->size);
        } else if(node->type == NODE_TYPE_LONGLONG) {
            (*item)[now].data = new char[BUFFER_SIZE];
            snprintf((*item)[now].data,BUFFER_SIZE-1,"%lld",node->obj.llnum);
            (*item)[now].data_len = strlen((*item)[now].data);
        }
        ++now;
    }
    freeValueItemIterator(&it);
    freeValueItemList(vlist);

    return ret;
}

int redis_db_session::sadd(MKEY, MDETA, value_item* item)
{
    int ret;
    redisClient *client;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(SADD_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(SADD_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len,
            client->db->logiclock, version);
    client->argv[client->argc++] = createStringObject(item->data,
            item->data_len,0,0);

    ret = processCommand(client);
    if(ret == REDIS_OK) {
        ret = client->returncode;
    }
    return ret;
}

int redis_db_session::spop(MKEY, MDETA, OITEMVSN) {
    int ret;
    redisClient *client;
    void* client_return_value = NULL;
    *item = NULL;

    //lock in this scope
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(SPOP_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(SPOP_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, version);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    *ret_version = client->version;
    client_return_value = client->return_value;
    client->return_value = NULL;

    if(ret != REDIS_OK || client_return_value == NULL) {
        return ret;
    }

    value_item_list* vlist = (value_item_list*)(client_return_value);
    if(vlist->len == 0) {
        freeValueItemList(vlist);
        return ret;
    }

    *item = new value_item;
    value_item_iterator *it = createValueItemIterator(vlist);
    value_item_node *node = nextValueItemNode(&it);

    if(node->type == NODE_TYPE_ROBJ) {
        robj* obj = (robj*)(node->obj.obj);
        (*item)->data_len = sdslen((char*)(obj->ptr));
        (*item)->data = new char[(*item)->data_len];
        memcpy((*item)->data, obj->ptr, sdslen((char*)obj->ptr));
    } else if(node->type == NODE_TYPE_BUFFER) {
        (*item)->data_len = node->size;
        (*item)->data = new char[(*item)->data_len];
        memcpy((*item)->data, node->obj.obj, node->size);
    } else if(node->type == NODE_TYPE_LONGLONG) {
        (*item)->data = new char[BUFFER_SIZE];
        snprintf((*item)->data,BUFFER_SIZE-1,"%lld",node->obj.llnum);
        (*item)->data_len = strlen((*item)->data);
    }

    freeValueItemIterator(&it);
    freeValueItemList(vlist);

    return ret;
}

int redis_db_session::srem(MKEY, MDETA, value_item* item)
{
    int ret;
    redisClient *client;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(SREM_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->remove_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(SREM_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key,key_len,client->db->logiclock,version);
    client->argv[client->argc++] = createStringObject(item->data,item->data_len,0,0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    return client->returncode;
}

//zset
int redis_db_session::zscore(MKEY, char* value, size_t value_len, double* retscore)
{
    int ret;
    redisClient *client;
    *retscore = 0;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(ZSCORE_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(ZSCORE_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    client->argv[client->argc++] = createStringObject(value,value_len, 0, 0);
    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    if(ret != REDIS_OK) {
        return ret;
    }

    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }
    *retscore = client->retvalue.dnum;
    return ret;

}

int redis_db_session::zrevrange(MKEY, int start, int end, OITEMSVSN,
       double** scores_items, int* scores_items_len,  int withscore)
{
    int ret;
    redisClient *client;
    void* client_return_value = NULL;
    *item = NULL;
    *item_num = 0;
    char str_start[BUFFER_SIZE];
    char str_end[BUFFER_SIZE];
    snprintf(str_start,BUFFER_SIZE-1,"%d",start);
    snprintf(str_end,BUFFER_SIZE-1,"%d",end);

    //lock in this scope
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(ZREVRANGE_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    if (withscore == 0) {
        client->cmd = getCommand(ZREVRANGE_COMMAND);
    } else {
        client->cmd = getCommand(ZREVRANGEWITHSCORE_COMMAND);
    }
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    client->argv[client->argc++] = createStringObject(str_start, strlen(str_start), 0, 0);
    client->argv[client->argc++] = createStringObject(str_end, strlen(str_end), 0, 0);

    ret = processCommand(client);
    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    *ret_version = client->version;
    client_return_value = client->return_value;
    client->return_value = NULL;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }

    if(ret != REDIS_OK || client_return_value == NULL) {
        return ret;
    }

    value_item_list* vlist = (value_item_list*)(client_return_value);
    if(vlist->len == 0) {
        freeValueItemList(vlist);
        return ret;
    }

    if (withscore == 0) {
        *item_num = vlist->len;
        *item = new value_item[*item_num];
    } else {
        *item_num = vlist->len / 2;
        *item = new value_item[*item_num];

        *scores_items_len = *item_num;
        *scores_items = new double[*scores_items_len];
    }

    value_item_iterator *it = createValueItemIterator(vlist);
    value_item_node *node = NULL;
    int now = 0;
    int items_index = 0;
    int scores_items_index = 0;
    while((node = nextValueItemNode(&it)) != NULL)
    {
        if(node->type == NODE_TYPE_ROBJ) {
            robj* obj = (robj*)(node->obj.obj);
            (*item)[items_index].data_len = sdslen((char*)(obj->ptr));
            (*item)[items_index].data = new char[(*item)[items_index].data_len];
            memcpy((*item)[items_index].data, obj->ptr, sdslen((char*)obj->ptr));
            items_index++;
        } else if(node->type == NODE_TYPE_BUFFER) {
            (*item)[items_index].data_len = node->size;
            (*item)[items_index].data = new char[(*item)[items_index].data_len];
            memcpy((*item)[items_index].data, node->obj.obj, node->size);
            items_index++;
        } else if(node->type == NODE_TYPE_DOUBLE) {
            //may should redefine value_item then decr once copy
            if (now % 2 == 0) {
                (*item)[items_index].data = new char[BUFFER_SIZE];
                snprintf((*item)[items_index].data,BUFFER_SIZE-1,"%6lf",node->obj.dnum);
                (*item)[items_index].data_len = strlen((*item)[items_index].data);
                items_index++;
            } else {
                (*scores_items)[scores_items_index] = node->obj.dnum;
                scores_items_index++;
            }
        }
        ++now;
    }
    freeValueItemIterator(&it);
    freeValueItemList(vlist);

    return ret;
}

int redis_db_session::zrange(MKEY, int start, int end, OITEMSVSN,
       double** scores_items, int* scores_items_len,  int withscore)
{
    int ret;
    redisClient *client;
    void* client_return_value = NULL;
    *item = NULL;
    *item_num = 0;
    char str_start[BUFFER_SIZE];
    char str_end[BUFFER_SIZE];
    snprintf(str_start,BUFFER_SIZE-1,"%d",start);
    snprintf(str_end,BUFFER_SIZE-1,"%d",end);

    //lock in this scope
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(ZRANGE_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    if (withscore == 0) {
        client->cmd = getCommand(ZRANGE_COMMAND);
    } else {
        client->cmd = getCommand(ZRANGEWITHSCORE_COMMAND);
    }
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    client->argv[client->argc++] = createStringObject(str_start, strlen(str_start), 0, 0);
    client->argv[client->argc++] = createStringObject(str_end, strlen(str_end), 0, 0);

    ret = processCommand(client);
    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    *ret_version = client->version;
    client_return_value = client->return_value;
    client->return_value = NULL;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }

    if(ret != REDIS_OK || client_return_value == NULL) {
        return ret;
    }

    value_item_list* vlist = (value_item_list*)(client_return_value);
    if(vlist->len == 0) {
        freeValueItemList(vlist);
        return ret;
    }

    if (withscore == 0) {
        *item_num = vlist->len;
        *item = new value_item[*item_num];
    } else {
        *item_num = vlist->len / 2;
        *item = new value_item[*item_num];

        *scores_items_len = *item_num;
        *scores_items = new double[*scores_items_len];
    }

    value_item_iterator *it = createValueItemIterator(vlist);
    value_item_node *node = NULL;
    int now = 0;
    int items_index = 0;
    int scores_items_index = 0;
    while((node = nextValueItemNode(&it)) != NULL)
    {
        if(node->type == NODE_TYPE_ROBJ) {
            robj* obj = (robj*)(node->obj.obj);
            (*item)[items_index].data_len = sdslen((char*)(obj->ptr));
            (*item)[items_index].data = new char[(*item)[items_index].data_len];
            memcpy((*item)[items_index].data, obj->ptr, sdslen((char*)obj->ptr));
            items_index++;
        } else if(node->type == NODE_TYPE_BUFFER) {
            (*item)[items_index].data_len = node->size;
            (*item)[items_index].data = new char[(*item)[items_index].data_len];
            memcpy((*item)[items_index].data, node->obj.obj, node->size);
            items_index++;
        } else if(node->type == NODE_TYPE_DOUBLE) {
            //may should redefine value_item then decr once copy
            if (withscore != 0 && now % 2 == 1) {
                (*scores_items)[scores_items_index] = node->obj.dnum;
                scores_items_index++;
            } else {
                (*item)[items_index].data = new char[BUFFER_SIZE];
                snprintf((*item)[items_index].data,BUFFER_SIZE-1,"%6lf",node->obj.dnum);
                (*item)[items_index].data_len = strlen((*item)[items_index].data);
                items_index++;
            }
        }
        ++now;
    }
    freeValueItemIterator(&it);
    freeValueItemList(vlist);

    return ret;
}

int redis_db_session::genericZrangebyscore(MKEY, double start, double end,
        OITEMSVSN, double** scores_items, int* scores_items_len, int limit, int withscore, bool reverse)
{
    log_debug("enter redis_db_session::genericZrangebyscore()\n");
    int ret;
    redisClient *client;
    void* client_return_value = NULL;
    *item = NULL;
    *item_num = 0;
    char str_start[BUFFER_SIZE];
    char str_end[BUFFER_SIZE];
    char str_limit[BUFFER_SIZE];
    char str_withscore[BUFFER_SIZE];
    snprintf(str_start,BUFFER_SIZE-1,"%6lf",start);
    snprintf(str_end,BUFFER_SIZE-1,"%6lf",end);
    if (limit != -1 || withscore != 0) {
        snprintf(str_limit,BUFFER_SIZE-1,"%d",limit);
        snprintf(str_withscore,BUFFER_SIZE-1,"%d",withscore);
    }

    int cmd = ZRANGEBYSCORE_COMMAND;
    if (reverse) {
        cmd = ZREVRANGEBYSCORE_COMMAND;
    }

    //lock in this scope
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(cmd)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(cmd);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    client->argv[client->argc++] = createStringObject(str_start, strlen(str_start), 0, 0);
    client->argv[client->argc++] = createStringObject(str_end, strlen(str_end), 0, 0);
    if (limit != -1 || withscore != 0) {
        client->argv[client->argc++] = createStringObject(str_limit, strlen(str_limit), 0, 0);
        client->argv[client->argc++] = createStringObject(str_withscore, strlen(str_withscore), 0, 0);
    }

    ret = processCommand(client);
    if(ret != REDIS_OK) {
        return ret;
    }

    ret = client->returncode;
    *ret_version = client->version;
    client_return_value = client->return_value;
    client->return_value = NULL;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }

    if(ret != REDIS_OK || client_return_value == NULL) {
        return ret;
    }

    value_item_list* vlist = (value_item_list*)(client_return_value);
    if(vlist->len == 0) {
        freeValueItemList(vlist);
        return ret;
    }

    if (withscore == 0) {
        *item_num = vlist->len;
        *item = new value_item[*item_num];
    } else {
        *item_num = vlist->len / 2;
        *item = new value_item[*item_num];

        *scores_items_len = *item_num;
        *scores_items = new double[*scores_items_len];
    }

    value_item_iterator *it = createValueItemIterator(vlist);
    value_item_node *node = NULL;
    int now = 0;
    int items_index = 0;
    int scores_items_index = 0;
    while((node = nextValueItemNode(&it)) != NULL)
    {
        log_debug("redis_db_session::genericZrangebyscore() node->type=%d,size=%u\n",node->type,node->size);
        if(node->type == NODE_TYPE_ROBJ) {
            robj* obj = (robj*)(node->obj.obj);
            log_debug("redis_db_session::genericZrangebyscore() obj->encoding=%u",obj->encoding);
            if(obj->encoding== REDIS_STRING)
            {
                (*item)[items_index].data_len = sdslen((char*)(obj->ptr));
                (*item)[items_index].data = new char[(*item)[items_index].data_len];
                memcpy((*item)[items_index].data, obj->ptr, sdslen((char*)obj->ptr));
                items_index++;
            }
            else if(obj->encoding==REDIS_ENCODING_INT)
            {
                //decode the string from long/void*
                char * long_to_str=new char[31];
                snprintf(long_to_str,31,"%ld",obj->ptr);
                (*item)[items_index].data_len = strnlen(long_to_str,31); 
                (*item)[items_index].data = long_to_str; 
                items_index++;
                log_debug("redis_db_session::genericZrangebyscore() REDIS_ENCODING_INT ptr=%ld,data=%s",obj->ptr,long_to_str );
            }
        } else if(node->type == NODE_TYPE_BUFFER) {
            (*item)[items_index].data_len = node->size;
            (*item)[items_index].data = new char[(*item)[items_index].data_len];
            memcpy((*item)[items_index].data, node->obj.obj, node->size);
            items_index++;
        } else if(node->type == NODE_TYPE_DOUBLE) {
            //may should redefine value_item then decr once copy
            if (now % 2 == 1 && withscore != 0) {
                (*scores_items)[scores_items_index] = node->obj.dnum;
                scores_items_index++;
            } else {
                (*item)[items_index].data = new char[BUFFER_SIZE];
                snprintf((*item)[items_index].data,BUFFER_SIZE-1,"%6lf",node->obj.dnum);
                (*item)[items_index].data_len = strlen((*item)[items_index].data);
                items_index++;
            }
        }
        ++now;
    }
    freeValueItemIterator(&it);
    freeValueItemList(vlist);

    return ret;
}

int redis_db_session::zadd(MKEY, MDETA, double score, value_item* item)
{
    int ret;
    redisClient *client;
    char str_score[BUFFER_SIZE];
    snprintf(str_score,BUFFER_SIZE-1,"%6lf",score);
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(ZADD_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(ZADD_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len,
            client->db->logiclock, version);
    client->argv[client->argc++] = createStringObject(str_score,
            strlen(str_score), 0, 0);
    client->argv[client->argc++] = createStringObject(item->data,
            item->data_len, 0, 0);

    ret = processCommand(client);
    if(ret == REDIS_OK) {
        ret = client->returncode;
    }

	return ret;
}

int redis_db_session::zrank(MKEY, char* value, size_t value_len, long long* rank)
{
    int ret;
    redisClient *client;
    *rank = 0;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(ZRANK_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(ZRANK_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    client->argv[client->argc++] = createStringObject(value,value_len, 0, 0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    if(ret != REDIS_OK) {
        return ret;
    }

    *rank = client->retvalue.llnum;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }
    return ret;
}

int redis_db_session::zrevrank(MKEY, char* value, size_t value_len, long long* rank)
{
    int ret;
    redisClient *client;
    *rank = 0;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(ZREVRANK_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(ZREVRANK_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    client->argv[client->argc++] = createStringObject(value,value_len, 0, 0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    if(ret != REDIS_OK) {
        return ret;
    }

    *rank = client->retvalue.llnum;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }

	return ret;
}

int redis_db_session::zcount(MKEY, double start, double end, long long *retnum)
{
    int ret;
    redisClient *client;
    char str_start[BUFFER_SIZE];
    char str_end[BUFFER_SIZE];
    snprintf(str_start,BUFFER_SIZE-1,"%6lf",start);
    snprintf(str_end,BUFFER_SIZE-1,"%6lf",end);
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(ZCOUNT_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(ZCOUNT_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    client->argv[client->argc++] = createStringObject(str_start, strlen(str_start), 0, 0);
    client->argv[client->argc++] = createStringObject(str_end, strlen(str_end), 0, 0);

    ret = processCommand(client);
    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    if(ret != REDIS_OK) {
        return ret;
    }

	*retnum = client->retvalue.llnum;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }
	return ret;
}

int redis_db_session::zincrby(MKEY, MDETA, value_item* value, double addscore, double *retvalue)
{
    int ret;
    redisClient *client;
    *retvalue = 0;
    char str_addscore[BUFFER_SIZE];
    snprintf(str_addscore,BUFFER_SIZE-1,"%6lf",addscore);
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(ZINCRBY_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(ZINCRBY_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, version);
    client->argv[client->argc++] = createStringObject(str_addscore,
            strlen(str_addscore), 0, 0);
    client->argv[client->argc++] = createStringObject(value->data,
            value->data_len, 0, 0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    if(ret != REDIS_OK) {
        return ret;
    }

    *retvalue = client->retvalue.dnum;

    return ret;
}

int redis_db_session::zrem(MKEY, MDETA, value_item* item)
{
    int ret;
    redisClient *client;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(ZREM_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(ZREM_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key,key_len,client->db->logiclock,version);
    client->argv[client->argc++] = createStringObject(item->data,item->data_len,0,0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    return client->returncode;
}

int redis_db_session::zremrangebyrank(MKEY, MDETA, int start, int end, long long* remnum)
{
    int ret;
    redisClient *client;
    char str_start[BUFFER_SIZE];
    char str_end[BUFFER_SIZE];
    snprintf(str_start,BUFFER_SIZE-1,"%d",start);
    snprintf(str_end,BUFFER_SIZE-1,"%d",end);

    PROFILER_BEGIN("db context lock");
	DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(ZREMRANGEBYRANK_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(ZREMRANGEBYRANK_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key,key_len,client->db->logiclock,version);
    client->argv[client->argc++] = createStringObject(str_start, strlen(str_start),0,0);
    client->argv[client->argc++] = createStringObject(str_end, strlen(str_end),0,0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
	if(ret != REDIS_OK) {
		return ret;
	}

	*remnum = client->retvalue.llnum;
	return ret;
}

int redis_db_session::zremrangebyscore(MKEY, MDETA, double start, double end, long long* remnum)
{
    int ret;
    redisClient *client;
    char str_start[BUFFER_SIZE];
    char str_end[BUFFER_SIZE];
    snprintf(str_start,BUFFER_SIZE-1,"%6lf",start);
    snprintf(str_end,BUFFER_SIZE-1,"%6lf",end);

    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(ZREMRANGEBYSCORE_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->write_count++;

    client->version_care = version_care;
    client->expiretime = expiretime;
    client->cmd = getCommand(ZREMRANGEBYSCORE_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key,key_len,client->db->logiclock,version);
    client->argv[client->argc++] = createStringObject(str_start, strlen(str_start), 0, 0);
    client->argv[client->argc++] = createStringObject(str_end, strlen(str_end), 0, 0);

	ret = processCommand(client);

	*remnum = client->retvalue.llnum;

	if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
	if(ret != REDIS_OK) {
		return ret;
	}

	return ret;
}

int redis_db_session::zcard(MKEY, long long* retnum)
{
    int ret;
    redisClient *client;
    *retnum = 0;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(ZCARD_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(ZCARD_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    ret = client->returncode;
    if(ret != REDIS_OK) {
        return ret;
    }

    *retnum = client->retvalue.llnum;
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }
    return ret;
}

int redis_db_session::hexists(MKEY, CFIELD)
{
    int ret;
    redisClient *client;
    PROFILER_BEGIN("db context lock");
    DB_CONTEXT_LOCK(area);
    PROFILER_END();
    ret = get_redis_client(&client,area,getCommand(HEXISTS_COMMAND)->argc);
    if (ret != REDIS_OK) {
        return ret;
    }

    client->db->read_count++;

    client->cmd = getCommand(HEXISTS_COMMAND);
    client->argv[client->argc++] = NULL;
    client->argv[client->argc++] = createStringObject(key, key_len, client->db->logiclock, 0);
    client->argv[client->argc++] = createStringObject(field, field_len, 0, 0);

    ret = processCommand(client);

    if(ret != REDIS_OK) {
        return ret;
    }
    if(client->returncode == REDIS_OK) {
        client->db->hit_count++;
    }
    return client->returncode;
}
