#include "redis.h"

#include <signal.h>

/*-----------------------------------------------------------------------------
 * C-level DB API
 *----------------------------------------------------------------------------*/

robj *lookupKeyWithVersion(redisDb *db, robj *key, uint16_t* version) {
    dictEntry *de = dictFind(db->dict, key->ptr);
    if(de) {
        robj *val = dictGetEntryVal(de);
        sds key_tmp = dictGetEntryKey(de);

        if (version) {
            *version = sdsversion(key_tmp);
        }

        val->lru = shared.lruclock;
        return val;
    } else {
        *version = 0;
        return NULL;
    }
}

robj *lookupKeyReadWithVersion(redisDb *db, robj *key, uint16_t *version) {
    expireIfNeeded(db,key);
    if (version) {
        *version = 0;
        return lookupKeyWithVersion(db,key,version);
    }
    return lookupKeyWithVersion(db,key,NULL);
}

robj *lookupKeyWriteWithVersion(redisDb *db, robj *key, uint16_t *version) {
    expireIfNeeded(db,key);
    if (version) {
        *version = 0;
        return lookupKeyWithVersion(db,key,version);
    }
    return lookupKeyWithVersion(db,key,NULL);
}

/* Add the key to the DB. If the key already exists REDIS_ERR is returned,
 * otherwise REDIS_OK is returned, and the caller should increment the
 * refcount of 'val'. */
int dbAdd(redisDb *db, robj *key, robj *val) {
    /* Perform a lookup before adding the key, as we need to copy the
     * key value. */
    if (dictFind(db->dict, key->ptr) != NULL) {
        return REDIS_ERR;
    } else {
        sds copy = sdsdup(key->ptr);
        dictAdd(db->dict, copy, val);
        return REDIS_OK;
    }
}

/* modify  key in dict */
int dbUpdateKey(redisDb *db, robj* key) {
    if (dictFind(db->dict, key->ptr) == NULL) {
        return 0;
    }

    return dictUpdateKey(db->dict, key->ptr);
}

/* like dbReplace but it will change key in db
 * version will will change */
int dbSuperReplace(redisDb *db, robj *key, robj *val) {
    if (dictFind(db->dict,key->ptr) == NULL) {
        sds copy = sdsdup(key->ptr);
        dictAdd(db->dict, copy, val);
        return 1;
    } else {
        dictSuperReplace(db->dict, key->ptr, val);
        return 0;
    }
}

/* If the key does not exist, this is just like dbAdd(). Otherwise
 * the value associated to the key is replaced with the new one.
 *
 * On update (key already existed) 0 is returned. Otherwise 1. */
int dbReplace(redisDb *db, robj *key, robj *val) {
    if (dictFind(db->dict,key->ptr) == NULL) {
        sds copy = sdsdup(key->ptr);
        dictAdd(db->dict, copy, val);
        return 1;
    } else {
        dictReplace(db->dict, key->ptr, val);
        return 0;
    }
}

int dbExists(redisDb *db, robj *key) {
    return dictFind(db->dict,key->ptr) != NULL;
}

/* Return a random key, in form of a Redis object.
 * If there are no keys, NULL is returned.
 *
 * The function makes sure to return keys not already expired. */
robj *dbRandomKey(redisDb *db) {
    struct dictEntry *de;

    while(1) {
        sds key;
        robj *keyobj;

        de = dictGetRandomKey(db->dict);
        if (de == NULL) return NULL;

        key = dictGetEntryKey(de);
        keyobj = createStringObject(key,sdslen(key),sdslogiclock(key),sdsversion(key));
        if (dictFind(db->expires,key)) {
            if (expireIfNeeded(db,keyobj)) {
                decrRefCount(keyobj);
                continue; /* search for another key. This expired. */
            }
        }
        return keyobj;
    }
}

/* Delete a key, value, and associated expiration entry if any, from the DB */
int dbDelete(redisDb *db, robj *key) {
    /* Deleting an entry from the expires dict will not free the sds of
     * the key, because it is shared with the main dictionary. */
    if (dictSize(db->expires) > 0) dictDelete(db->expires,key->ptr);
    return dictDelete(db->dict,key->ptr) == DICT_OK;
}

/* Empty the whole database */
long long emptyDb(redisServer *server) {
    int j;
    long long removed = 0;

    int max_dbnum = server->db_num;
    for (j = 0; j < max_dbnum; j++) {
        removed += dictSize(server->db[j].dict);
        dictEmpty(server->db[j].dict);
        dictEmpty(server->db[j].expires);
    }
    return removed;
}

redisDb* selectDb(redisServer *server, int id) {
    if (id < 0 || id >= server->db_num)
        return NULL;
    return (&(server->db[id]));
}

//int flushRedisObjectLen(FILE* fp, int len) {
//
//}
//
//int flushRedisSds(FILE* fp, sds s) {
//    if (fp == NULL) {
//        return -1;
//    }
//    struct sdshdr* sh = s - (sizeof(struct sdshdr));
//    int size = sizeof(struct sdshdr) + sh->len;
//    if (fwrite((void*)&size, sizeof(int), 1, fp) == 0) return -1;
//    if (fwrite(sh, size, 1, fp) == 0) return -1;
//    return size;
//}
//
//int flushRedisString(FILE* fp, sds key, robj* val) {
//    if (fp == NULL) {
//        return -1;
//    }
//    flushRedisSds(fp, key);
//
//    if (fwrite((void*)&(val->type), 4, 1, fp) == 0) return -1;
//    if (fwrite((void*)&(val->encoding), 4, 1, fp) == 0) return -1;
//    if (fwrite((void*)&(val->lru), 24, 1, fp) == 0) return -1;
//    if (fwrite((void*)&(val->refcount), sizeof(val->refcount), 1, fp) == 0) return -1;
//    return flushRedisSds(fp, val->ptr);
//}
//
//int flushRedisList(FILE* fp, sds key, robj* val) {
//    if (fp == NULL) {
//        return -1;
//    }
//    flushRedisSds(fp, key);
//
//    if (fwrite((void*)&(val->type), 4, 1, fp) == 0) return -1;
//    if (fwrite((void*)&(val->encoding), 4, 1, fp) == 0) return -1;
//    if (fwrite((void*)&(val->lru), 24, 1, fp) == 0) return -1;
//    if (fwrite((void*)&(val->refcount), sizeof(val->refcount), 1, fp) == 0) return -1;
//
//    if (val->encoding == REDIS_ENCODING_ZIPLIST) {
//        unsigned char* p;
//        unsigned char* vstr;
//        unsigned int vlen;
//        long long vlong;
//
//        int objsize = ziplistLen(val->ptr);
//        if (fwrite((void*)&(objsize), 4, 1, fp) == 0) return -1;
//        p = ziplistIndex(o->ptr, 0);
//        while(ziplistGet(p,&vstr,&vlen,&vlong)) {
//            if (vstr) {
//                flushRedisSds(fp, vstr,
//            } else {
//
//            }
//        }
//    } else if (val->encoding == REDIS_ENCODING_LINKEDLIST) {
//
//    } else {
//        redisPanic("Unknown encoding");
//    }
//    return 0;
//}
//
//int flushRedisSet(FILE* fp, sds key, robj* val) {
//
//}
//
//int flushRedisZSet(FILE* fp, sds key, robj* val) {
//
//}
//
//int flushRedisHash(FILE* fp, sds key, robj* val) {
//
//}
//
//int flushDictEntry(int fd, dictEntry *de, int fd) {
//    sds key = dictGetEntryKey(de);
//    robj* val = dictGetEntryVal(de);
//    switch(val->type) {
//        case REDIS_STRING:flushRedisString(fd,key,val);break;
//        case REDIS_LIST:flushRedisList(fd,key,val);break;
//        case REDIS_SET:flushRedisSet(fd,key,val);break;
//        case REDIS_ZSET:flushRedisZSet(fd,key,val);break;
//        case REDIS_HASH:flushRedisHash(fd,key,val);break;
//        default://TODO show ertor type
//    }
//}
//
//int dumpDb(dictIterator *iter, int fd) {
//    dictEntry *de = NULL;
//    do {
//        de = dictNext(iter);
//        flushDictEntry(de, fd);
//    } while(de);
//}

redisClient* selectClient(redisServer *server, int id) {
    if (id < 0 || id >= server->db_num)
        return NULL;
    return server->clients[id];
}

/*-----------------------------------------------------------------------------
 * Type agnostic commands operating on the key space
 *----------------------------------------------------------------------------*/
void delCommand(redisClient *c) {
    c->returncode = REDIS_ERR;
    int deleted = 0, j;

    for (j = 1; j < c->argc; j++) {
        if (dbDelete(c->db,c->argv[j])) {
            c->db->dirty++;
            deleted++;
        }
    }
    if(deleted == 0) {
        c->returncode = REDIS_OK_NOT_EXIST;
        return;
    }

    c->retvalue.llnum = deleted;
    c->returncode = REDIS_OK;
}

void existsCommand(redisClient *c) {
    c->returncode = REDIS_ERR;
    expireIfNeeded(c->db,c->argv[1]);
    if (dbExists(c->db,c->argv[1])) {
        c->returncode = REDIS_OK;
    } else {
        c->returncode = REDIS_OK_NOT_EXIST;
    }
}

void typeCommand(redisClient *c) {
    robj *o = lookupKeyReadWithVersion(c->db,c->argv[1], &(c->version));
    if (o == NULL) {
        c->retvalue.llnum = REDIS_NONE;
        c->returncode = REDIS_OK_NOT_EXIST;
    } else {
        switch(o->type) {
        case REDIS_STRING:
            c->retvalue.llnum = REDIS_STRING;
            break;
        case REDIS_LIST:
            c->retvalue.llnum = REDIS_LIST;
            break;
        case REDIS_SET:
            c->retvalue.llnum = REDIS_SET;
            break;
        case REDIS_ZSET:
            c->retvalue.llnum = REDIS_ZSET;
            break;
        case REDIS_HASH:
            c->retvalue.llnum = REDIS_HASH;
            break;
        default:
            c->retvalue.llnum = REDIS_UNKNOWN;
            break;
        }
        c->returncode = REDIS_OK;
    }
}

void renameGenericCommand(redisClient *c, int nx) {
    robj *o;
    long long expire;
    uint16_t version;

    /* To use the same key as src and dst is probably an error */
    if (sdscmp(c->argv[1]->ptr,c->argv[2]->ptr) == 0) {
        c->returncode = REDIS_SAME_OBJECT_ERR;
        return;
    }

    if ((o = lookupKeyReadWithVersion(c->db,c->argv[1],&(c->version))) == NULL) {
        c->returncode = REDIS_OK_NOT_EXIST;
        return;
    }

    incrRefCount(o);
    expire = getExpire(c->db,c->argv[1]);
    if (lookupKeyWriteWithVersion(c->db,c->argv[2],&version) != NULL) {
        if (nx) {
            decrRefCount(o);
            c->returncode = REDIS_OK_BUT_ALREADY_EXIST;
            return;
        }
        /* Overwrite: delete the old key before creating the new one with the same name. */
        dbDelete(c->db,c->argv[2]);
    }

    sdsversion_change(c->argv[2]->ptr, c->version);
    if(c->version_care) {
        sdsversion_add(c->argv[2]->ptr, 1);
    }

    dbAdd(c->db,c->argv[2],o);
    if (expire != -1) setExpire(c->db,c->argv[2],expire);
    dbDelete(c->db,c->argv[1]);
    c->db->dirty++;
    c->returncode = REDIS_OK;
}

void renameCommand(redisClient *c) {
    renameGenericCommand(c,0);
}

void renamenxCommand(redisClient *c) {
    renameGenericCommand(c,1);
}

/*-----------------------------------------------------------------------------
 * Expires API
 *----------------------------------------------------------------------------*/

int removeExpire(redisDb *db, robj *key) {
    /* An expire may only be removed if there is a corresponding entry in the
     * main dict. Otherwise, the key will never be freed. */
    redisAssert(dictFind(db->dict,key->ptr) != NULL);
    return dictDelete(db->expires,key->ptr) == DICT_OK;
}

int removeXExpire(redisDb *db, robj *key) {
    if (dictFind(db->dict,key->ptr) == NULL) {
        return DICT_ERR;
    }
    return dictDelete(db->expires,key->ptr) == DICT_OK;
}

void setExpire(redisDb *db, robj *key, time_t when) {
    dictEntry *de;

    /* Reuse the sds from the main dict in the expire dict */
    de = dictFind(db->dict,key->ptr);
    redisAssert(de != NULL);
    dictReplace(db->expires,dictGetEntryKey(de),(void*)when);
}

void setXExpire(redisDb *db, robj *key, time_t when) {
    dictEntry *de;

    /* Reuse the sds from the main dict in the expire dict */
    de = dictFind(db->dict,key->ptr);
    if(de == NULL) return;
    dictReplace(db->expires,dictGetEntryKey(de),(void*)when);
}

/* Return the expire time of the specified key, or -1 if no expire
 * is associated with this key (i.e. the key is non volatile) */
time_t getExpire(redisDb *db, robj *key) {
    dictEntry *de;

    /* No expire? return ASAP */
    if (dictSize(db->expires) == 0 ||
       (de = dictFind(db->expires,key->ptr)) == NULL) return -1;

    /* The entry was found in the expire dict, this means it should also
     * be present in the main dict (safety check). */
    redisAssert(dictFind(db->dict,key->ptr) != NULL);
    return (time_t) dictGetEntryVal(de);
}

/* Return the logic clock of the specified key, or 0 if no key exist */
uint32_t getLogiClock(redisDb *db, robj *key) {
    uint32_t logiclock;
    dictEntry *de;
    de = dictFind(db->dict,key->ptr);
    if (de == NULL) {
        return 0;
    }
    sds skey = (sds) dictGetEntryKey(de);
    logiclock = sdslogiclock(skey);

    redisAssert(logiclock != 0);
    return logiclock;
}

int expireIfNeeded(redisDb *db, robj *key) {
    uint32_t logiclock = getLogiClock(db, key);
    if (logiclock == 0) {
        return 0;
    }
    if (db->logiclock > logiclock) {
        /* Delete the key */
        db->need_remove_key--;
        db->stat_expiredkeys++;
        return dbDelete(db,key);
    }

    time_t when = getExpire(db,key);

    if (when < 0) return 0; /* No expire for this key */

    /* Return when this key has not expired */
    if (time(NULL) <= when) return 0;

    /* Delete the key */
    db->stat_expiredkeys++;
    return dbDelete(db,key);
}

/*-----------------------------------------------------------------------------
 * Expires Commands
 *----------------------------------------------------------------------------*/

//tair's new expire generic Command
//something interesting with tair's expire protocol
//if seconds < 0 then
//we consider that you don't care with expire,will nothing to do
//else if seconds == 0 then
//we consider that you want remove the existing timeout on key(just as redis's persist)
//else if seconds > 0 && seconds <= now time then
//we consider that you give us duration, like 10s(just as redis's expire)
//else if seconds > now time then
//we conssider that you give us UNIX timestamp (seconds since January 1, 1970)(just as redis's expireat)
//so it's equal persist + expire + expireat
void expireXGenericCommand(redisClient *c, robj *key, robj *param) {
    dictEntry *de;
    long seconds;
    time_t when;

    if (getLongFromObject(param, &seconds) != REDIS_OK) {
        c->returncode = REDIS_ERR_IS_NOT_INTEGER;
        return;
    }

    de = dictFind(c->db->dict,key->ptr);
    if (de == NULL) {
        c->returncode = REDIS_OK_NOT_EXIST;
        return;
    }

    if (seconds > 0) {
        time_t now = time(NULL);
        if (seconds <= now) {
            when = now+seconds;
        } else {
            when = seconds;
        }
        setExpire(c->db,key,when);
        c->db->dirty++;
    } else if(seconds == 0 && removeExpire(c->db, c->argv[1])) {
        c->db->dirty++;
    }

    c->returncode = REDIS_OK;
    return;
}

void expireCommand(redisClient *c) {
    expireXGenericCommand(c,c->argv[1],c->argv[2]);
}

void ttlCommand(redisClient *c) {
    time_t expire, ttl = -1;

    expire = getExpire(c->db,c->argv[1]);
    if (expire != -1) {
        ttl = (expire-time(NULL));
        if (ttl < 0) ttl = -1;
    } else if (dbExists(c->db, c->argv[1]) == 0) {
        //mean not exist
        c->retvalue.llnum = (long long)ttl;
        if (c->retvalue.llnum == -1) {
            c->retvalue.llnum = 0;
        }
        c->returncode = REDIS_OK_NOT_EXIST;
        return;
    }

    c->retvalue.llnum = (long long)ttl;
    if (c->retvalue.llnum == -1) {
        c->retvalue.llnum = 0;
    }
    c->returncode = REDIS_OK;
}

void persistCommand(redisClient *c) {
    dictEntry *de;

    de = dictFind(c->db->dict,c->argv[1]->ptr);
    if (de == NULL) {
        c->returncode = REDIS_OK_NOT_EXIST;
    } else {
        if (removeExpire(c->db,c->argv[1])) {
            c->db->dirty++;
        }
        c->returncode = REDIS_OK;
    }
}

size_t dbSize(redisDb *db) {
    return dictSize(db->dict);
}
