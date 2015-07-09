/*
 * Copyright (c) 2009-2010, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis.h"

#ifdef HAVE_BACKTRACE
#include <execinfo.h>
#include <ucontext.h>
#endif /* HAVE_BACKTRACE */

#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <pthread.h>
#include <sys/resource.h>

/* Our shared "common" objects */

struct sharedObjectsStruct shared;

/* Global vars that are actally used as constants. The following double
 * values are used for double on-disk serialization, and are initialized
 * at runtime to avoid strange compiler optimizations. */

double R_Zero, R_PosInf, R_NegInf, R_Nan;

/*================================= Globals ================================= */

/* Global vars */
struct redisLogConfig logConfig;

/*============================ Utility functions ============================ */

void redisLog(int level, const char *fmt, ...) {
    const int syslogLevelMap[] = { LOG_DEBUG, LOG_INFO, LOG_NOTICE, LOG_WARNING };
    const char *c = ".-*#";
    time_t now = time(NULL);
    va_list ap;
    FILE *fp;
    char buf[64];
    char msg[REDIS_MAX_LOGMSG_LEN];

    if (level < logConfig.verbosity) return;

    fp = (logConfig.logfile == NULL) ? stdout : fopen(logConfig.logfile,"a");
    if (!fp) return;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    strftime(buf,sizeof(buf),"%d %b %H:%M:%S",localtime(&now));
    fprintf(fp,"[%d] %s %c %s\n",(int)getpid(),buf,c[level],msg);
    fflush(fp);

    if (logConfig.logfile) fclose(fp);

    if (logConfig.syslog_enabled) syslog(syslogLevelMap[level], "%s", msg);
}

/* Redis generally does not try to recover from out of memory conditions
 * when allocating objects or strings, it is not clear if it will be possible
 * to report this condition to the client since the networking layer itself
 * is based on heap allocation for send buffers, so we simply abort.
 * At least the code will be simpler to read... */
void oom(const char *msg) {
    redisLog(REDIS_WARNING, "%s: Out of memory\n",msg);
    sleep(1);
    abort();
}


void _redisAssert(char *estr, char *file, int line) {
    redisLog(REDIS_WARNING,"=== ASSERTION FAILED ===");
    redisLog(REDIS_WARNING,"==> %s:%d '%s' is not true",file,line,estr);
#ifdef HAVE_BACKTRACE
    redisLog(REDIS_WARNING,"(forcing SIGSEGV in order to print the stack trace)");
    *((char*)-1) = 'x';
#endif
}

void _redisPanic(char *msg, char *file, int line) {
    redisLog(REDIS_WARNING,"!!! Software Failure. Press left mouse button to continue");
    redisLog(REDIS_WARNING,"Guru Meditation: %s #%s:%d",msg,file,line);
#ifdef HAVE_BACKTRACE
    redisLog(REDIS_WARNING,"(forcing SIGSEGV in order to print the stack trace)");
    *((char*)-1) = 'x';
#endif
}

/*====================== Hash table type implementation  ==================== */

/* This is an hash table type that uses the SDS dynamic strings libary as
 * keys and radis objects as values (objects can hold SDS strings,
 * lists, sets). */

void dictVanillaFree(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    zfree(val);
}

void dictListDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    listRelease((list*)val);
}

int dictSdsKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

/* key1 will change key2 */
void dictRedisObjectModify(void *privdata, void *key1, void *key2)
{
    DICT_NOTUSED(privdata);
    robj *o1 = key1, *o2 = key2;
    unsigned int timestamp = sdslogiclock(o1->ptr);
    sdslogiclock_update(o2->ptr, timestamp);
}

/* A case insensitive version used for the command lookup table. */
int dictSdsKeyCaseCompare(void *privdata, const void *key1,
        const void *key2)
{
    DICT_NOTUSED(privdata);

    return strcasecmp(key1, key2) == 0;
}

void dictRedisObjectDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Values of swapped out keys as set to NULL */
    decrRefCount(val);
}

void dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

int dictObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    const robj *o1 = key1, *o2 = key2;
    return dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
}

unsigned int dictObjHash(const void *key) {
    const robj *o = key;
    return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

unsigned int dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

unsigned int dictSdsCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictEncObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    robj *o1 = (robj*) key1, *o2 = (robj*) key2;
    int cmp;

    if (o1->encoding == REDIS_ENCODING_INT &&
        o2->encoding == REDIS_ENCODING_INT)
            return o1->ptr == o2->ptr;

    o1 = getDecodedObject(o1);
    o2 = getDecodedObject(o2);
    cmp = dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
    decrRefCount(o1);
    decrRefCount(o2);
    return cmp;
}

unsigned int dictEncObjHash(const void *key) {
    robj *o = (robj*) key;

    if (o->encoding == REDIS_ENCODING_RAW) {
        return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
    } else {
        if (o->encoding == REDIS_ENCODING_INT) {
            char buf[32];
            int len;

            len = ll2string(buf,32,(long)o->ptr);
            return dictGenHashFunction((unsigned char*)buf, len);
        } else {
            unsigned int hash;

            o = getDecodedObject(o);
            hash = dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
            decrRefCount(o);
            return hash;
        }
    }
}

/* Sets type */
dictType setDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictRedisObjectDestructor, /* key destructor */
    NULL,                      /* val destructor */
    NULL,                      /* key modify */
    NULL                       /* val modify */
};

/* Sorted sets hash (note: a skiplist is used in addition to the hash table) */
dictType zsetDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictRedisObjectDestructor, /* key destructor */
    NULL,                      /* val destructor */
    NULL,                      /* key modify */
    NULL                       /* val modify */
};

/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType dbDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictRedisObjectDestructor,  /* val destructor */
    NULL,                       /* key modify */
    NULL                        /* val modify */
};

/* Db->expires */
dictType keyptrDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    NULL,                      /* val destructor */
    NULL,                      /* key modify */
    NULL                       /* val modify */
};

/* Command table. sds string -> command struct pointer. */
dictType commandTableDictType = {
    dictSdsCaseHash,           /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCaseCompare,     /* key compare */
    dictSdsDestructor,         /* key destructor */
    NULL,                      /* val destructor */
    NULL,                      /* key modify */
    NULL                       /* val modify */
};

/* Hash type hash table (note that small hashes are represented with zimpaps) */
dictType hashDictType = {
    dictEncObjHash,             /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictEncObjKeyCompare,       /* key compare */
    dictRedisObjectDestructor,  /* key destructor */
    dictRedisObjectDestructor,  /* val destructor */
    dictRedisObjectModify,      /* key modify */
    NULL                        /* val modify */
};

/* Keylist hash table type has unencoded redis objects as keys and
 * lists as values. It's used for blocking operations (BLPOP) and to
 * map swapped keys to a list of clients waiting for this keys to be loaded. */
dictType keylistDictType = {
    dictObjHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictObjKeyCompare,          /* key compare */
    dictRedisObjectDestructor,  /* key destructor */
    dictListDestructor,         /* val destructor */
    NULL,                       /* key modify */
    NULL                        /* val modify */
};

int htNeedsResize(dict *dict) {
    long long size, used;

    size = dictSlots(dict);
    used = dictSize(dict);
    return (size && used && size > DICT_HT_INITIAL_SIZE &&
            (used*100/size < REDIS_HT_MINFILL));
}

/* If the percentage of used slots in the HT reaches REDIS_HT_MINFILL
 * we resize the hash table to save memory */
void tryResizeHashTables(redisServer *server) {
    int j;

    int dbnum = get_malloc_dbnum();
    int max_dbnum = server->db_num;
    for (j = 0; j < max_dbnum; j++) {
        set_malloc_dbnum(j);
        redisDb *db = server->db + j;
        if (htNeedsResize(db->dict))
            dictResize(db->dict);
        if (htNeedsResize(db->expires))
            dictResize(db->expires);
    }
    set_malloc_dbnum(dbnum);
}

void tryResizeHashTableByDb(redisServer *server, int index) {
    int dbnum = get_malloc_dbnum();
    set_malloc_dbnum(index);
    redisDb *db = server->db + index;
    if (htNeedsResize(db->dict))
        dictResize(db->dict);
    if (htNeedsResize(db->expires))
        dictResize(db->expires);
    set_malloc_dbnum(dbnum);
}

/* Our hash table implementation performs rehashing incrementally while
 * we write/read from the hash table. Still if the server is idle, the hash
 * table will use two tables for a long time. So we try to use 1 millisecond
 * of CPU time at every serverCron() loop in order to rehash some key. */
void incrementallyRehash(redisServer *server) {
    int j;

    int dbnum = get_malloc_dbnum();
    int max_dbnum = server->db_num;
    for (j = 0; j < max_dbnum; j++) {
        set_malloc_dbnum(j);
        if (dictIsRehashing(server->db[j].dict)) {
            dictRehashMilliseconds(server->db[j].dict,1);
            break; /* already used our millisecond for this loop... */
        }
    }
    set_malloc_dbnum(dbnum);
}

void incrementallyRehashByDb(redisServer *server, int index) {
    int dbnum = get_malloc_dbnum();
    set_malloc_dbnum(index);
    if (dictIsRehashing(server->db[index].dict)) {
        dictRehashMilliseconds(server->db[index].dict,1);
        /* already used our millisecond for this loop... */
    }
    set_malloc_dbnum(dbnum);
}

/* This function is called once a background process of some kind terminates,
 * as we want to avoid resizing the hash tables when there is a child in order
 * to play well with copy-on-write (otherwise when a resize happens lots of
 * memory pages are copied). The goal of this function is to update the ability
 * for dict.c to resize the hash tables accordingly to the fact we have o not
 * running childs. */
void updateDictResizePolicy(redisServer *server) {
    REDIS_NOTUSED(server);
    dictEnableResize();
}


void updateLRUClock() {
    shared.lruclock = (time(NULL)/REDIS_LRU_CLOCK_RESOLUTION) &
                                                REDIS_LRU_CLOCK_MAX;
}

void activeExpireCycleByDb(struct redisServer *server, int index) {
    uint32_t logiclock;
    long num;
    long loop_cnt;
    dictEntry *de;

    int dbnum = get_malloc_dbnum();

    set_malloc_dbnum(index);
    int expired;
    redisDb *db = server->db+index;

    loop_cnt = 0;
    do {
        num = db->need_remove_key;

        expired = 0;
        if (num > REDIS_EXPIRELOOKUPS_PER_CRON)
            num = REDIS_EXPIRELOOKUPS_PER_CRON;
        while (num--) {
            loop_cnt++;
            if ((de = dictGetRandomKey(db->dict)) == NULL) break;
            sds key = dictGetEntryKey(de);
            logiclock = sdslogiclock(key);
            if (db->logiclock > logiclock) {
                robj *keyobj = createStringObject(key,sdslen(key),sdslogiclock(key),sdsversion(key));
                dbDelete(db,keyobj);
                decrRefCount(keyobj);
                expired++;
                db->need_remove_key--;
                db->stat_expiredkeys++;
            }
        }
    } while ((loop_cnt < REDIS_CRON_LOOP_LIMIT) && (expired > REDIS_EXPIRELOOKUPS_PER_CRON / 4));

    /* Continue to expire if at the end of the cycle more than 25%
     * of the keys were expired. */
    loop_cnt = 0;
    do {
        num = dictSize(db->expires);
        time_t now = time(NULL);

        expired = 0;
        if (num > REDIS_EXPIRELOOKUPS_PER_CRON)
            num = REDIS_EXPIRELOOKUPS_PER_CRON;
        while (num--) {
            loop_cnt++;
            time_t t;

            if ((de = dictGetRandomKey(db->expires)) == NULL) break;
            t = (time_t) dictGetEntryVal(de);
            if (now > t) {
                sds key = dictGetEntryKey(de);
                logiclock = sdslogiclock(key);
                if (db->logiclock > logiclock) {
                    db->need_remove_key--;
                }
                robj *keyobj = createStringObject(key,sdslen(key),sdslogiclock(key),sdsversion(key));
                dbDelete(db,keyobj);
                decrRefCount(keyobj);
                expired++;
                db->stat_expiredkeys++;
            }
        }
    } while ((loop_cnt < REDIS_CRON_LOOP_LIMIT) && (expired > REDIS_EXPIRELOOKUPS_PER_CRON / 4));

    set_malloc_dbnum(dbnum);
}

void activeExpireCycle(struct redisServer *server) {
    int j;
    uint32_t logiclock;
    long num;
    dictEntry *de;

    int dbnum = get_malloc_dbnum();
    int max_dbnum = server->db_num;
    for (j = 0; j < max_dbnum; j++) {
        set_malloc_dbnum(j);
        int expired;
        redisDb *db = server->db+j;

        do {
            num = db->need_remove_key;

            expired = 0;
            if (num > REDIS_EXPIRELOOKUPS_PER_CRON)
                num = REDIS_EXPIRELOOKUPS_PER_CRON;
            while (num--) {
                if ((de = dictGetRandomKey(db->dict)) == NULL) break;
                sds key = dictGetEntryKey(de);
                logiclock = sdslogiclock(key);
                if (db->logiclock > logiclock) {
                    robj *keyobj = createStringObject(key,sdslen(key),sdslogiclock(key),sdsversion(key));
                    dbDelete(db,keyobj);
                    decrRefCount(keyobj);
                    expired++;
                    db->need_remove_key--;
                    db->stat_expiredkeys++;
                }
            }
        } while (expired > REDIS_EXPIRELOOKUPS_PER_CRON/4);

        /* Continue to expire if at the end of the cycle more than 25%
         * of the keys were expired. */
        do {
            num = dictSize(db->expires);
            time_t now = time(NULL);

            expired = 0;
            if (num > REDIS_EXPIRELOOKUPS_PER_CRON)
                num = REDIS_EXPIRELOOKUPS_PER_CRON;
            while (num--) {
                time_t t;

                if ((de = dictGetRandomKey(db->expires)) == NULL) break;
                t = (time_t) dictGetEntryVal(de);
                if (now > t) {
                    sds key = dictGetEntryKey(de);
                    logiclock = sdslogiclock(key);
                    if (db->logiclock > logiclock) {
                        db->need_remove_key--;
                    }
                    robj *keyobj = createStringObject(key,sdslen(key),sdslogiclock(key),sdsversion(key));
                    dbDelete(db,keyobj);
                    decrRefCount(keyobj);
                    expired++;
                    db->stat_expiredkeys++;
                }
            }
        } while (expired > REDIS_EXPIRELOOKUPS_PER_CRON/4);
    }
    set_malloc_dbnum(dbnum);
}

/* =========================== Server initialization ======================== */

int serverCronByDb(struct redisServer *server, int index) {
    int loops = server->cronloops;

    long long size = dictSlots(server->db[index].dict);
    long long used = dictSize(server->db[index].dict);
    long long vkeys = dictSize(server->db[index].expires);
    if (!(loops % 50) && (used || vkeys)) {
        redisLog(REDIS_VERBOSE,"DB %d: %lld keys (%lld volatile) in %lld slots HT.",index,used,vkeys,size);
    }

    if (!(loops % 10)) tryResizeHashTableByDb(server,index);
    if (server->activerehashing) incrementallyRehashByDb(server,index);

    /* Show information about connected clients */
    if (!(loops % 50)) {
        //redisLog(REDIS_VERBOSE,"%zu bytes in use", zmalloc_used_memory());
    }

    /* Expire a few keys per cycle, only if this is a master.
     * On slaves we wait for DEL operations synthesized by the master
     * in order to guarantee a strict consistency. */
    profiler_begin("active expire");
    activeExpireCycleByDb(server,index);
    profiler_end();
    return 10;
}

/* =========================== Server initialization ======================== */

void createSharedObjects() {
    updateLRUClock();
}

void freeSharedObjects() {
}

void initDbLock(redisServer* server, int max_dbnum) {
    int i;
    server->db_mutexs = zmalloc(sizeof(pthread_mutex_t)*max_dbnum);
    for(i = 0; i < max_dbnum; i++) {
        pthread_mutex_init(&(server->db_mutexs[i]), NULL);
    }
}

void destoryDbLock(redisServer* server) {
    int i;
    int mutex_num = server->db_num;
    for(i = 0; i < mutex_num; i++) {
        pthread_mutex_destroy(&(server->db_mutexs[i]));
    }
    zfree(server->db_mutexs);
    server->db_mutexs = NULL;
}

void initServer(redisServer *server, const redisConfig *config) {
    /* set rdb's log level */
    logConfig.verbosity = config->verbosity;

    /* set zipmap, ziplist, intset's max value and max entires */
    server->hash_max_zipmap_entries  = REDIS_HASH_MAX_ZIPMAP_ENTRIES;
    server->hash_max_zipmap_value    = REDIS_HASH_MAX_ZIPMAP_VALUE;
    server->list_max_ziplist_entries = REDIS_LIST_MAX_ZIPLIST_ENTRIES;
    server->list_max_ziplist_value   = REDIS_LIST_MAX_ZIPLIST_VALUE;
    server->set_max_intset_entries   = REDIS_SET_MAX_INTSET_ENTRIES;

    /* set list,hash,zset,set's max size */
    server->list_max_size = config->list_max_size;
    server->hash_max_size = config->hash_max_size;
    server->zset_max_size = config->zset_max_size;
    server->set_max_size  = config->set_max_size;

    /* set unit num */
    if (config->unit_num > 0) {
        server->unit_num = config->unit_num;
    } else {
        server->unit_num = MAX_UNIT_NUM;
    }

    /* set area group num */
    if (config->area_group_num > 0) {
        server->area_group_num = config->area_group_num;
    } else {
        server->area_group_num = MAX_AREA_GROUP_NUM;
    }

    /* set db num */
    server->db_num = server->unit_num * server->area_group_num;
    if (server->db_num > MAX_DBNUM) {
        /* limit max db num */
        server->db_num = MAX_DBNUM;
        server->unit_num = MAX_UNIT_NUM;
        server->area_group_num = MAX_AREA_GROUP_NUM;
    }

    /* set rdb's max memory */
    if (config->maxmemory != NULL) {
        server->maxmemory = memtoll(config->maxmemory,NULL);
    } else {
        server->maxmemory = memtoll("10gb",NULL);
    }

    /* get default db maxmemory */
    unsigned long long db_maxmemory;
    if (config->db_maxmemory != NULL) {
        db_maxmemory = memtoll(config->db_maxmemory,NULL);
    } else {
        db_maxmemory = server->maxmemory;
    }

    /* set rdb's policy, when reach max memory */
    if (config->maxmemory_policy >= 0) {
        server->maxmemory_policy = config->maxmemory_policy;
    } else {
        server->maxmemory_policy = REDIS_MAXMEMORY_ALLKEYS_LRU;
    }

    /* set rdb's samples */
    if (config->maxmemory_samples > 0) {
        server->maxmemory_samples = config->maxmemory_samples;
    } else {
        server->maxmemory_samples = 3;
    }

    /* active the rehashing mode */
    server->activerehashing   = 1;

    /* Double constants initialization */
    R_Zero   = 0.0;
    R_PosInf = 1.0/R_Zero;
    R_NegInf = -1.0/R_Zero;
    R_Nan    = R_Zero/R_Zero;

    int j = 0;
    int range_side = 0;
    int max_dbnum = server->db_num;
    server->db = zmalloc(sizeof(redisDb)*max_dbnum);
    server->clients = zmalloc(sizeof(redisClient*)*max_dbnum);
    initDbLock(server, max_dbnum);

    for (j = 0; j < max_dbnum; j++) {
        range_side = j / server->unit_num * server->unit_num;
        memset(&(server->db[j]), 0, sizeof(redisDb));
        server->db[j].dict = dictCreate(&dbDictType,NULL);
        server->db[j].expires = dictCreate(&keyptrDictType,NULL);
        server->db[j].id = j;
        server->db[j].area_group_start = range_side;
        server->db[j].area_group_end = range_side + server->unit_num;
        server->db[j].status = REDIS_DB_STATUS_WORKING;
        pthread_mutex_init(&(server->db[j].lock), NULL);
        server->db[j].maxmemory = db_maxmemory;
        server->db[j].maxmemory_samples = server->maxmemory_samples;

        server->db[j].stat_evictedkeys = 0;
        server->db[j].stat_expiredkeys = 0;
        server->db[j].write_count = 0;
        server->db[j].read_count = 0;
        server->db[j].hit_count = 0;
        server->db[j].remove_count = 0;

        server->db[j].logiclock = 1;
        server->db[j].need_remove_key = 0;
        server->db[j].filter_list.head = NULL;
        server->db[j].filter_list.size = 0;

        server->db[j].dirty = 0;
        server->db[j].loading = 0;
        server->db[j].lastsave = 0;
        server->db[j].loading_total_bytes = 0;
        server->db[j].loading_loaded_bytes = 0;
        server->db[j].loading_start_time = 0;

        server->clients[j] = createClient(server, j);
    }
    server->rdb_compression = 1;
    server->cronloops = 0;
    server->stat_numcommands = 0;
    server->stat_numconnections = 0;
    server->stat_starttime = time(NULL);
    srand(time(NULL)^getpid());
}

void unInitServer(redisServer* server) {
	int j = 0;
    int max_dbnum = server->db_num;
	for(j = 0; j < max_dbnum; j++) {
		dictRelease(server->db[j].dict);
		dictRelease(server->db[j].expires);
        pthread_mutex_destroy(&(server->db[j].lock));
        freeClient(server->clients[j]);
	}

	zfree(server->db);
    zfree(server->clients);
}

/* Call() is the core of Redis execution of a command */
void call(redisClient *c) {
    /* later we will do some statistics about command used time, dirty, and so on,
     * it should take more better way for less gettimeofday syscall */
    c->cmd->proc(c);
    c->server->stat_numcommands++;
}

/* If this function gets called we already read a whole
 * command, argments are in the client argv/argc fields.
 * processCommand() execute the command or prepare the
 * server for a bulk read from the client.
 *
 * If 1 is returned the client is still alive and valid and
 * and other operations can be performed by the caller. Otherwise
 * if 0 is returned the client was destroied (i.e. after QUIT). */
int processCommand(redisClient *c) {
    /* Handle the maxmemory directive.
     *
     * First we try to free some memory if possible (if there are volatile
     * keys in the dataset). If there are not the only thing we can do
     * is returning an error. */
    profiler_begin("free db memory");
    if (c->db->maxmemory) {
        freeDBMemoryIfNeeded(c->db);
    }
    profiler_end();
    if (c->db->maxmemory && zmalloc_db_used_memory(c->db->id) > c->db->maxmemory) {
        return REDIS_ERR_REACH_MAXMEMORY;
    }

    profiler_begin("free memory from other area");
    while(c->server->maxmemory && zmalloc_used_memory() > c->server->maxmemory) {
        //TODO if alloc all area's maxmemory are large than server's maxmemory, then
        //it will lead to used_memory large than server's maxmemory, it's so trouble.
        //don't alloc more memory than server maxmemory, if you do that, we will enter
        //this bench to release memory, it's a bad way. it's slower than others. so avoid
        //alloc areas' total memory than server can provided.Please avoid entering to this
        //loop.
        freeMemoryIfNeeded(c->server, c->db->id);
    }
    profiler_end();

    profiler_begin("call method");
    call(c);
    profiler_end();

    return REDIS_OK;
}

/*================================== Commands =============================== */

/* Convert an amount of bytes into a human readable string in the form
 * of 100B, 2G, 100M, 4K, and so forth. */
void bytesToHuman(char *s, unsigned long long n) {
    double d;

    if (n < 1024) {
        /* Bytes */
        sprintf(s,"%lluB",n);
        return;
    } else if (n < (1024*1024)) {
        d = (double)n/(1024);
        sprintf(s,"%.2fK",d);
    } else if (n < (1024LL*1024*1024)) {
        d = (double)n/(1024*1024);
        sprintf(s,"%.2fM",d);
    } else if (n < (1024LL*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024);
        sprintf(s,"%.2fG",d);
    }
}

/* ============================ Maxmemory directive  ======================== */

int setDBMaxmemory(redisServer *server, int id, uint64_t maxmem) {
    if (id < 0 || id >= server->db_num)
        return REDIS_ERR;
    if (server->db[id].maxmemory != maxmem) {
        pthread_mutex_lock(&(server->db[id].lock));
        server->db[id].maxmemory = maxmem;
        pthread_mutex_unlock(&(server->db[id].lock));
    }
    return REDIS_OK;
}

void lock_db_memory_from_to(redisDb *db, int _from, int _to) {
    if (_from < _to) {
        pthread_mutex_lock(&((db + _from - db->id)->lock));
        pthread_mutex_lock(&((db + _to - db->id)->lock));
    } else {
        pthread_mutex_lock(&((db + _to - db->id)->lock));
        pthread_mutex_lock(&((db + _from - db->id)->lock));
    }
}

void unlock_db_memory_from_to(redisDb *db, int _from, int _to) {
    if (_from < _to) {
        pthread_mutex_unlock(&((db + _to - db->id)->lock));
        pthread_mutex_unlock(&((db + _from - db->id)->lock));
    } else {
        pthread_mutex_unlock(&((db + _from - db->id)->lock));
        pthread_mutex_unlock(&((db + _to - db->id)->lock));
    }
}

void incr_max_memory_unsafe(redisDb *db, unsigned long long _n) {
    db->maxmemory += _n;
}

void decr_max_memory_unsafe(redisDb *db, unsigned long long _n) {
    db->maxmemory -= _n;
}

int freeDBMemory(redisDb *db, int expires_db) {
    uint32_t logiclock;
    dict *dict = NULL;
    if (expires_db)
        dict = db->expires;
    else
        dict = db->dict;

    if (dictSize(dict) == 0)
        return 0;

    sds bestkey = NULL;
    struct dictEntry *de = NULL;
    long bestval = 0;
    for (int k = 0; k < db->maxmemory_samples; k++) {
        sds thiskey;
        long thisval;
        robj *o;

        de = dictGetRandomKey(dict);
        thiskey = dictGetEntryKey(de);

        logiclock = sdslogiclock(thiskey);
        if (db->logiclock > logiclock) {
            bestkey = thiskey;
            break;
        }

        /* When policy is volatile-lru we need an additonal lookup
         * to locate the real key, as dict is set to db->expires. */
        if (expires_db)
            de = dictFind(db->dict, thiskey);
        o = dictGetEntryVal(de);
        thisval = estimateObjectIdleTime(o);

        /* Higher idle time is better candidate for deletion */
        if (bestkey == NULL || thisval > bestval) {
            bestkey = thiskey;
            bestval = thisval;
        }
    }
    /* Finally remove the selected key. */
    if (bestkey) {
        logiclock = sdslogiclock(bestkey);
        if (db->logiclock > logiclock) {
            db->need_remove_key--;
        }
        robj *keyobj = createStringObject(bestkey,sdslen(bestkey),sdslogiclock(bestkey),sdsversion(bestkey));
        dbDelete(db,keyobj);
        db->stat_evictedkeys++;
        decrRefCount(keyobj);
        return 1;
    }
    return 0;
}

int shiftSpace(struct redisDb *db) {
    int i, index, start, end, no, need_unlock_now_lock, force_shift_space;
    unsigned long long tmp_used_memory, tmp_maxmemory, size;
    unsigned long long tmp_used_memory2, delta, tmp, less_need_memory;
    unsigned long long delta1, delta2;
    start = db->area_group_start;
    end = db->area_group_end;
    index = -1;
    delta = 0;
    no = 0;
    force_shift_space = 0;
    for(i = start; i < end; i++) {
        /* notes: here maybe used_memory large than maxmemory, because of the concurrenty access,
         * and used_memory,maxmemory are unsigned long long, so we must do below, this doesn't do lock */
        tmp_used_memory = zmalloc_db_used_memory(i);
        tmp_maxmemory = (db + (i - db->id))->maxmemory;
        if (tmp_used_memory < tmp_maxmemory) {
            tmp = tmp_maxmemory - tmp_used_memory;
            if (delta < tmp) {
                delta = tmp;
                index = i;
            }
        }
    }

    if (index == -1) {
        /* all units in area are full
         * when the unit's maxmemory is larger than 1k(I consider that every
         * unit must larger than 1k, otherwise this unit is meanless)*/
        if (db->maxmemory < REDIS_DB_LESS_MEMORY) {
            /* need to free others' memory */
            force_shift_space = 1;
        } else {
            /* need to free itself's memory */
            return 0;
        }
    } else if (index == db->id) {
        /* maybe we should check again, enter this bench, I don't know why,
         * so maybe we need to do some log, but returning zero is unsually safe.
         * */
        return 0;
    } else {
        /* find a better unit to do shift space */
        lock_db_memory_from_to(db, db->id, index);

        tmp_used_memory = zmalloc_db_used_memory(index);
        tmp_maxmemory = (db + (index - db->id))->maxmemory;
        less_need_memory = 0;
        tmp_used_memory2 = zmalloc_db_used_memory(db->id);
        if (tmp_used_memory2 >= db->maxmemory) {
            less_need_memory = tmp_used_memory2 - db->maxmemory;
        }
        if (tmp_used_memory < tmp_maxmemory) {
            no = 0;
            delta1 = tmp_maxmemory - tmp_used_memory;
            delta2 = ((delta + 1) >> 1);
            if (less_need_memory < delta2) {
                size = delta2;
            } else if (less_need_memory < delta1) {
                size = less_need_memory;
            } else {
                /* yes, as you see if can't get enough memory from one unit,we give him all this unit's free memory,
                 * this is a lazy and imprecise way, at next time enter into this unit,
                 *  it will shift new space from others, and it only need less price than lock all units in area */
                size = delta1;
            }
            decr_max_memory_unsafe(db + index - db->id, size);
            incr_max_memory_unsafe(db, size);

            unlock_db_memory_from_to(db, db->id, index);
            return 1;
        } else {
            //Oh NO, memory size is changed
            no = 1;
            unlock_db_memory_from_to(db, db->id, index);
        }
    }

    if (no == 1) {
        /* lock each two units of area from low to high
         * you see, here is interesting, we from the low to high,
         * if reach himself,we lock it, and never realese it until complete all logic,
         * if index == -1, find no unit not full, we unlock his lock
         * if index != -1 and index == himself, aha his memory is not full now
         * if index != -1 and index == others, in this bench must has two lock, himself and other unit which
         * will shift space */
        index = -1;
        delta = 0;
        for(i = start; i < end; i++) {
            need_unlock_now_lock = 1;
            pthread_mutex_lock(&((db + (i - db->id))->lock));
            tmp_used_memory = zmalloc_db_used_memory(i);
            tmp_maxmemory = (db + (i - db->id))->maxmemory;
            if (tmp_used_memory < tmp_maxmemory) {
                tmp = tmp_maxmemory - tmp_used_memory;
                if (delta < tmp) {
                    if (index != -1 && index != db->id) {
                        pthread_mutex_unlock(&((db + (index - db->id))->lock));
                    }
                    delta = tmp;
                    index = i;
                    need_unlock_now_lock = 0;
                }
            }
            if (i != db->id && need_unlock_now_lock == 1) {
                pthread_mutex_unlock(&((db + (i - db->id))->lock));
            }
        }

        if (index != -1) {
            if (index == db->id) {
                pthread_mutex_unlock(&((db + (index - db->id))->lock));
                //need check, so return zero
                return 0;
            } else {
                delta1 = tmp_maxmemory - tmp_used_memory;
                delta2 = ((delta + 1) >> 1);
                if (less_need_memory < delta2) {
                    size = delta2;
                } else if (less_need_memory < delta1) {
                    size = less_need_memory;
                } else {
                    size = delta1;
                }
                decr_max_memory_unsafe(db + index - db->id, size);
                incr_max_memory_unsafe(db, size);

                pthread_mutex_unlock(&(db->lock));
                pthread_mutex_unlock(&((db + (index - db->id))->lock));
                return 1;
            }
        } else {
            pthread_mutex_unlock(&(db->lock));
            if (db->maxmemory < REDIS_DB_LESS_MEMORY) {
                force_shift_space = 1;
            } else {
                return 0;
            }
        }
    }

    if (force_shift_space == 1) {
        index = -1;
        delta = 0;
        for(i = db->area_group_start; i < end; i++) {
            tmp_maxmemory = (db + (i - db->id))->maxmemory;
            if (delta < tmp_maxmemory) {
                delta = tmp_maxmemory;
                index = i;
            }
        }

        lock_db_memory_from_to(db, db->id, index);

        decr_max_memory_unsafe(db + index - db->id, (2 * REDIS_DB_LESS_MEMORY));
        incr_max_memory_unsafe(db, (2 * REDIS_DB_LESS_MEMORY));

        unlock_db_memory_from_to(db, db->id, index);
    }

    return 0;
}

void freeDBMemoryIfNeeded(struct redisDb *db) {
    /* we all first use "space shift"(the name which is I give the way for get memory for others),
     * in same area group, they can get memory from other members of the same area group, it may like
     * the thread in process, but a litte difference, thread has its own thread stack, but the unit of area
     * group not.the units of area group at the begining has have same memory size, for example, the area size
     * is 100M, we have 4 units in the area,then each unit in the area has 25M memory, but with time going, may
     * be the hash function don't make each unit of area has the same memory used,when one unit of area occurs
     * used memory large than it has, then we can do space shift, but it is not accurate, for accurate memory, will
     * need much more CPU resource, it's not we want to see. so the space shift is lazy and probable.
     * but it is efficient */

    /* now just support lru, first volatile-lru, second allkeys-lru*/
    int freed;
    while (db->maxmemory && zmalloc_db_used_memory(db->id) > db->maxmemory) {
        freed = 0;
        if (shiftSpace(db)) continue;

        if (freeDBMemory(db, 1))
            freed++;
        else if (freeDBMemory(db, 0))
            freed++;

        if (!freed) return; /* nothing to free... */
    }
}

void freeMemoryIfNeeded(struct redisServer *server, int trigger) {
    int i, j, k;
    uint32_t logiclock;
    sds thiskey;

    int old_dbnum = get_malloc_dbnum();
    /* if there are some key out of time, then we should try to lock,
     * if can't, then scan next db */
    for (i = 0; i < server->db_num; i++) {
        set_malloc_dbnum(i);
        redisDb *db = server->db+i;
        if (db->need_remove_key > 0) {
            if (trigger != i) {
                if(!pthread_mutex_trylock(&(server->db_mutexs[i]))) {
                    struct dictEntry *de;
                    de = dictGetRandomKey(db->dict);
                    if (de == NULL) {
                        pthread_mutex_unlock(&(server->db_mutexs[i]));
                        continue;
                    }
                    thiskey = dictGetEntryKey(de);

                    logiclock = sdslogiclock(thiskey);
                    if (db->logiclock > logiclock) {
                        robj *keyobj = createStringObject(thiskey,sdslen(thiskey),
                                sdslogiclock(thiskey),sdsversion(thiskey));
                        dbDelete(db,keyobj);
                        db->stat_evictedkeys++;
                        db->need_remove_key--;
                        decrRefCount(keyobj);
                    }
                    pthread_mutex_unlock(&(server->db_mutexs[i]));
                }
            }
        }
    }
    set_malloc_dbnum(old_dbnum);


    /* Remove keys accordingly to the active policy as long as we are
     * over the memory limit. */
    if (server->maxmemory_policy == REDIS_MAXMEMORY_NO_EVICTION) return;


    old_dbnum = get_malloc_dbnum();
    for (j = 0; j < server->db_num; j++) {
        long bestval = 0; /* just to prevent warning */
        sds bestkey = NULL;
        struct dictEntry *de;
        redisDb *db = server->db+j;
        dict *dict;

        set_malloc_dbnum(j);

        if (trigger == j) continue;
        if (!pthread_mutex_trylock(&(server->db_mutexs[j]))) {
            if (server->maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_LRU ||
                    server->maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_RANDOM)
            {
                dict = server->db[j].dict;
            } else {
                dict = server->db[j].expires;
            }
            if (dictSize(dict) == 0) {
                pthread_mutex_unlock(&(server->db_mutexs[j]));
                continue;
            }

            /* volatile-random and allkeys-random policy */
            if (server->maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_RANDOM ||
                    server->maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_RANDOM)
            {
                de = dictGetRandomKey(dict);
                bestkey = dictGetEntryKey(de);
            }

            /* volatile-lru and allkeys-lru policy */
            else if (server->maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_LRU ||
                    server->maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_LRU)
            {
                for (k = 0; k < server->maxmemory_samples; k++) {
                    sds thiskey;
                    long thisval;
                    robj *o;

                    de = dictGetRandomKey(dict);
                    thiskey = dictGetEntryKey(de);
                    /* When policy is volatile-lru we need an additonal lookup
                     * to locate the real key, as dict is set to db->expires. */
                    if (server->maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_LRU)
                        de = dictFind(db->dict, thiskey);
                    o = dictGetEntryVal(de);
                    thisval = estimateObjectIdleTime(o);

                    /* Higher idle time is better candidate for deletion */
                    if (bestkey == NULL || thisval > bestval) {
                        bestkey = thiskey;
                        bestval = thisval;
                    }
                }
            }

            /* volatile-ttl */
            else if (server->maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_TTL) {
                for (k = 0; k < server->maxmemory_samples; k++) {
                    sds thiskey;
                    long thisval;

                    de = dictGetRandomKey(dict);
                    thiskey = dictGetEntryKey(de);
                    thisval = (long) dictGetEntryVal(de);

                    /* Expire sooner (minor expire unix timestamp) is better
                     * candidate for deletion */
                    if (bestkey == NULL || thisval < bestval) {
                        bestkey = thiskey;
                        bestval = thisval;
                    }
                }
            }

            /* Finally remove the selected key. */
            if (bestkey) {
                logiclock = sdslogiclock(bestkey);
                if (db->logiclock > logiclock) {
                    db->need_remove_key--;
                }
                robj *keyobj = createStringObject(bestkey,sdslen(bestkey),
                        sdslogiclock(bestkey),sdsversion(bestkey));
                dbDelete(db,keyobj);
                db->stat_evictedkeys++;
                decrRefCount(keyobj);
            }
            pthread_mutex_unlock(&(server->db_mutexs[j]));
        }
    }
    set_malloc_dbnum(old_dbnum);
}

struct dumpThreadInfo* create_dump_thread_info(redisServer *server, char *filename,
        int filenamelength, int area) {
    struct dumpThreadInfo* info = zmalloc(sizeof(struct dumpThreadInfo));
    if (info == NULL) {
        return NULL;
    }

    info->filename = zmalloc(sizeof(filenamelength + 1));
    memcpy(info->filename, filename, filenamelength);
    info->filename[filenamelength] = '\0';
    info->area = area;
    info->server = server;

    return info;
}

void free_dump_thread_info(struct dumpThreadInfo* info) {
    if (info) {
        if (info->filename) {
            zfree(info->filename);
        }
        zfree(info);
    }
}

void* dump_db_thread(void* argv) {
    pthread_detach(pthread_self());
    dumpThreadInfo* info = (dumpThreadInfo *)argv;
    //rdbSave(info->server, info->filename, info->area);
    free_dump_thread_info(info);
    return NULL;
}

void* load_db_thread(void* argv) {
    pthread_detach(pthread_self());
    loadThreadInfo* info = (loadThreadInfo *)argv;
    //rdbLoad(info->server, info->filename, info->area);
    free_load_thread_info(info);
    return NULL;
}

/* The End */
