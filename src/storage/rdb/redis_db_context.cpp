#include "redis_db_context.h"
#include "redis_server_cron.h"
#include "scope_lock.h"
#include "common/log.hpp"

using namespace tbsys;

USE_NS

bool redis_db_context::inited = false;

/**
 * double check
 */
void redis_db_context::initialize_shared_data()
{
    // check onec
    if (inited) return;
    createSharedObjects();
    zmalloc_enable_thread_safeness();
    inited = true;
    assert(zmalloc_thread_safeness() == 1);
}

redis_db_context::redis_db_context(const redisConfig &config) : server_cron(NULL), server(NULL)
{
    // call static init function
    redis_db_context::initialize_shared_data();
    server         = create_context(config);
    unit_num       = server->unit_num;
    area_group_num = server->area_group_num;
}

redis_db_context::~redis_db_context()
{
    destroy_context(server);
    server = NULL;
    freeSharedObjects();
}

redisServer *redis_db_context::create_context(const redisConfig &config)
{
    redisServer* context = new redisServer;
    if (context == NULL) {
        return NULL;
    }
    initServer(context, &config);

    return context;
}

void redis_db_context::destroy_context(redisServer *context)
{
    int mutex_num = context->db_num;
    for(int i = 0; i < mutex_num; i++) {
        pthread_mutex_lock(&(context->db_mutexs[i]));
    }
    unInitServer(context);
    for(int i = 0; i < mutex_num; i++) {
        pthread_mutex_unlock(&(context->db_mutexs[i]));
    }

    destoryDbLock(context);
	delete context;
}


void redis_db_context::stop() {
    if (server_cron == NULL)
        return;
    server_cron->stop();
    server_cron->wait();
    delete server_cron;
    server_cron = NULL;
}

void redis_db_context::start() {
    if (server_cron != NULL)
        return;
    server_cron = new redis_server_cron(*this);
    server_cron->start();
}

uint64_t redis_db_context::get_db_used_maxmemory(uint32_t db)
{
    return zmalloc_db_used_memory(db);
}

void redis_db_context::set_db_maxmemory(int db, uint64_t maxmem)
{
    if (setDBMaxmemory(server, db, maxmem) != REDIS_OK) {
        log_debug ("set db %d maxmemory %llu failed", db, maxmem);
    }
}

uint64_t redis_db_context::get_maxmemory()
{
    return server->maxmemory;
}

size_t redis_db_context::item_count(uint32_t dbnum) const
{
    if (dbnum >= (uint32_t)server->db_num) return 0;
    return dbSize(server->db + dbnum);
}

size_t redis_db_context::autoremove_count(uint32_t dbnum) const
{
    if (dbnum >= (uint32_t)server->db_num)
        return 0;
    return server->db[dbnum].stat_evictedkeys +
      server->db[dbnum].stat_expiredkeys;
}

void redis_db_context::reset_autoremove_count(uint32_t dbnum)
{
    if (dbnum >= (uint32_t)server->db_num)
        return;

    server->db[dbnum].stat_evictedkeys = 0;
    server->db[dbnum].stat_expiredkeys = 0;
}

int redis_db_context::get_write_count(uint32_t dbnum) const
{
    if (dbnum >= (uint32_t)server->db_num)
        return 0;
    return server->db[dbnum].write_count;
}

void redis_db_context::reset_write_count(uint32_t dbnum)
{
    if (dbnum >= (uint32_t)server->db_num)
        return;
    server->db[dbnum].write_count = 0;
}

int redis_db_context::get_read_count(uint32_t dbnum) const
{
    if (dbnum >= (uint32_t)server->db_num)
        return 0;
    return server->db[dbnum].read_count;
}

void redis_db_context::reset_read_count(uint32_t dbnum)
{
    if (dbnum >= (uint32_t)server->db_num)
        return;
    server->db[dbnum].read_count = 0;
}

int redis_db_context::get_hit_count(uint32_t dbnum) const
{
    if (dbnum >= (uint32_t)server->db_num)
        return 0;
    return server->db[dbnum].hit_count;
}

void redis_db_context::reset_hit_count(uint32_t dbnum)
{
    if (dbnum >= (uint32_t)server->db_num)
        return;
    server->db[dbnum].hit_count = 0;
}

int redis_db_context::get_remove_count(uint32_t dbnum) const
{
    if (dbnum >= (uint32_t)server->db_num)
        return 0;
    return server->db[dbnum].remove_count;
}

void redis_db_context::reset_remove_count(uint32_t dbnum)
{
    if (dbnum >= (uint32_t)server->db_num)
        return;
    server->db[dbnum].remove_count = 0;
}

