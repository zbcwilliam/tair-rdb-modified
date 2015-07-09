#ifndef REDIS_DB_CONTEXT_H
#define REDIS_DB_CONTEXT_H

#include "redis/redislib.h"
#include <tbsys.h>
#include <Mutex.h>

#include "redis_define.h"

BEGIN_NS

class redis_server_cron;


class redis_db_context
{
public:
    redis_db_context();
    redis_db_context(const redisConfig &config);
	virtual ~redis_db_context();

    void start();
    void stop();

    void set_db_maxmemory(int dbid, uint64_t quota);
    uint64_t get_maxmemory();
    static uint64_t get_db_used_maxmemory(uint32_t db);

    size_t autoremove_count(uint32_t dbnum) const;
    void reset_autoremove_count(uint32_t dbnum);

    size_t item_count(uint32_t dbnum) const;

    int get_read_count(uint32_t dbnum) const;
    void reset_read_count(uint32_t dbnum);

    int get_write_count(uint32_t dbnum) const;
    void reset_write_count(uint32_t dbnum);

    int get_hit_count(uint32_t dbnum) const;
    void reset_hit_count(uint32_t dbnum);

    int get_remove_count(uint32_t dbnum) const;
    void reset_remove_count(uint32_t dbnum);
public:
    int get_unit_num() {return unit_num;}
    int get_area_num() {return area_group_num;}
private:
    redisServer *create_context();
    redisServer *create_context(const redisConfig &config);
    void destroy_context(redisServer *server);
private:
    redis_server_cron *server_cron;
    redisServer *server;
    int area_group_num;
    int unit_num;
    //int mutex_num;
    friend class redis_db_session;
    friend class redis_server_cron;
private:
    static bool inited;
    static void initialize_shared_data();
};

END_NS

#endif
