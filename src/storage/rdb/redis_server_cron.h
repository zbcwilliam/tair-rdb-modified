#ifndef REDIS_SERVER_CRON_H
#define REDIS_SERVER_CRON_H

#include <tbsys.h>
#include "redis_define.h"

BEGIN_NS

class redis_db_context;

class redis_server_cron : public tbsys::CDefaultRunnable 
{
public:
    redis_server_cron(const redis_db_context &context);
    virtual ~redis_server_cron();
    virtual void run(tbsys::CThread *thread, void *arg);  
    virtual void stop();
private:
    const redis_db_context &context;

    tbsys::CThreadCond cond;
};

END_NS

#endif

