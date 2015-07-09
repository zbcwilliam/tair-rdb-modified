#include "redis.h"
#include <sys/uio.h>

void *dupClientReplyValue(void *o) {
    incrRefCount((robj*)o);
    return o;
}

int listMatchObjects(void *a, void *b) {
    return equalStringObjects(a,b);
}

redisClient *createClient(struct redisServer *server, int index) {
    redisClient *c = zmalloc(sizeof(redisClient));
    if (!c) return NULL;
    c->db        = &(server->db[index]);
    c->old_dbnum = 0;
    c->oldargc   = 0;
    c->argc      = 0;
    c->argv      = NULL;
    c->cmd       = NULL;
    c->server    = server;
    return c;
}

/* Create a duplicate of the last object in the reply list when
 * it is not exclusively owned by the reply list. */
robj *dupLastObjectIfNeeded(list *reply) {
    robj *new, *cur;
    listNode *ln;
    redisAssert(listLength(reply) > 0);
    ln = listLast(reply);
    cur = listNodeValue(ln);
    if (cur->refcount > 1) {
        new = dupStringObject(cur);
        decrRefCount(cur);
        listNodeValue(ln) = new;
    }
    return listNodeValue(ln);
}

static void freeClientArgv(redisClient *c) {
    int j;
    for (j = 0; j < c->argc; j++) {
        if(c->argv[j] != NULL) {
            decrRefCount(c->argv[j]);
            c->argv[j] = NULL;
        }
    }
    c->argc = 0;
    c->cmd = NULL;
}

/* resetClient prepare the client to process the next command */
void resetClient(redisClient *c) {
    freeClientArgv(c);
}

void freeClient(redisClient *c) {
    if (!c) return;
    freeClientArgv(c);
    zfree(c);
}
