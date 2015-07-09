#include <pthread.h>

class scope_lock
{
public:
    scope_lock(pthread_mutex_t* mutex) {
        _mutex = mutex;
        pthread_mutex_lock(_mutex);
    }
    ~scope_lock() {
        pthread_mutex_unlock(_mutex);
    }
private:
    scope_lock(scope_lock& lock);
private:
    pthread_mutex_t *_mutex;
};
