#ifndef __MEM_QUEUE__
#define __MEM_QUEUE__

#include <tbnet.h>
#include <tbsys.h>

#include "queue_config.h"

template<class T>
class MemQueue {
public:
    MemQueue(const int mode = ONE_READ_ONE_WRITE,
            int size = _default_max_size);
    ~MemQueue();

    bool isInit();

    bool push(T* data);
    bool pop(T** data);
    bool empty();
    bool full();
private:
    int _head_index;
    int _tail_index;
    int _max_size;
    T** _mem_queue;

    int _mode;

    const static int _default_max_size;

    pthread_mutex_t _head_index_lock;
    pthread_mutex_t _tail_index_lock;

    bool _is_inited;
};

template<class T>
const int MemQueue<T>::_default_max_size = 4*1024;

template<class T>
bool MemQueue<T>::isInit() {
    return _is_inited;
}

template<class T>
MemQueue<T>::MemQueue(const int mode, const int size) {
    _is_inited = false;

    if (size <= 0) {
        _max_size = _default_max_size;
    } else {
        _max_size = size;
    }

    _mem_queue = NULL;
    void* tmp = calloc(sizeof(T*), (_max_size + 1));
    if (tmp == NULL) {
        TBSYS_LOG(ERROR, "%s", strerror(errno));
        return;
    }
    _mem_queue = (T**)tmp;

    _head_index = 0;
    _tail_index = 0;

    _mode = mode;

    if (_mode == ONE_READ_MULTI_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_init(&_tail_index_lock, NULL);
    }
    if (_mode ==  MULTI_READ_ONE_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_init(&_head_index_lock, NULL);
    }

    _is_inited = true;
}

template<class T>
MemQueue<T>::~MemQueue() {
    if (_mode == ONE_READ_MULTI_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_destroy(&_tail_index_lock);
    }
    if (_mode == MULTI_READ_ONE_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_destroy(&_head_index_lock);
    }

    if (_mem_queue) {
        free(_mem_queue);
    }
}

template<class T>
bool MemQueue<T>::push(T* data) {
    bool isOk = false;
    if (_mode == ONE_READ_MULTI_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_lock(&_tail_index_lock);
    }
    if (_mem_queue[_tail_index] == NULL) {
        _mem_queue[_tail_index] = data;
        _tail_index = (_tail_index + 1) % _max_size;
        isOk = true;
    }
    if (_mode == ONE_READ_MULTI_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_unlock(&_tail_index_lock);
    }
    return isOk;
}

template<class T>
bool MemQueue<T>::pop(T** data) {
    bool isOk = false;
    if (_mode == MULTI_READ_ONE_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_lock(&_head_index_lock);
    }
    *data = _mem_queue[_head_index];
    if (*data != NULL) {
        _mem_queue[_head_index] = NULL;
        _head_index = (_head_index + 1) % _max_size;
        isOk = true;
    }
    if (_mode == MULTI_READ_ONE_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_unlock(&_head_index_lock);
    }
    return isOk;
}

template<class T>
bool MemQueue<T>::empty() {
    bool isOk;
    if (_mode == ONE_READ_MULTI_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_lock(&_tail_index_lock);
    }
    if (_mode == MULTI_READ_ONE_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_lock(&_head_index_lock);
    }
    isOk = (_tail_index == _head_index);
    if (_mode == ONE_READ_MULTI_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_unlock(&_tail_index_lock);
    }
    if (_mode == MULTI_READ_ONE_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_unlock(&_head_index_lock);
    }
    return isOk;
}

template<class T>
bool MemQueue<T>::full() {
    bool isOk;
    if (_mode == ONE_READ_MULTI_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_lock(&_tail_index_lock);
    }
    if (_mode == MULTI_READ_ONE_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_lock(&_head_index_lock);
    }
    isOk = ((_tail_index + 1) % _max_size == _head_index);
    if (_mode == ONE_READ_MULTI_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_unlock(&_tail_index_lock);
    }
    if (_mode == MULTI_READ_ONE_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_unlock(&_head_index_lock);
    }
    return isOk;
}

#endif
