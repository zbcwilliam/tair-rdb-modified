#ifndef __LOCAL_QUEUE__
#define __LOCAL_QUEUE__

#include "queue_config.h"
#include "disk_queue.hpp"
#include "mem_queue.hpp"
#include "base_queue_node.hpp"

//notes: one read one write mode
//write must be one or less than one
//read must be one or less than one
class LocalQueue {
#define DISK_QUEUE_CONFIG_FILE "queue/local_disk_queue"
public:
typedef bool (*serialize_func_)(BaseQueueNode* data, tbnet::DataBuffer **buffer);
typedef bool (*deserialize_func_)(BaseQueueNode** data, tbnet::DataBuffer *buffer);
public:
    LocalQueue(serialize_func_ serializer, deserialize_func_ deserializer) :
        _disk_queue(DISK_QUEUE_CONFIG_FILE, ONE_READ_ONE_WRITE, 0, serializer, deserializer) ,
        _mem_queue() {
            _push_next_node_id = 0;
            _pop_next_node_id = 0;
            _may_next_in_mem = NULL;
            _may_next_in_disk = NULL;
            _mode = ONE_READ_MULTI_WRITE;
            init();
        }
    ~LocalQueue() {}

    bool isInit();

    bool push(BaseQueueNode* node);
    bool pop(BaseQueueNode** node);
private:
    void init();
private:
    int _mode;
    long long _push_next_node_id;
    long long _pop_next_node_id;
    DiskQueue<BaseQueueNode> _disk_queue;
    MemQueue<BaseQueueNode> _mem_queue;
    BaseQueueNode* _may_next_in_mem;
    BaseQueueNode* _may_next_in_disk;

    pthread_mutex_t _head_index_lock;
    pthread_mutex_t _tail_index_lock;

    bool failed_deal_switch;
};

void LocalQueue::init() {
    if (_mode == ONE_READ_MULTI_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_init(&_tail_index_lock, NULL);
    }
    if (_mode ==  MULTI_READ_ONE_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_init(&_head_index_lock, NULL);
    }
    failed_deal_switch = false;
}

bool LocalQueue::isInit() {
    return _disk_queue.isInit() &&
        _mem_queue.isInit();
}

bool LocalQueue::push(BaseQueueNode* node) {
    bool is_ok = false;
    if (node == NULL) {
        return is_ok;
    }

    pthread_mutex_lock(&_tail_index_lock);

    node->set_node_id(_push_next_node_id);
    if (!_mem_queue.full()) {
        is_ok = _mem_queue.push(node);
    } else {
        is_ok = _disk_queue.push(node);
    }

    if (is_ok) {
        _push_next_node_id++;
    }

    pthread_mutex_unlock(&_tail_index_lock);

    return is_ok;
}

bool LocalQueue::pop(BaseQueueNode** node) {
    bool is_ok = false;
    int id = _pop_next_node_id;
    if (_may_next_in_mem != NULL) {
        if (id == _may_next_in_mem->get_node_id()) {
            *node = _may_next_in_mem;
            _may_next_in_mem = NULL;
            is_ok = true;
            failed_deal_switch = false;
            _pop_next_node_id++;
            return is_ok;
        }
    } else if (!_mem_queue.empty()) {
        is_ok = _mem_queue.pop(node);
        if (is_ok) {
            if (id == (*node)->get_node_id()) {
                _pop_next_node_id++;
                failed_deal_switch = false;
                return is_ok;
            } else {
                _may_next_in_mem = *node;
                *node = NULL;
                is_ok = false;
            }
        }
    }

    if (_may_next_in_disk != NULL) {
        if (id == _may_next_in_disk->get_node_id()) {
            *node = _may_next_in_disk;
            _may_next_in_disk = NULL;
            is_ok = true;
            failed_deal_switch = false;
            _pop_next_node_id++;
            return is_ok;
        }
    } else if (!_disk_queue.empty()) {
        is_ok = _disk_queue.pop(node);
        if (is_ok) {
            if (id == (*node)->get_node_id()) {
                _pop_next_node_id++;
                failed_deal_switch = false;
                return is_ok;
            } else {
                _may_next_in_disk = *node;
                *node = NULL;
                is_ok = false;
            }
        }
    }

    long long id_in_mem;
    long long id_in_disk;

    if (_may_next_in_mem == NULL && _may_next_in_disk == NULL) {
        //almost no data,less failed disk && memory null
        //if failed disk && memory null, when data push in,this state will be other state
        failed_deal_switch = false;
        return is_ok;
    } else if (_may_next_in_mem != NULL && _may_next_in_disk != NULL) {
        //must lost data
        id_in_mem = _may_next_in_mem->get_node_id();
        id_in_disk = _may_next_in_disk->get_node_id();
        if (id_in_mem < id_in_disk) {
            *node = _may_next_in_mem;
            _may_next_in_mem = NULL;
            is_ok = true;
            failed_deal_switch = false;
            TBSYS_LOG(INFO, "lost packet from %lld to %lld", _pop_next_node_id, id_in_mem);
            _pop_next_node_id = id_in_mem + 1;
        } else if (id_in_mem < id_in_disk) {
            *node = _may_next_in_disk;
            _may_next_in_disk = NULL;
            is_ok = true;
            failed_deal_switch = false;
            TBSYS_LOG(INFO, "lost packet from %lld to %lld", _pop_next_node_id, id_in_disk);
            _pop_next_node_id = id_in_disk + 1;
        }
    } else if (_may_next_in_mem == NULL) {
        //must lost data
        *node = _may_next_in_disk;
        _may_next_in_disk = NULL;
        is_ok = true;
        failed_deal_switch = false;
        TBSYS_LOG(INFO, "lost packet from %lld to %lld", _pop_next_node_id, id_in_disk);
        _pop_next_node_id = id_in_disk + 1;
    } else {
        //may we need wait once more
        if (failed_deal_switch) {
            *node = _may_next_in_mem;
            _may_next_in_mem = NULL;
            is_ok = true;
            failed_deal_switch = true;
            TBSYS_LOG(INFO, "lost packet from %lld to %lld", _pop_next_node_id, id_in_mem);
            _pop_next_node_id = id_in_mem + 1;
        } else {
            failed_deal_switch = true;
        }
    }

    return is_ok;
}

#endif
