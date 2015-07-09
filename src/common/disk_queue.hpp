#ifndef __DISK_QUEUE__
#define __DISK_QUEUE__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <tbsys.h>
#include <tbnet.h>

#include "file_op.hpp"
#include "queue_config.h"

template<class T>
class DiskQueue {
public:
typedef bool (*serialize_func)(T* data, tbnet::DataBuffer** dataBuffer);
typedef bool (*deserialize_func)(T** data, tbnet::DataBuffer* dataBuffer);
public:
    DiskQueue(const char* queue_name, const int mode, const int max_file_size,
            serialize_func _serialize, deserialize_func _deserialize, bool isnew = true);
    ~DiskQueue();

    bool empty();
    bool push(T* data);
    bool pop(T** data);
public:
    int getMode() {return _mode;}
    char* getQueueName() {return _queue_name;}
    int getMaxFileSize() {return _max_file_size;}
    bool isInit();
private:
    void initMode(const int mode);
    void initQueueName(const char* queue_name, const int len);
    void initMaxFileSize(const int size);

    bool loadConfig();
    bool newConfig();
    int initFD(const int id, const int mode);
    bool initQueueHead();
    bool initQueueTail();

    bool writeIntProperty(const char* key, const int value, const int maxlen, const int offset);
    bool readIntProperty(const char* key, int* value, const int maxlen, const int offset);
    bool writeRecord(tbnet::DataBuffer* dataBuffer);
    bool readRecord(tbnet::DataBuffer** dataBuffer);
    bool isNeedEndSegment();
    bool endSegment(int* fd);
    bool nextSegment(int* fd, int* id, int* offset, const int mode);

private:
    int _head_fd;
    int _tail_fd;

    bool _head_fd_no_need_seek;
    bool _tail_fd_no_need_seek;

    pthread_mutex_t _head_fd_lock;
    pthread_mutex_t _tail_fd_lock;

    const static int MAX_FILE_SIZE_FIELD_LEN;
    const static char* MAX_FILE_SIZE_FIELD;
    int _max_file_size;

    const static int FILE_START_ID_FIELD_LEN;
    const static char* FILE_START_ID_FIELD;
    int _file_start_id;

    const static int FILE_END_ID_FIELD_LEN;
    const static char* FILE_END_ID_FIELD;
    int _file_end_id;

    const static int FILE_START_ID_OFFSET_FIELD_LEN;
    const static char* FILE_START_OFFSET_FIELD;
    int _file_start_offset;

    const static int FILE_END_ID_OFFSET_FIELD_LEN;
    const static char* FILE_END_OFFSET_FIELD;
    int _file_end_offset;

    int _queue_config_fd;
    int _mode;
    char* _queue_name;
    serialize_func _serialize;
    deserialize_func _deserialize;

    int _is_init;
    const static char* DEFAULT_QUEUE_NAME;
    const static int SEGMENT_END_FLAG_LEN;
    const static char* SEGMENT_END_FLAG;
};

template<class T>
const int DiskQueue<T>::MAX_FILE_SIZE_FIELD_LEN =          64;
template<class T>
const int DiskQueue<T>::FILE_START_ID_FIELD_LEN =          64;
template<class T>
const int DiskQueue<T>::FILE_END_ID_FIELD_LEN =            64;
template<class T>
const int DiskQueue<T>::FILE_START_ID_OFFSET_FIELD_LEN =   64;
template<class T>
const int DiskQueue<T>::FILE_END_ID_OFFSET_FIELD_LEN =     64;

template<class T>
const char* DiskQueue<T>::MAX_FILE_SIZE_FIELD = "max_file_size";
template<class T>
const char* DiskQueue<T>::FILE_START_ID_FIELD = "file_start_id";
template<class T>
const char* DiskQueue<T>::FILE_END_ID_FIELD = "file_end_id";
template<class T>
const char* DiskQueue<T>::FILE_START_OFFSET_FIELD = "file_start_offset";
template<class T>
const char* DiskQueue<T>::FILE_END_OFFSET_FIELD = "file_end_offset";

template<class T>
const char* DiskQueue<T>::DEFAULT_QUEUE_NAME = ".queue.list";
template<class T>
const int DiskQueue<T>::SEGMENT_END_FLAG_LEN = 15;
template<class T>
const char* DiskQueue<T>::SEGMENT_END_FLAG = "==END SEGMENT==";

template<class T>
DiskQueue<T>::DiskQueue(const char* queue_name, const int mode, const int max_file_size,
        serialize_func serialize, deserialize_func deserialize, bool isnew) {
    _is_init = false;
    _file_start_id = 0;
    _file_end_id = 0;
    _file_start_offset = -1;
    _file_end_offset = -1;

    _queue_config_fd = -1;
    _head_fd = -1;
    _tail_fd = -1;

    _head_fd_no_need_seek = true;
    _tail_fd_no_need_seek = true;

    _serialize = serialize;
    _deserialize = deserialize;

    _queue_name = NULL;

    initMaxFileSize(max_file_size);

    initQueueName(queue_name, queue_name?strlen(queue_name):0);

    if (mode <= 0) {
        _mode = ONE_READ_ONE_WRITE;
    }
    initMode(mode);

    bool isOk = false;
    if (isnew == false) {
        isOk = loadConfig();
        if (isOk == false) {
            isOk = newConfig();
        }
    } else {
        isOk = newConfig();
    }

    if (isOk) {
        isOk = initQueueHead();
    }

    if (isOk) {
        isOk = initQueueTail();
    }
    if (!isOk) {
        return;
    }

    _is_init = true;
}

template<class T>
bool DiskQueue<T>::isInit() {
    return _is_init;
}

template<class T>
DiskQueue<T>::~DiskQueue() {
    if (_head_fd >= 0) {
        close(_head_fd);
        _head_fd = -1;
    }
    if (_tail_fd >= 0) {
        close(_tail_fd);
        _tail_fd = -1;
    }
    if (_queue_config_fd >= 0) {
        close(_queue_config_fd);
        _queue_config_fd = -1;
    }

    if (_queue_name) {
        free(_queue_name);
        _queue_name = NULL;
    }

    if (_mode == MULTI_READ_ONE_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_destroy(&_head_fd_lock);
    }
    if (_mode == MULTI_READ_MULTI_WRITE ||
            _mode == ONE_READ_MULTI_WRITE) {
        pthread_mutex_destroy(&_tail_fd_lock);
    }
}

template<class T>
bool DiskQueue<T>::empty() {
    bool is_empty = false;
    if (_mode == MULTI_READ_ONE_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_lock(&_head_fd_lock);
    }
    if (_mode == MULTI_READ_MULTI_WRITE ||
            _mode == ONE_READ_MULTI_WRITE) {
        pthread_mutex_lock(&_tail_fd_lock);
    }

    if (_file_start_id == _file_end_id &&
            _file_start_offset == _file_end_offset) {
        is_empty = true;
    } else {
        is_empty = false;
    }

    if (_mode == MULTI_READ_MULTI_WRITE ||
            _mode == ONE_READ_MULTI_WRITE) {
        pthread_mutex_unlock(&_tail_fd_lock);
    }
    if (_mode == MULTI_READ_ONE_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_unlock(&_head_fd_lock);
    }

    return is_empty;
}

template<class T>
bool DiskQueue<T>::push(T* data) {
    if (_serialize == NULL || _is_init == false) {
        TBSYS_LOG(ERROR, "%s", strerror(errno));
        return 0;
    }

    tbnet::DataBuffer* dataBuffer = NULL;
    bool isOk = _serialize(data, &dataBuffer);
    if (isOk) {
        if (_mode == ONE_READ_MULTI_WRITE ||
                _mode == MULTI_READ_MULTI_WRITE) {
            pthread_mutex_lock(&_tail_fd_lock);
        }
        isOk = writeRecord(dataBuffer);
        if (_mode == ONE_READ_MULTI_WRITE ||
                _mode == MULTI_READ_MULTI_WRITE) {
            pthread_mutex_unlock(&_tail_fd_lock);
        }
    }

    if (data != NULL) {
        delete data;
    }
    if (dataBuffer != NULL) {
        delete dataBuffer;
    }
    return isOk;
}

template<class T>
bool DiskQueue<T>::pop(T** data) {
    bool isOk = false;
    if (_deserialize == NULL || _is_init == 0) {
        TBSYS_LOG(ERROR, "%s", strerror(errno));
        return isOk;
    }

    tbnet::DataBuffer* dataBuffer = NULL;
    if (_mode == MULTI_READ_ONE_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_lock(&_head_fd_lock);
    }
    isOk = readRecord(&dataBuffer);
    if (_mode == MULTI_READ_ONE_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_unlock(&_head_fd_lock);
    }

    if (isOk) {
        isOk = _deserialize(data, dataBuffer);
    }

    if (dataBuffer != NULL) {
        delete dataBuffer;
    }

    return isOk;
}

template<class T>
bool DiskQueue<T>::initQueueHead() {
    _head_fd = initFD(_file_start_id, 0);
    if (_head_fd < 0) {
        return false;
    }

    if (_mode == MULTI_READ_ONE_WRITE ||
            _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_init(&_head_fd_lock, NULL);
    }
    return true;
}

template<class T>
bool DiskQueue<T>::initQueueTail() {
    _tail_fd = initFD(_file_end_id, O_TRUNC);
    if (_tail_fd < 0) {
        return false;
    }

    if (_mode == ONE_READ_MULTI_WRITE ||
           _mode == MULTI_READ_MULTI_WRITE) {
        pthread_mutex_init(&_tail_fd_lock, NULL);
    }
    return true;
}

template<class T>
bool DiskQueue<T>::writeIntProperty(const char* key, const int value,
        const int maxlen, const int offset) {
    if (_queue_config_fd < 0) {
        _queue_config_fd = FileOp::open(_queue_name);
        if (_queue_config_fd < 0) {
            return false;
        }
    }

    void* tmp = calloc(maxlen, sizeof(char));
    if (tmp == NULL) {
        TBSYS_LOG(ERROR, "%s", strerror(errno));
        return false;
    }
    char* buffer = (char* )tmp;
    snprintf(buffer, maxlen, "%s %d", key, value);

    if (offset >= 0) {
        if (FileOp::seek(_queue_config_fd, offset) < 0) {
            if (tmp != NULL) {
                free(tmp);
            }
            return false;
        }
    }

    int write_len = FileOp::write(_queue_config_fd, (const char*)buffer, maxlen);
    if (tmp != NULL) {
        free(tmp);
    }
    return (write_len == maxlen);
}

template<class T>
bool DiskQueue<T>::readIntProperty(const char* key, int* value, const int maxlen,
        const int offset) {
    if (_queue_config_fd < 0) {
        _queue_config_fd = FileOp::open(_queue_name);
        if (_queue_config_fd < 0) {
            return false;
        }
    }

    if (offset >= 0) {
        if (FileOp::seek(_queue_config_fd, offset) < 0) {
            return false;
        }
    }

    void* tmp = malloc(sizeof(char) * (maxlen + 1));
    if (tmp == NULL) {
        TBSYS_LOG(ERROR, "%s", strerror(errno));
        return false;
    }

    char* buffer = NULL;
    int len;
    len = FileOp::read(_queue_config_fd, &buffer, maxlen);
    if (len != maxlen) {
        TBSYS_LOG(ERROR, "%s", strerror(errno));
        if (buffer != NULL) {
            free(buffer);
            buffer = NULL;
        }
        free(tmp);
        return false;
    }
    sscanf(buffer, "%s %d", (char*)tmp, value);
    free(buffer);
    free(tmp);

    return true;
}

template<class T>
bool DiskQueue<T>::writeRecord(tbnet::DataBuffer* dataBuffer) {
    if (isNeedEndSegment()) {
        if(!endSegment(&_tail_fd)) {
            return false;
        }
        if (!nextSegment(&_tail_fd, &_file_end_id, &_file_end_offset, O_TRUNC)) {
            return false;
        }
        _tail_fd_no_need_seek = true;
    }

    int data_len = dataBuffer->getDataLen();
    char* data = dataBuffer->getData();

    void* tmp_buffer = malloc(sizeof(char)*(sizeof(data_len)+data_len));
    if(tmp_buffer == NULL) {
        return false;
    }
    char* write_buffer = (char*)tmp_buffer;

    memcpy(write_buffer, (char*)&data_len, sizeof(data_len));
    memcpy(write_buffer+sizeof(data_len), data, data_len);

    if (_tail_fd_no_need_seek == false) {
        off_t record_start = FileOp::seek(_tail_fd, _file_end_offset);
        if (record_start < 0) {
            return false;
        }
        _tail_fd_no_need_seek = true;
    }

    int len = FileOp::write(_tail_fd, write_buffer, data_len+sizeof(data_len));
    if (len != (int)(data_len + sizeof(data_len))) {
        _tail_fd_no_need_seek = false;
        free(write_buffer);
        return false;
    }

    _file_end_offset += sizeof(data_len) + data_len;
    _tail_fd_no_need_seek = true;

    free(write_buffer);
    return true;
}

template<class T>
bool DiskQueue<T>::readRecord(tbnet::DataBuffer** dataBuffer) {
    if (_head_fd_no_need_seek == false) {
        off_t record_start = FileOp::seek(_head_fd, _file_start_offset);
        if (record_start < 0) {
            return false;
        }
    }

    //to easy do deal with multiway to exit
    _head_fd_no_need_seek = false;

    char* data = NULL;
    int data_len;

    int len;
    char* data_len_tmp = NULL;
    int* tmp_int = 0;

    data_len = -1;
    while(data_len < 0) {
        len = FileOp::read(_head_fd, &data_len_tmp, sizeof(data_len));
        if (len != sizeof(data_len)) {
            if (data_len_tmp) {
                free(data_len_tmp);
            }
            return false;
        }
        tmp_int = (int*)(data_len_tmp);
        data_len = *tmp_int;
        if (data_len_tmp) {
            free(data_len_tmp);
        }

        if (data_len < 0) {
            int isOk = nextSegment(&_head_fd, &_file_start_id, &_file_start_offset, 0);
            if (!isOk) {
                return false;
            }
        }
    }

    len = FileOp::read(_head_fd, &data, data_len);
    if (len != data_len) {
        return false;
    }

    (*dataBuffer) = new tbnet::DataBuffer();
    (*dataBuffer)->writeBytes(data, data_len);
    _file_start_offset += sizeof(data_len) + data_len;
    //trun head_fd_no_need_seek to true
    _head_fd_no_need_seek = true;

    return true;
}

template<class T>
bool DiskQueue<T>::isNeedEndSegment() {
    if (_file_end_offset >= _max_file_size) {
        return true;
    }
    return false;
}

template<class T>
bool DiskQueue<T>::endSegment(int* fd) {
    off_t end_offset = FileOp::seek(*fd, _file_end_offset);
    if (end_offset < 0) {
       return false;
    }
    int flag_len = -1 * SEGMENT_END_FLAG_LEN;
    int len = FileOp::write(*fd, (const char*)&flag_len, sizeof(flag_len));
    if (len != sizeof(flag_len)) {
        return false;
    }
    len = FileOp::write(*fd, (const char*)(SEGMENT_END_FLAG), -flag_len);
    if (len != -flag_len) {
        return false;
    }

    if (*fd > 0) {
        close(*fd);
        *fd = -1;
    }

    return true;
}

template<class T>
bool DiskQueue<T>::nextSegment(int* fd, int* id, int* offset, int mode) {
    if (*fd > 0) {
        close(*fd);
        *fd = -1;
    }

    int next_id = *id + 1;
    *fd = initFD(next_id, mode);
    if (*fd < 0) {
        return false;
    }
    *id = next_id;
    *offset = 0;

    return true;
}

template<class T>
int DiskQueue<T>::initFD(const int id, const int mode) {
    char buffer[256];
    sprintf(buffer, "%s_%d", _queue_name, id);
    return FileOp::open(buffer, mode);
}

template<class T>
void DiskQueue<T>::initMode(const int mode) {
    _mode = mode;
}

template<class T>
void DiskQueue<T>::initQueueName(const char* queue_name, const int len) {
    if (_queue_name) {
        free(_queue_name);
    }

    int slen = len;
    if (queue_name == NULL || slen == 0) {
        queue_name = DEFAULT_QUEUE_NAME;
        slen = strlen(DEFAULT_QUEUE_NAME);
    }
    void* buffer = malloc(sizeof(char) * (slen + 1));
    if (buffer == NULL) {
        TBSYS_LOG(ERROR, "%s", strerror(errno));
        exit(1);
    }
    _queue_name = (char*)buffer;

    memcpy(_queue_name, queue_name, slen);
    _queue_name[slen] = '\0';
}

template<class T>
void DiskQueue<T>::initMaxFileSize(const int size) {
    _max_file_size = (size ? size : (1 << 28));
}

template<class T>
bool DiskQueue<T>::newConfig() {
    if (_queue_config_fd > 0) {
        close(_queue_config_fd);
        _queue_config_fd = -1;
    }
    _queue_config_fd = FileOp::open(_queue_name, O_TRUNC);
    if (_queue_config_fd < 0) {
        return false;
    }

    int isOk;
    isOk = writeIntProperty(MAX_FILE_SIZE_FIELD, _max_file_size,
            MAX_FILE_SIZE_FIELD_LEN, 0);
    if (!isOk) {
        return false;
    }

    _file_start_id = 0;
    isOk = writeIntProperty(FILE_START_ID_FIELD, _file_start_id,
            FILE_START_ID_FIELD_LEN, -1);
    if (!isOk) {
        return false;
    }

    _file_end_id = 0;
    isOk = writeIntProperty(FILE_END_ID_FIELD, _file_end_id,
            FILE_END_ID_FIELD_LEN, -1);
    if (!isOk) {
        return false;
    }

    _file_start_offset = 0;
    isOk = writeIntProperty(FILE_START_OFFSET_FIELD, _file_start_offset,
            FILE_START_ID_OFFSET_FIELD_LEN, -1);
    if (!isOk) {
        return false;
    }

    _file_end_offset = 0;
    isOk = writeIntProperty(FILE_END_OFFSET_FIELD, _file_end_offset,
            FILE_END_ID_OFFSET_FIELD_LEN, -1);
    if (!isOk) {
        return false;
    }

    return true;
}

template<class T>
bool DiskQueue<T>::loadConfig() {
    _queue_config_fd = FileOp::open(_queue_name);
    if (_queue_config_fd <= 0) {
        return false;
    }

    int isOk;
    isOk = readIntProperty(MAX_FILE_SIZE_FIELD, &_max_file_size,
            MAX_FILE_SIZE_FIELD_LEN, 0);
    if (!isOk) {
       return false;
    }

    isOk = readIntProperty(FILE_START_ID_FIELD, &_file_start_id,
            FILE_START_ID_FIELD_LEN, -1);
    if (!isOk) {
        return false;
    }

    isOk = readIntProperty(FILE_END_ID_FIELD, &_file_end_id,
            FILE_END_ID_FIELD_LEN, -1);
    if (!isOk) {
        return false;
    }

    isOk = readIntProperty(FILE_START_OFFSET_FIELD, &_file_start_offset,
            FILE_START_ID_OFFSET_FIELD_LEN, -1);
    if (!isOk) {
        return false;
    }

    isOk = readIntProperty(FILE_END_OFFSET_FIELD, &_file_end_offset,
            FILE_END_ID_OFFSET_FIELD_LEN, -1);
    if (!isOk) {
        return false;
    }

    return true;
}

#endif
