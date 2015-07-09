#ifndef __BINLOG__
#define __BINLOG__

#include "data_entry.hpp"
#include "file_op.hpp"

using namespace tair::common;

#define BINLOG_UNUSED(v) ((void*)(&(v)))
enum {
    USR_CACHED_LOG,
    SYS_CACHED_LOG,
    NO_CACHED_LOG
};

/*
 * binlog format: (RECORD_SIZEB)
 * "RECORD_START" record_len(4B) virtual_clock(8B) should_ret(4B) but_ret(4B) pcode(4) key_len(4) key(must < 1993B) "RECORD_END"
 */

class BinLog {
#define BIN_LOG_PREFIX "binlog_"
#define RECORD_SIZE 2048
#define RECORD_START "RECORD_START"
#define RECORD_END "RECORD_END"

public:
    BinLog(const char* filename, int mode = SYS_CACHED_LOG);
    ~BinLog();
    void setFileName(const char* filename) {
        if (filename == NULL) {
            return;
        }
        void* tmp = malloc(sizeof(char)*(strlen(filename)+1));
        if (tmp == NULL) {
            return;
        }
        if (_filename != NULL) {
            free(_filename);
        }
        _filename = (char*)tmp;
        strcpy(_filename,filename);
    }

    char* getFileName() {return _filename;}
    int getFD() {return _fd;}
protected:
    bool write(const int should_ret, const int but_ret, const int pcode, const data_entry* key);
    bool read(int* should_ret, int* but_ret, int* pcode, data_entry** key);
private:
    void setDetailFileName() {
        int name_len = 128;  //strlen("binlog_")+strlen(str(_file_id))+1
        int file_name_len = 0;
        if (_filename != NULL) {
            file_name_len = strlen(_filename);
        }
        name_len += file_name_len;
        void* tmp = malloc(sizeof(char)*name_len);
        if (tmp == NULL) {
            return;
        }
        _detail_filename = (char*)tmp;
        strcpy(_detail_filename,BIN_LOG_PREFIX);
        strcat(_detail_filename,_filename);
        snprintf(_detail_filename+7+file_name_len, 10, "%010d",_file_id);
    }

    bool usr_cached_write(const char* buffer, const int buffer_len);
    bool sys_cached_write(const char* buffer, const int buffer_len);
    bool no_cached_write(const char* buffer, const int buffer_len);

    bool usr_cached_read(char** buffer, const int buffer_len);
    bool sys_cached_read(char** buffer, const int buffer_len);

    bool next_log_file();
private:
    bool _need_seek;
    char* _filename;
    char* _detail_filename;
    int _fd;
    int _mode;
    unsigned int _file_id;
    unsigned long long _record_num;
    unsigned long long _file_max_record_num;
};

BinLog::BinLog(const char* filename, int mode) {
    _filename = NULL;
    _detail_filename = NULL;
    _fd = -1;
    _mode = mode;
    _file_id = 0;
    _record_num = 0;
    _file_max_record_num = (1<<20);
    _need_seek = false;
    setFileName(filename);
    setDetailFileName();

    _fd = FileOp::open(_detail_filename, O_APPEND);
    if (_fd < 0) {
        return;
    }
}

BinLog::~BinLog() {
    if (_filename != NULL) {
        free(_filename);
    }
    if (_detail_filename != NULL) {
        free(_detail_filename);
    }
    if (_fd >= 0) {
        close(_fd);
    }
}

bool BinLog::next_log_file() {
    if (_fd >= 0) {
        close(_fd);
        _fd = -1;
    }
    _file_id++;
    setDetailFileName();
    _fd = FileOp::open(_detail_filename, O_APPEND);
    if (_fd < 0) {
        return false;
    }

    _record_num = 0;
    _need_seek = false;
    return true;
}

bool BinLog::write(const int should_ret, const int but_ret, const int pcode, const data_entry* key) {
    if (key == NULL || _fd < 0) {
        return false;
    }

    char buffer[RECORD_SIZE];
    memset(buffer, 0, sizeof(buffer));
    int offset = 0;
    int record_size = sizeof(long long)+sizeof(int)*4+key->get_size();
    if (record_size + sizeof(int) + 23 > RECORD_SIZE) {
        return false;
    }
    memcpy(buffer+offset, RECORD_START, 12);
    offset += 12;
    memcpy(buffer+offset, (char*)&record_size, sizeof(int));
    offset += sizeof(int);
    unsigned long long vclock = _file_id*_file_max_record_num+_record_num;
    memcpy(buffer+offset, (char*)&vclock, sizeof(unsigned long long));
    offset += sizeof(unsigned long long);
    memcpy(buffer+offset, (char*)&should_ret, sizeof(int));
    offset += sizeof(should_ret);
    memcpy(buffer+offset, (char*)&but_ret, sizeof(int));
    offset += sizeof(but_ret);
    memcpy(buffer+offset, (char*)&pcode, sizeof(int));
    offset += sizeof(pcode);
    int size = key->get_size();
    memcpy(buffer+offset, (char*)&size, sizeof(int));
    offset += sizeof(int);
    memcpy(buffer+offset, key->get_data(), key->get_size());
    offset += key->get_size();
    memcpy(buffer+offset, RECORD_END, 11);
    offset += 11;

    if (_record_num == _file_max_record_num) {
        if (next_log_file() == false) {
            return false;
        }
    }

    if (_need_seek == true) {
        off_t where = lseek(_fd, _record_num * RECORD_SIZE, SEEK_SET);
        if (where == (off_t)-1) {
            _need_seek = true;
            return false;
        }
        _need_seek = false;
    }

    bool ok = false;
    switch(_mode) {
    case SYS_CACHED_LOG:
        ok = sys_cached_write(buffer, RECORD_SIZE);
        break;
    case USR_CACHED_LOG:
        ok = usr_cached_write(buffer, RECORD_SIZE);
        break;
    case NO_CACHED_LOG:
        ok = no_cached_write(buffer, RECORD_SIZE);
        break;
    }

    if (ok == true) {
        _record_num++;
        _need_seek = false;
    } else {
        off_t where = FileOp::seek(_fd, _record_num * RECORD_SIZE);
        if (where == (off_t)-1) {
            _need_seek = true;
        } else {
            _need_seek = false;
        }
    }

    return ok;
}

bool BinLog::read(int* should_ret, int* but_ret, int* pcode, data_entry** key) {
    int record_size;
    unsigned long long vclock = 0;
    char *buffer = NULL, *buff = NULL;

    bool ok = false;

    BINLOG_UNUSED(record_size);
    *should_ret = 0;
    *but_ret = 0;
    *pcode = 0;
    *key = NULL;

    switch(_mode) {
    case SYS_CACHED_LOG:
    case NO_CACHED_LOG:
        ok = sys_cached_read(&buff, RECORD_SIZE);
        break;
    case USR_CACHED_LOG:
        ok = usr_cached_read(&buff, RECORD_SIZE);
        break;
    default:
        return false;
    }

    if (!ok) {
        if (buff != NULL) {
            free(buff);
        }
        return false;
    }

    buffer = buff;

    buffer += 12;
    memcpy((char*)&record_size, buffer, sizeof(int));
    //record_size = (int)(*(int*)(buffer+offset));
    buffer += sizeof(int);
    memcpy((char*)&vclock, buffer, sizeof(unsigned long long));
    //vclock = (unsigned long long)(*(unsigned long long*)(buffer+offset));
    buffer += sizeof(unsigned long long);
    memcpy((char*)should_ret, buffer, sizeof(int));
    //*should_ret = (int)(*(int*)(buffer+offset));
    buffer += sizeof(int);
    memcpy((char*)but_ret, buffer, sizeof(int));
    //*but_ret = (int)(*(int*)(buffer+offset));
    buffer += sizeof(int);
    memcpy((char*)pcode, buffer, sizeof(int));
    //*pcode = (int)(*(int*)(buffer+offset));
    buffer += sizeof(int);
    int key_len = 0;
    memcpy((char*)&key_len, buffer, sizeof(int));
    //int key_len = (int)(*(int*)(buffer+offset));
    buffer += sizeof(int);

    *key = new data_entry(buffer, key_len);
    if (*key == NULL) {
        free(buff);
        return false;
    }

    free(buff);
    return true;
}

bool BinLog::usr_cached_write(const char* buffer, const int buffer_len) {
    return false;
}

bool BinLog::sys_cached_write(const char* buffer, const int buffer_len) {
    int len = FileOp::write(_fd, buffer, buffer_len);
    if (len != buffer_len) {
        return false;
    }
    return true;
}

bool BinLog::no_cached_write(const char* buffer, const int buffer_len) {
    return false;
}

bool BinLog::usr_cached_read(char** buffer, const int buffer_len) {
    return false;
}

bool BinLog::sys_cached_read(char** buffer, const int buffer_len) {
    int len = FileOp::read(_fd, buffer, buffer_len);
    if (len != buffer_len) {
        return false;
    }
    return true;
}

class BinLogReader:public BinLog {
public:
    BinLogReader(const char* filename, bool needlock, int mode = SYS_CACHED_LOG)
        : BinLog(filename, mode) {
        _need_lock = needlock;
        if (_need_lock) {
            pthread_mutex_init(&_mutex, NULL);
        }
    }

    ~BinLogReader() {
        if (_need_lock) {
            pthread_mutex_destroy(&_mutex);
        }
    }
public:
    bool read(int* should_ret, int* but_ret, int* pcode, data_entry** key) {
        if (_need_lock) {
            pthread_mutex_lock(&_mutex);
        }
        bool ok = BinLog::read(should_ret, but_ret, pcode, key);
        if (_need_lock) {
            pthread_mutex_unlock(&_mutex);
        }

        return ok;
    }
private:
    pthread_mutex_t _mutex;
    bool _need_lock;
};

class BinLogWriter:public BinLog {
public:
    BinLogWriter(const char* filename, bool needlock, int mode = SYS_CACHED_LOG)
        : BinLog(filename, mode) {
        _need_lock = needlock;
        if (_need_lock) {
            pthread_mutex_init(&_mutex, NULL);
        }
    }

    ~BinLogWriter() {
        if (_need_lock) {
            pthread_mutex_destroy(&_mutex);
        }
    }

public:
    bool append(const int should_ret, const int but_ret, const int pcode, const data_entry* key) {
        if (_need_lock) {
            pthread_mutex_lock(&_mutex);
        }
        bool ok = BinLog::write(should_ret, but_ret, pcode, key);
        if (_need_lock) {
            pthread_mutex_unlock(&_mutex);
        }

        return ok;
    }
private:
    pthread_mutex_t _mutex;
    bool _need_lock;
};

#endif
