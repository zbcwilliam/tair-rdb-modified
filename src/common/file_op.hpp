#ifndef __FILE_OP__
#define __FILE_OP__

#include <tbsys.h>
#include <tbnet.h>

class FileOp {
public:
    static int open(char* name, int mode = O_CREAT|O_RDWR);
    static bool mkdirs(char *dirpath); 
    static int read(const int fd, char** buffer, const int32_t nbytes);
    static int write(const int fd, const char* buffer, const int32_t nbytes);
    static off_t seek(int fd, off_t offset);
};

int FileOp::open(char* file_name, int mode) {
    int len = strlen(file_name);
    int index;
    for(index = len - 1; index >= 0 && file_name[index] != '/'; index--);
    if (index >= 0) {
        char dir_end = file_name[index + 1];
        file_name[index + 1] = '\0';
        if (!mkdirs(file_name)) {
            file_name[index + 1] = dir_end;
            TBSYS_LOG(ERROR, "%s", strerror(errno));
            return -1;
        }
        file_name[index + 1] = dir_end;
    }

    int fd = ::open(file_name, mode|O_CREAT|O_RDWR, 0644);
    if (fd < 0) {
        TBSYS_LOG(ERROR, "%s", strerror(errno));
        return -1;
    }
    return fd;
}

bool FileOp::mkdirs(char *dirpath) 
{
    struct stat stats;
    if (lstat (dirpath, &stats) == 0 && S_ISDIR (stats.st_mode)) 
        return true;

    mode_t umask_value = umask (0);
    umask (umask_value);
    mode_t mode = ((S_IRWXU | S_IRWXG | S_IRWXO) & (~ umask_value)) | S_IWUSR | S_IXUSR;

    char *slash = dirpath;
    while (*slash == '/')
        slash++;

    int ret;
    while (1) {
        slash = strchr (slash, '/');
        if (slash == NULL) {
            break;
        }

        *slash = '\0';
        ret = mkdir(dirpath, mode);
        *slash++ = '/';
        if (ret && errno != EEXIST) {
            return false;
        }

        while (*slash == '/') {
            slash++;
        }
    }
    
    ret = mkdir(dirpath, mode);
    if (ret && errno != EEXIST) {
        return false;
    }
    return true;
}

int FileOp::read(const int fd, char** buffer, const int32_t nbytes) {
    void* tmp = malloc(sizeof(char) * (nbytes + 1));
    if (tmp == NULL) {
        TBSYS_LOG(ERROR, "%s:nbytes: %d", strerror(errno), nbytes);
        *buffer = NULL;
        return 0;
    }
    char* buff = (char*)tmp;

    int32_t left = nbytes;
    int32_t read_len = 0;
    int now_try_times = 0;
    int max_try_times = 10;
    while (left > 0) {
        if ((read_len = ::read(fd, buff, left)) < 0) {
            read_len = -errno;
            if (EINTR == -read_len || EAGAIN == -read_len) {
                if (now_try_times > max_try_times) {
                    break;
                }
                now_try_times++;
                continue; /* call read() again */
            } else {
                TBSYS_LOG(ERROR, "%s", strerror(errno));
                break;
            }
        } else if (0 == read_len) {
            break; //reach end
        }

        now_try_times = 0;
        left -= read_len;
        buff += read_len;
        buff[0] = '\0';
    }

    *buffer = (char*)tmp;
    if (buff - (*buffer) == 0) {
        free(tmp);
        *buffer = NULL;
    }
    return (buff - (*buffer));
}

int FileOp::write(const int fd, const char* buffer, const int32_t nbytes) {
    int32_t left = nbytes;
    int32_t written_len = 0;
    const char* buff = buffer;
    
    int now_try_times = 0;
    int max_try_times = 10;
    while (left > 0) {
        if ((written_len = ::write(fd, buff, left)) < 0) {
            written_len = -errno;
            if (EINTR == -written_len || EAGAIN == -written_len) {
                if (now_try_times > max_try_times) {
                    break;
                }
                now_try_times++;
                continue;
            } else {
                TBSYS_LOG(ERROR, "%s", strerror(errno));
                break;
            }
        } else if (0 == written_len) {
            break;
        }

        now_try_times = 0;
        left -= written_len;
        buff += written_len;
    }

    return (buff - buffer);
}

off_t FileOp::seek(int fd, off_t offset) {
    off_t record_start = lseek(fd, offset, SEEK_SET);
    if (record_start < 0) {
        TBSYS_LOG(ERROR, "%s", strerror(errno));
        return -1;
    }
    return record_start;
}

#endif
