#include <gtest/gtest.h>
#include <queue>
#include <iostream>
#include "disk_queue.hpp"

class TestObject;

typedef struct ReadInfo {
    int mode;
    int max_read;
    int now_read;
    pthread_mutex_t lock;
    DiskQueue<TestObject>* diskQueue;
} ReadInfo;

typedef struct WriteInfo {
    int mode;
    int max_write;
    int now_write;
    pthread_mutex_t lock;
    DiskQueue<TestObject>* diskQueue;
} WriteInfo;

char* getRandomString(int len, int isallrandom);

class TestObject {
    public:
        TestObject(char* data, int data_len) {
            _data = data;
            _data_len = data_len;
        }
        static TestObject* createRandomOne(int data_len, int isallrandom);
        static TestObject* createOne(char* data, int data_len) {
            return new TestObject(data, data_len);
        }
        ~TestObject(){if (_data) free(_data);}

        TestObject* copy();

        char* getData() {return _data;}
        int getDataLen() {return _data_len;}
        
    private:
        char* _data;
        int _data_len;
};

TestObject* TestObject::copy() {
    int data_len = getDataLen();
    char* data = (char*)malloc(sizeof(char) * (data_len+1));
    memcpy(data, getData(), data_len);
    data[data_len] = '\0';

    return new TestObject(data, data_len);
}

TestObject* TestObject::createRandomOne(int data_len, int isallrandom) {
    return new TestObject(getRandomString(data_len, isallrandom), data_len);
}

char* getRandomString(int len, int isallrandom) {
    void* tmp = calloc(sizeof(char), (len + 1));
    if (tmp == NULL) {
        return NULL;
    }
    char* buffer = (char *)tmp;
    srand(time(NULL));
    int i;
    for(i = 0; i < len; i++) {
        if (isallrandom) {
            buffer[i] = rand() % 256;
        } else {
            buffer[i] = 'a' + rand()%26;
        }
    }
    return buffer;
}

bool serializer(TestObject* data, tbnet::DataBuffer** dataBuffer) {
    TestObject* node = (TestObject*)data;
    char* t_value = node->getData();
    int t_value_len = node->getDataLen();

    tbnet::DataBuffer* buffer = new tbnet::DataBuffer();
    buffer->writeInt32(t_value_len);
    buffer->writeBytes(t_value,t_value_len);

    (*dataBuffer) = buffer;
    return true;
}

bool deserializer(TestObject** data, tbnet::DataBuffer* dataBuffer) {
    int t_value_len = dataBuffer->readInt32();
    char* buffer = (char*)calloc(sizeof(char), t_value_len + 1);
    dataBuffer->readBytes(buffer, t_value_len);
    (*data) = new TestObject(buffer, t_value_len);

    return true;
}

void* read_thread_func(void* argv) {
    ReadInfo* readInfo = (ReadInfo*)argv;
    TestObject* testObject = NULL;
    while(true) {
        int isOk = readInfo->diskQueue->pop(&testObject);
        if (!isOk) {
            if (readInfo->now_read >= readInfo->max_read) {
                break;
            }
            //EXPECT_EQ(1, isOk);
            continue;
        }
        pthread_mutex_lock(&readInfo->lock);
        readInfo->now_read++;
        if (readInfo->now_read >= readInfo->max_read) {
            pthread_mutex_unlock(&readInfo->lock);
            break;
        }
        pthread_mutex_unlock(&readInfo->lock);
    }
    return NULL;
}

void* write_thread_func(void* argv) {
    WriteInfo* writeInfo = (WriteInfo*)argv;
    while(true) {
        if (writeInfo->now_write >= writeInfo->max_write) {
            break;
        }
        pthread_mutex_lock(&writeInfo->lock);
        if (writeInfo->now_write < writeInfo->max_write) { 
            TestObject* testObject = TestObject::createRandomOne(1024,1);
            int isOk = writeInfo->diskQueue->push(testObject);
            EXPECT_EQ(1, isOk);
        }
        if (writeInfo->now_write >= writeInfo->max_write) {
            pthread_mutex_unlock(&writeInfo->lock);
            break;
        }
        writeInfo->now_write++;
        pthread_mutex_unlock(&writeInfo->lock);
    }
    return NULL;
}

TEST(test_simple_1, DISKQUEUE_ONE_READ_ONE_WRITE) {
    DiskQueue<TestObject> disk_queue(NULL, 0, 0, serializer, deserializer);
    
    ASSERT_EQ(1, disk_queue.empty());
    ASSERT_STREQ(".queue.list", disk_queue.getQueueName());
    ASSERT_EQ(0, disk_queue.getMode());
    ASSERT_EQ(1024*1024*256, disk_queue.getMaxFileSize());

    int i;
    std::queue<TestObject*> TestObjectQueue;
    for(i = 0; i < 100; i++) {
        TestObject* tmp = TestObject::createRandomOne(1024, 0);
        TestObject* tmp1 = tmp->copy();
        TestObjectQueue.push(tmp1);
        ASSERT_EQ(1, disk_queue.push(tmp));
    }

    while(!TestObjectQueue.empty()) {
        TestObject* tmp = TestObjectQueue.front();
        TestObjectQueue.pop();
        TestObject* tmp2 = NULL;
        ASSERT_EQ(0, disk_queue.empty());
        ASSERT_EQ(1, disk_queue.pop(&tmp2));
        ASSERT_STREQ(tmp->getData(), tmp2->getData()); 
        delete tmp;
        delete tmp2;
    }

    ASSERT_EQ(1, disk_queue.empty());
}

TEST(test_large_visibe_char_1, DISKQUEUE_MULTI_READ_ONE_WRITE) {
    DiskQueue<TestObject> disk_queue("queue/zhangsan", 1, 1024*1024*10, serializer, deserializer);

    ASSERT_EQ(1, disk_queue.empty());
    ASSERT_STREQ("queue/zhangsan", disk_queue.getQueueName());
    ASSERT_EQ(1, disk_queue.getMode());
    ASSERT_EQ(1024*1024*10, disk_queue.getMaxFileSize());

    int i;
    std::queue<TestObject*> TestObjectQueue;
    for(i = 0; i < 1024*10*10; i++) {//10M * 10
        TestObject* tmp = TestObject::createRandomOne(1024, 0);
        TestObject* tmp1 = tmp->copy();
        TestObjectQueue.push(tmp1);
        ASSERT_EQ(1, disk_queue.push(tmp));
    }

    while(!TestObjectQueue.empty()) {
        TestObject* tmp = TestObjectQueue.front();
        TestObjectQueue.pop();
        TestObject* tmp2 = NULL;
        ASSERT_EQ(0, disk_queue.empty());
        ASSERT_EQ(1, disk_queue.pop(&tmp2));
        ASSERT_STREQ(tmp->getData(), tmp2->getData());
        delete tmp;
        delete tmp2;
    }
    
    ASSERT_EQ(1, disk_queue.empty());
}   

TEST(test_large_unvisibe_char_1, DISKQUEUE_MULTI_READ_MULTI_WRITE) {
    DiskQueue<TestObject> disk_queue("queue/lisi", 1, 1024*1024*10, serializer, deserializer);

    ASSERT_EQ(1, disk_queue.empty());
    ASSERT_STREQ("queue/lisi", disk_queue.getQueueName());
    ASSERT_EQ(1, disk_queue.getMode());
    ASSERT_EQ(1024*1024*10, disk_queue.getMaxFileSize());

    int i;
    std::queue<TestObject*> TestObjectQueue;
    for(i = 0; i < 1024*10; i++) {//10M * 10
        TestObject* tmp = TestObject::createRandomOne(1024, 1);
        TestObject* tmp1 = tmp->copy();
        TestObjectQueue.push(tmp1);
        ASSERT_EQ(1, disk_queue.push(tmp));
    }

    int index = 0;
    while(!TestObjectQueue.empty()) {
        TestObject* tmp = TestObjectQueue.front();
        TestObjectQueue.pop();
        TestObject* tmp2 = NULL;
        ASSERT_EQ(0, disk_queue.empty());
        ASSERT_EQ(1, disk_queue.pop(&tmp2));
        
        int data_len = tmp->getDataLen();
        char* adata = tmp->getData();
        char* bdata = tmp2->getData();
        for(i = 0; i < data_len; i++) {
            ASSERT_EQ(adata[i], bdata[i]);
        }

        delete tmp;
        delete tmp2;
        
        index++;
    }
}

TEST(test_large_unvisibe_char_2_test_file_refresh, DISKQUEUE_MULTI_READ_MULTI_WRITE) {
    DiskQueue<TestObject> disk_queue("queue/lisi", 1, 1024*1024*10, serializer, deserializer);

    ASSERT_EQ(1, disk_queue.empty());
    ASSERT_STREQ("queue/lisi", disk_queue.getQueueName());
    ASSERT_EQ(1, disk_queue.getMode());
    ASSERT_EQ(1024*1024*10, disk_queue.getMaxFileSize());

    int i;
    std::queue<TestObject*> TestObjectQueue;
    for(i = 0; i < 1024*10; i++) {//10M * 10
        TestObject* tmp = TestObject::createRandomOne(1024, 1);
        TestObject* tmp1 = tmp->copy();
        TestObjectQueue.push(tmp1);
        ASSERT_EQ(1, disk_queue.push(tmp));
    }

    int index = 0;
    while(!TestObjectQueue.empty()) {
        TestObject* tmp = TestObjectQueue.front();
        TestObjectQueue.pop();
        TestObject* tmp2 = NULL;
        ASSERT_EQ(0, disk_queue.empty());
        ASSERT_EQ(1, disk_queue.pop(&tmp2));
        
        int data_len = tmp->getDataLen();
        char* adata = tmp->getData();
        char* bdata = tmp2->getData();
        for(i = 0; i < data_len; i++) {
            ASSERT_EQ(adata[i], bdata[i]);
        }

        delete tmp;
        delete tmp2;
        
        index++;
    }
}

TEST(test_2_threads_1, DISKQUEUE_ONE_READ_ONE_WRITE) {
    DiskQueue<TestObject>* disk_queue = new DiskQueue<TestObject>("queue/2_thread_1", 0,
            1024*1024*10, serializer, deserializer);
    
    ReadInfo* readInfo = new ReadInfo;
    readInfo->now_read = 0;
    readInfo->max_read = 1024*10;
    readInfo->diskQueue = disk_queue;
    pthread_mutex_init(&readInfo->lock, NULL);
    
    WriteInfo* writeInfo = new WriteInfo;
    writeInfo->now_write = 0;
    writeInfo->max_write = 1024*10;
    writeInfo->diskQueue = disk_queue;
    pthread_mutex_init(&writeInfo->lock, NULL);

    pthread_t read_thread;
    pthread_t write_thread;

    if (pthread_create(&write_thread, NULL, write_thread_func, (void*)writeInfo) != 0) {
        printf("error\n");
        return;
    }

    if (pthread_create(&read_thread, NULL, read_thread_func, (void*)readInfo) != 0) {
        printf("error\n");
        return;
    }

    pthread_join(write_thread, NULL);
    pthread_join(read_thread, NULL);

    ASSERT_EQ(1, disk_queue->empty());
    
    delete readInfo->diskQueue;
    pthread_mutex_destroy(&readInfo->lock);
    delete readInfo;

    pthread_mutex_destroy(&writeInfo->lock);
    delete writeInfo;
}

TEST(test_n_1_threads_1, DISKQUEUE_MULTI_READ_ONE_WRITE) {
    DiskQueue<TestObject>* disk_queue = new DiskQueue<TestObject>("queue/N_thread_1", 1,
            1024*1024*10, serializer, deserializer);
    
    ReadInfo* readInfo = new ReadInfo;
    readInfo->now_read = 0;
    readInfo->max_read = 1024*10;
    readInfo->diskQueue = disk_queue;
    pthread_mutex_init(&readInfo->lock, NULL);
    
    WriteInfo* writeInfo = new WriteInfo;
    writeInfo->now_write = 0;
    writeInfo->max_write = 1024*10;
    writeInfo->diskQueue = disk_queue;
    pthread_mutex_init(&writeInfo->lock, NULL);

    pthread_t read_thread[10];
    pthread_t write_thread;

    if (pthread_create(&write_thread, NULL, write_thread_func, (void*)writeInfo) != 0) {
        printf("error\n");
        return;
    }

    for(int i = 0; i < 10; i++) {
        if (pthread_create(&read_thread[i], NULL, read_thread_func, (void*)readInfo) != 0) {
            printf("error\n");
            return;
        }
    }

    pthread_join(write_thread, NULL);
    for(int i = 0; i < 10; i++) {
        pthread_join(read_thread[i], NULL);
    }

    ASSERT_EQ(readInfo->now_read, writeInfo->now_write);
    ASSERT_EQ(readInfo->max_read, readInfo->now_read);
    ASSERT_EQ(1, disk_queue->empty());

    printf("emtf\n");
    delete readInfo->diskQueue;
    pthread_mutex_destroy(&readInfo->lock);
    delete readInfo;

    pthread_mutex_destroy(&writeInfo->lock);
    delete writeInfo;
}

TEST(test_1_n_threads_1, DISKQUEUE_ONE_READ_MULTI_WRITE) {
    DiskQueue<TestObject>* disk_queue = new DiskQueue<TestObject>("queue/1_thread_n", 2,
            1024*1024*10, serializer, deserializer);
    
    ReadInfo* readInfo = new ReadInfo;
    readInfo->now_read = 0;
    readInfo->max_read = 1024*10;
    readInfo->diskQueue = disk_queue;
    pthread_mutex_init(&readInfo->lock, NULL);
    
    WriteInfo* writeInfo = new WriteInfo;
    writeInfo->now_write = 0;
    writeInfo->max_write = 1024*10;
    writeInfo->diskQueue = disk_queue;
    pthread_mutex_init(&writeInfo->lock, NULL);

    pthread_t read_thread;
    pthread_t write_thread[10];

    for(int i = 0; i < 10; i++) {
        if (pthread_create(&write_thread[i], NULL, write_thread_func, (void*)writeInfo) != 0) {
            printf("error\n");
            return;
        }
    }

    if (pthread_create(&read_thread, NULL, read_thread_func, (void*)readInfo) != 0) {
        printf("error\n");
        return;
    }

    for(int i = 0; i < 10; i++) {
        pthread_join(write_thread[i], NULL);
    }
    pthread_join(read_thread, NULL);

    ASSERT_EQ(1, disk_queue->empty());
    printf("now_read: %d\n", readInfo->now_read);
    ASSERT_EQ(readInfo->now_read, writeInfo->now_write);
    ASSERT_EQ(readInfo->max_read, readInfo->now_read);
    
    delete readInfo->diskQueue;
    pthread_mutex_destroy(&readInfo->lock);
    delete readInfo;

    pthread_mutex_destroy(&writeInfo->lock);
    delete writeInfo;
}

TEST(test_n_n_threads_1, DISKQUEUE_MULTI_READ_MULTI_WRITE) {
    DiskQueue<TestObject>* disk_queue = new DiskQueue<TestObject>("queue/n_thread_n", 3,
            1024*1024*10, serializer, deserializer);
    
    ReadInfo* readInfo = new ReadInfo;
    readInfo->now_read = 0;
    readInfo->max_read = 1024*10;
    readInfo->diskQueue = disk_queue;
    pthread_mutex_init(&readInfo->lock, NULL);
    
    WriteInfo* writeInfo = new WriteInfo;
    writeInfo->now_write = 0;
    writeInfo->max_write = 1024*10;
    writeInfo->diskQueue = disk_queue;
    pthread_mutex_init(&writeInfo->lock, NULL);

    pthread_t read_thread[10];
    pthread_t write_thread[10];

    for(int i = 0; i < 10; i++) {
        if (pthread_create(&write_thread[i], NULL, write_thread_func, (void*)writeInfo) < 0) {
            printf("error\n");
            return;
        }
    }

    for(int i = 0; i < 10; i++) {
        if (pthread_create(&read_thread[i], NULL, read_thread_func, (void*)readInfo) < 0) {
            printf("error\n");
            return;
        }
    }

    for(int i = 0; i < 10; i++) {
        pthread_join(write_thread[i], NULL);
    }
    for(int i = 0; i < 10; i++) {
        pthread_join(read_thread[i], NULL);
    }

    ASSERT_EQ(readInfo->now_read, writeInfo->now_write);
    ASSERT_EQ(readInfo->max_read, readInfo->now_read);
    ASSERT_EQ(1, disk_queue->empty());
    
    delete readInfo->diskQueue;
    pthread_mutex_destroy(&readInfo->lock);
    delete readInfo;

    pthread_mutex_destroy(&writeInfo->lock);
    delete writeInfo;
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
