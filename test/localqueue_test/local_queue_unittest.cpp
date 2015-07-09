#include <gtest/gtest.h>
#include <queue>
#include <sys/time.h>
#include <unistd.h>
#include <iostream>
#include "local_queue.hpp"

#include "base_queue_node.hpp"
#include "test_queue_node.hpp"

typedef struct ReadInfo {
    int max_read;
    int now_read;
    pthread_mutex_t lock;
    LocalQueue* localQueue;
} ReadInfo;

typedef struct WriteInfo {
    int max_write;
    int now_write;
    pthread_mutex_t lock;
    LocalQueue* localQueue;
} WriteInfo;

void* read_thread_func(void* argv) {
    ReadInfo* readInfo = (ReadInfo*)argv;
    TestQueueNode* testObject = NULL;
    while(true) {
        int isOk = readInfo->localQueue->pop((BaseQueueNode**)&testObject);
        if (!isOk) {
            if (readInfo->now_read >= readInfo->max_read) {
                break;
            }
            continue;
        }
        readInfo->now_read++;
        if (readInfo->now_read >= readInfo->max_read) {
            break;
        }
    }
    printf("read over\n");
    return NULL;
}

void* write_thread_func(void* argv) {
    WriteInfo* writeInfo = (WriteInfo*)argv;

    struct timeval tv_start, tv_end;
    struct timezone tz_start, tz_end;
    gettimeofday(&tv_start, &tz_start);

    while(true) {
        if (writeInfo->now_write >= writeInfo->max_write) {
            break;
        }
        if (writeInfo->now_write < writeInfo->max_write) { 
            TestQueueNode* testObject = TestQueueNode::createRandomOne(1024,1);
            int isOk = writeInfo->localQueue->push(testObject);
            EXPECT_EQ(1, isOk);
        }
        if (writeInfo->now_write >= writeInfo->max_write) {
            break;
        }
        writeInfo->now_write++;
    }

    gettimeofday(&tv_end, &tz_end);
    printf("time %ld\n", (tv_end.tv_sec * 1000000 + tv_end.tv_usec)
            - (tv_start.tv_sec * 1000000 + tv_start.tv_usec));

    printf("write over\n");
    return NULL;
}

TEST(test_simple_1, DISKQUEUE_ONE_READ_ONE_WRITE) {
    LocalQueue local_queue(TestQueueNode::test_serialize_func_,
            TestQueueNode::test_deserialize_func_);
    int i;
    std::queue<TestQueueNode*> TestQueueNodeQueue;
    for(i = 0; i < 100; i++) {
        TestQueueNode* tmp = TestQueueNode::createRandomOne(1024, 0);
        TestQueueNode* tmp1 = tmp->copy();
        TestQueueNodeQueue.push(tmp1);
        ASSERT_EQ(1, local_queue.push(tmp));
    }

    while(!TestQueueNodeQueue.empty()) {
        TestQueueNode* tmp = TestQueueNodeQueue.front();
        TestQueueNodeQueue.pop();
        TestQueueNode* tmp2 = NULL;
        ASSERT_EQ(1, local_queue.pop((BaseQueueNode**)&tmp2));
        ASSERT_STREQ(tmp->getData(), tmp2->getData());
        delete tmp;
        delete tmp2;
    }
}

TEST(test_large_unvisibe_char_1, DISKQUEUE_ONE_READ_ONE_WRITE) {
    LocalQueue local_queue(TestQueueNode::test_serialize_func_,
            TestQueueNode::test_deserialize_func_);

    int i;
    std::queue<TestQueueNode*> TestQueueNodeQueue;
    for(i = 0; i < 1024*10; i++) {//10M * 10
        TestQueueNode* tmp = TestQueueNode::createRandomOne(1024, 1);
        TestQueueNode* tmp1 = tmp->copy();
        TestQueueNodeQueue.push(tmp1);
        ASSERT_EQ(1, local_queue.push(tmp));
    }

    int index = 0;
    while(!TestQueueNodeQueue.empty()) {
        TestQueueNode* tmp = TestQueueNodeQueue.front();
        TestQueueNodeQueue.pop();
        TestQueueNode* tmp2 = NULL;

        ASSERT_EQ(1, local_queue.pop((BaseQueueNode**)&tmp2));
        
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
    LocalQueue* local_queue = new LocalQueue(TestQueueNode::test_serialize_func_,
            TestQueueNode::test_deserialize_func_);
    
    ReadInfo* readInfo = new ReadInfo;
    readInfo->now_read = 0;
    readInfo->max_read = 1024*10;
    readInfo->localQueue = local_queue;
    pthread_mutex_init(&readInfo->lock, NULL);
    
    WriteInfo* writeInfo = new WriteInfo;
    writeInfo->now_write = 0;
    writeInfo->max_write = 1024*10;
    writeInfo->localQueue = local_queue;
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

    delete readInfo->localQueue;
    pthread_mutex_destroy(&readInfo->lock);
    delete readInfo;

    pthread_mutex_destroy(&writeInfo->lock);
    delete writeInfo;
}

TEST(test_2_threads_1i_large, DISKQUEUE_ONE_READ_ONE_WRITE) {
    LocalQueue* local_queue = new LocalQueue(TestQueueNode::test_serialize_func_,
            TestQueueNode::test_deserialize_func_);
    
    ReadInfo* readInfo = new ReadInfo;
    readInfo->now_read = 0;
    readInfo->max_read = 1024*1000;
    readInfo->localQueue = local_queue;
    pthread_mutex_init(&readInfo->lock, NULL);
    
    WriteInfo* writeInfo = new WriteInfo;
    writeInfo->now_write = 0;
    writeInfo->max_write = 1024*1000;
    writeInfo->localQueue = local_queue;
    pthread_mutex_init(&writeInfo->lock, NULL);

    pthread_t read_thread;
    pthread_t write_thread;
    
    struct timeval tv_start, tv_end;
    struct timezone tz_start, tz_end;
    gettimeofday(&tv_start, &tz_start);

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

    gettimeofday(&tv_end, &tz_end);
    printf("time %ld\n", (tv_end.tv_sec * 1000000 + tv_end.tv_usec)
            - (tv_start.tv_sec * 1000000 + tv_start.tv_usec));

    delete readInfo->localQueue;
    pthread_mutex_destroy(&readInfo->lock);
    delete readInfo;

    pthread_mutex_destroy(&writeInfo->lock);
    delete writeInfo;
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
