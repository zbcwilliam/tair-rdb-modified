#ifndef __TEST_QUEUE_NODE__
#define __TEST_QUEUE_NODE__

#include "base_queue_node.hpp"

class TestQueueNode : public BaseQueueNode {
public:
    TestQueueNode(int nodeId, char* data, int data_len)
        : BaseQueueNode(nodeId) {
            _data = data;
            _data_len = data_len;
        }
    ~TestQueueNode() {};

    char* getData() {return _data;}
    int getDataLen() {return _data_len;}
    static TestQueueNode* createRandomOne(int data_len, int isallrandom);
    static char* getRandomString(int len, int isallrandom);
    TestQueueNode* copy();

public:
    static bool test_serialize_func_(BaseQueueNode* data, tbnet::DataBuffer** dataBuffer);
    static bool test_deserialize_func_(BaseQueueNode** data, tbnet::DataBuffer* dataBuffer);
private:
    char* _data;
    int _data_len;
};

bool TestQueueNode::test_serialize_func_(BaseQueueNode* data, tbnet::DataBuffer** dataBuffer) {
    TestQueueNode* node = (TestQueueNode*)data;
    char* t_value = node->getData();
    int t_value_len = node->getDataLen();
    int t_id = node->get_node_id();

    tbnet::DataBuffer* buffer = new tbnet::DataBuffer();
    buffer->writeInt32(t_value_len);
    buffer->writeBytes(t_value,t_value_len);
    buffer->writeInt32(t_id);

    (*dataBuffer) = buffer;
    return true;
}

bool TestQueueNode::test_deserialize_func_(BaseQueueNode** data, tbnet::DataBuffer* dataBuffer) {
    int t_value_len = dataBuffer->readInt32();
    char* buffer = new char[t_value_len];
    dataBuffer->readBytes(buffer, t_value_len);
    int t_id = dataBuffer->readInt32();
    (*data) = new TestQueueNode(t_id, buffer, t_value_len);

    return true;
}

TestQueueNode* TestQueueNode::copy() {
    int data_len = getDataLen();
    char* data = (char*)malloc(sizeof(char) * data_len);
    memcpy(data, getData(), data_len);

    int t_id = get_node_id();

    return new TestQueueNode(t_id, data, data_len);
}

TestQueueNode* TestQueueNode::createRandomOne(int data_len,
        int isallrandom) {
    char* data = getRandomString(data_len, isallrandom);
    TestQueueNode* node = new TestQueueNode(0, data, data_len);
    return node;
}


char* TestQueueNode::getRandomString(int len, int isallrandom) {
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

#endif
