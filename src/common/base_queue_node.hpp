#ifndef __BASE_QUEUE_NODE__
#define __BASE_QUEUE_NODE__

class BaseQueueNode {
public:
    BaseQueueNode() {}
    BaseQueueNode(long long nodeId) {
        _node_id = nodeId;
    }
    ~BaseQueueNode() {}
    void set_node_id(long long nodeId) {_node_id = nodeId;}
    long long get_node_id() {return _node_id;}
private:
    long long _node_id;
};

#endif
