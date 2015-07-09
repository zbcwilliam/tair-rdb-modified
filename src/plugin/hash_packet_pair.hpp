#ifndef __HASH_PACKET_PAIR__
#define __HASH_PACKET_PAIR__

#include "base_queue_node.hpp"
#include "data_entry.hpp"
#include "base_packet.hpp"
#include "packet_factory.hpp"

using namespace tair;

class HashPacketPair : public BaseQueueNode {
public:
    HashPacketPair() {_ret = 0; _hashcode = 0; _packet = NULL;}
    HashPacketPair(int ret, uint32_t hashcode, base_packet* packet, int nodeId = 0)
        : BaseQueueNode(nodeId) {
            _ret = ret;
            _hashcode = hashcode;
            _packet = packet;
        }
    ~HashPacketPair() {};

    void set_ret(int ret) {
        _ret = ret;
    }

    void set_hashcode(uint32_t hashcode) {
        _hashcode = hashcode;
    }

    void set_packet(base_packet* packet) {
        if (_packet) {
            delete _packet;
        }
        _packet = packet;
    }

    int get_ret() {return _ret;}
    uint32_t get_hashcode() {return _hashcode;}
    base_packet* get_packet() {return _packet;}
public:
    static bool hp_serialize_func_(BaseQueueNode* data, tbnet::DataBuffer** dataBuffer);
    static bool hp_deserialize_func_(BaseQueueNode** data, tbnet::DataBuffer* dataBuffer);
private:
    int _ret;
    uint32_t _hashcode;
    base_packet* _packet;
};

bool HashPacketPair::hp_serialize_func_(BaseQueueNode* data, tbnet::DataBuffer** dataBuffer) {
    if (data == NULL) {
        return false;
    }

    (*dataBuffer) = new tbnet::DataBuffer();

    HashPacketPair* pair = (HashPacketPair*)data;
    int ret = pair->get_ret();
    (*dataBuffer)->writeInt32(ret);
    long long id = pair->get_node_id();
    (*dataBuffer)->writeInt64(id);
    uint32_t hashcode = pair->get_hashcode();
    (*dataBuffer)->writeInt32(hashcode);
    tbnet::Packet* packet = pair->get_packet();
    if (packet != NULL) {
        int pcode = packet->getPCode();
        (*dataBuffer)->writeInt32(pcode);
        packet->encode((*dataBuffer));
    }

    return true;
}

bool HashPacketPair::hp_deserialize_func_(BaseQueueNode** data, tbnet::DataBuffer* dataBuffer) {
    if (dataBuffer == NULL) {
        return false;
    }

    HashPacketPair* pair = new HashPacketPair();
    (*data) = pair;

    int ret = dataBuffer->readInt32();
    long long id = dataBuffer->readInt64();
    uint32_t hashcode = dataBuffer->readInt32();

    int pcode = dataBuffer->readInt32();
    tbnet::Packet* packet = tair_packet_factory::_createPacket(pcode);
    if (packet == NULL) {
        delete pair;
        return false;
    }

    pair->set_ret(ret);
    pair->set_node_id(id);
    pair->set_hashcode(hashcode);
    pair->set_packet((base_packet*)packet);

    return true;
}

#endif
