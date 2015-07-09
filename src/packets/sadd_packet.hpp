/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * sadd packet
 *
 * Version: $Id: sadd_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_SADD_PACKET_H
#define TAIR_PACKET_SADD_PACKET_H
#include "base_packet.hpp"
namespace tair
{
  class request_sadd:public base_packet
  {
  public:
    request_sadd()
    {
      setPCode(TAIR_REQ_SADD_PACKET);
      server_flag = 0;
      area = 0;
      version = 0;
      expire = 0;
    }

    request_sadd(const uint16_t iarea, const uint16_t iversion,
            const int32_t iexpire, const data_entry &ikey,
            const data_entry &ivalue)
    {
      setPCode(TAIR_REQ_SADD_PACKET);
      server_flag = 0;
      area = iarea;
      version = iversion;
      expire = iexpire;
      key = ikey;
      value = ivalue;
    }

    request_sadd(request_sadd & packet)
    {
      setPCode(packet.getPCode());
      server_flag = packet.server_flag;
      area = packet.area;

      version = packet.version;
      expire = packet.expire;
      key.clone(packet.key);
      value.clone(packet.value);
    }

    bool encode(tbnet::DataBuffer * output)
    {
      CREATE_HEADER;

      PUT_INT16_TO_BUFFER(output, version);
      PUT_INT32_TO_BUFFER(output, expire);
      PUT_DATAENTRY_TO_BUFFER(output, key);
      PUT_DATAENTRY_TO_BUFFER(output, value);

      return true;
    }

    bool decode(tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {

      HEADER_VERIFY;

      GETKEY_FROM_INT16 (input, version);
      GETKEY_FROM_INT32 (input, expire);
      GETKEY_FROM_DATAENTRY (input, key);
      GETKEY_FROM_DATAENTRY (input, value);

      key.set_version(version);

      return true;
    }

  public:
    uint16_t area;
    uint16_t version;
    int32_t expire;
    data_entry key;
    data_entry value;
  };

  class request_sadd_multi:public base_packet
  {
  public:
    request_sadd_multi() {
        setPCode(TAIR_REQ_SADDMULTI_PACKET);
        area = 0;
        expire = 0;
        keys_values_map.clear();
        sfree = 1;
    }

    request_sadd_multi(const uint16_t iarea, const int32_t iexpire,
            const map<data_entry*, vector<data_entry*> *> &ikeys_values_map, bool ifree)
    {
        setPCode(TAIR_REQ_SADDMULTI_PACKET);
        server_flag = 0;
        area = iarea;
        expire = iexpire;
        keys_values_map = ikeys_values_map;
        sfree = ifree;
    }

    request_sadd_multi(const uint16_t iarea, const int32_t iexpire,
            const data_entry* key, const vector<data_entry*>* values)
    {
        setPCode(TAIR_REQ_SADDMULTI_PACKET);
        server_flag = 0;
        sfree = true;
        area = iarea;
        expire = iexpire;

        if (key != NULL && values != NULL) {
            data_entry *tmp_key = new data_entry();
            tmp_key->clone(*key);
            vector<data_entry *> *tmp_values = new vector<data_entry *>();
            for(size_t i = 0; i < values->size(); i++) {
                data_entry *tmp_value = new data_entry();
                tmp_value->clone(*((*values)[i]));
                tmp_values->push_back(tmp_value);
            }
            keys_values_map.insert(make_pair(tmp_key, tmp_values));
        }
    }

    ~request_sadd_multi()
    {
        map<data_entry*, vector<data_entry*>* >::iterator iter;
        for(iter = keys_values_map.begin();
                iter != keys_values_map.end(); iter++) {
            data_entry* key = iter->first;
            vector<data_entry*>* values = iter->second;
            delete key;
            CLEAR_DATA_VECTOR(*values, sfree);
            delete values;
        }
    }

    request_sadd_multi(request_sadd_multi & packet)
    {
      setPCode(packet.getPCode());
      server_flag = packet.server_flag;
      area = packet.area;

      expire = packet.expire;

      sfree = packet.sfree;

      keys_values_map.clear();
      map<data_entry*, vector<data_entry *>*>::iterator iter;
      for(iter = packet.keys_values_map.begin();
              iter != keys_values_map.end(); iter++) {
          data_entry* key = iter->first;
          vector<data_entry*>* values = iter->second;

          data_entry* ckey = new data_entry();
          ckey->clone(*key);

          vector<data_entry*>* cvalues = new vector<data_entry*>();
          if (values != NULL) {
              int size = values->size();
              for(int i = 0; i < size; i++) {
                  data_entry* tmp = new data_entry();
                  tmp->clone(*tmp);
                  cvalues->push_back(tmp);
              }
          }

          keys_values_map.insert(make_pair(ckey, cvalues));
      }
    }

    const data_entry* get_random_key()
    {
      map<data_entry*, vector<data_entry*>* >::iterator iter;
      for(iter = keys_values_map.begin();
              iter != keys_values_map.end(); iter++) {
        return iter->first;
      }
      return NULL;
    }

    bool encode(tbnet::DataBuffer * output)
    {
      CREATE_HEADER;

      PUT_INT32_TO_BUFFER(output, expire);

      int keys_values_map_size = (int)(keys_values_map.size());
      PUT_INT32_TO_BUFFER(output, keys_values_map_size);
      map<data_entry*, vector<data_entry *>*>::iterator iter;
      for(iter = keys_values_map.begin();
              iter != keys_values_map.end(); iter++) {
        data_entry* key = iter->first;
        vector<data_entry*>* values = iter->second;
        PUT_DATAENTRY_TO_BUFFER(output, *key);
        PUT_DATAVECTOR_TO_BUFFER(output, *values);
      }

      return true;
    }

    bool decode(tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      HEADER_VERIFY;

      GETKEY_FROM_INT32(input, expire);

      int keys_values_map_size;
      GETKEY_FROM_INT32(input, keys_values_map_size);


      for(int i = 0; i < keys_values_map_size; i++) {
        data_entry* key = new data_entry();
        vector<data_entry *>* values = new vector<data_entry *>();
        keys_values_map.insert(make_pair(key, values));
        GETKEY_FROM_DATAENTRY(input, *key);
        GETKEY_FROM_DATAVECTOR(input, *values);
      }

      return true;
    }

  public:
    uint16_t area;
    int32_t expire;
    map<data_entry*, vector<data_entry*>* > keys_values_map;
  private:
    int sfree;
  };

  class response_sadd:public base_packet
  {
  public:

    response_sadd ()
    {
      config_version = 0;
      setPCode (TAIR_RESP_SADD_PACKET);
      code = 0;
    }

    bool encode (tbnet::DataBuffer * output)
    {
      output->writeInt32 (config_version);
      output->writeInt32 (code);

      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      GETKEY_FROM_INT32(input, config_version);
      GETKEY_FROM_INT32(input, code);
      return true;
    }

    void set_meta (uint32_t config_version, uint32_t code)
    {
      this->config_version = config_version;
      this->code = code;
    }

    void set_code (int cde)
    {
      code = cde;
    }

    int get_code ()
    {
      return code;
    }

	//not used
	void set_version(uint16_t version) {}
  public:
    uint32_t config_version;
  private:
    int32_t code;
    response_sadd (const response_sadd &);
  };
}
#endif
