/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * get packet
 *
 * Version: $Id: get_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_SMEMBERS_PACKET_H
#define TAIR_PACKET_SMEMBERS_PACKET_H
#include "base_packet.hpp"
#include <stdint.h>

#include "storage_manager.hpp"

#include <sstream>
#include <iomanip>

namespace tair
{

  std::string hexStr(char *data, int len);

  class request_smembers:public base_packet
  {
  public:
    request_smembers()
    {
      setPCode (TAIR_REQ_SMEMBERS_PACKET);
      server_flag = 0;
      area = 0;
    }

    request_smembers(const uint16_t iarea, const data_entry &ikey)
    {
      setPCode(TAIR_REQ_SMEMBERS_PACKET);
      server_flag = 0;
      area = iarea;
      key = ikey;
    }

    request_smembers(request_smembers & packet)
    {
      setPCode(packet.getPCode());
      server_flag = packet.server_flag;
      area = packet.area;
      key.clone(packet.key);
    }

    bool encode(tbnet::DataBuffer * output)
    {
      CREATE_HEADER;
      PUT_DATAENTRY_TO_BUFFER(output, key);
      return true;
    }

    bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
    {
      HEADER_VERIFY;
      GETKEY_FROM_DATAENTRY(input, key);
      log_debug("et_smembers::decode key=%s",hexStr(key.get_data(),key.get_size()).c_str());
      return true;
    }

  public:
    uint16_t area;
    data_entry key;
  };

  class request_smembers_multi:public base_packet
  {
  public:
    request_smembers_multi()
    {
      setPCode(TAIR_REQ_SMEMBERSMULTI_PACKET);
      server_flag = 0;
      area = 0;
      keys.clear();
      sfree = 1;
    }

    request_smembers_multi(const uint16_t iarea,
            const vector<data_entry*> &ikeys)
    {
      setPCode(TAIR_REQ_SMEMBERSMULTI_PACKET);
      server_flag = 0;
      area = iarea;
      keys = ikeys;
      sfree = 0;
    }

    ~request_smembers_multi()
    {
      CLEAR_DATA_VECTOR(keys, sfree);
    }

    request_smembers_multi(request_smembers_multi &packet)
    {
      setPCode(packet.getPCode());
      server_flag = packet.server_flag;
      area = packet.area;
      keys = packet.keys;
      assert(0);
    }
    
    bool encode(tbnet::DataBuffer *output)
    {
      CREATE_HEADER;

      PUT_DATAVECTOR_TO_BUFFER(output, keys);

      return true;
    }

    bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
    {
      HEADER_VERIFY;

      GETKEY_FROM_DATAVECTOR(input, keys);

      return true;
    }

  public:
    uint16_t area;
    vector<data_entry*> keys;
  private:
    int sfree;
  };

  class response_smembers:public base_packet
  {
#define RESPONSE_VALUES_MAXSIZE 32767
  public:
    response_smembers ()
    {
      setPCode (TAIR_RESP_SMEMBERS_PACKET);
      sfree = 1;
      config_version = 0;
    }

    ~response_smembers () {
      CLEAR_DATA_VECTOR(values, sfree);
    }

    bool encode(tbnet::DataBuffer * output)
    {
      if (values.size() > RESPONSE_VALUES_MAXSIZE)
      {
    	return false;
      }
      output->writeInt32 (config_version);
      output->writeInt16 (version);
      output->writeInt32 (code);
      output->writeInt32 (values.size ());
      data_entry *entry = NULL;
      for (size_t i = 0; i < values.size (); ++i)
      {
    	entry = values[i];
    	output->writeInt32 (entry->get_size ());
    	output->writeBytes (entry->get_data (), entry->get_size ());
      }
      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      GETKEY_FROM_INT32(input, config_version); 
      GETKEY_FROM_INT16(input, version);
      GETKEY_FROM_INT32(input, code);
      GETKEY_FROM_DATAVECTOR(input, values);

      for(size_t i = 0; i < values.size(); i++) {
          values[i]->set_version(version);
      }
      return true;
    }

    void set_meta(uint32_t config_version, uint32_t code)
    {
      this->config_version = config_version;
      this->code = code;
    }

    void set_version(uint16_t version)
    {
        this->version = version;
    }

    void add_data(data_entry *data)
    {
      if (data == NULL)
	    return;
      values.push_back(data);
    }

    int get_code()
    {
      return code;
    }
 
    void alloc_free(int ifree)
    {
      sfree = ifree;
    }
  public:
    uint32_t config_version;
    uint32_t code;
    uint16_t version;
    vector<data_entry *> values;
    int sfree;
  };

  class response_smembers_multi:public base_packet
  {
  public:
    response_smembers_multi()
    {
      setPCode(TAIR_RESP_SMEMBERSMULTI_PACKET);
      config_version = 0;
      values_version_vec.clear();
      sfree = 1;
      value_num = 0;
    }

    ~response_smembers_multi()
    {
       pair<uint16_t, vector<data_entry*>* >* tmp_pair;
       vector<data_entry*>* tmp_values;
       size_t values_version_vec_len = values_version_vec.size();
       for(size_t i = 0; i < values_version_vec_len; i++)
       {
         tmp_pair = values_version_vec[i];
         if (tmp_pair != NULL)
         {
           if (tmp_values != NULL)
           {
             tmp_values = tmp_pair->second;
             CLEAR_DATA_VECTOR(*tmp_values, sfree);
             delete tmp_values;
           }
           delete tmp_pair;
         }
       }
       values_version_vec.clear();
    }

    bool encode(tbnet::DataBuffer * output)
    {
      if (value_num > RESPONSE_VALUES_MAXSIZE)
      {
    	return false;
      }
      PUT_INT32_TO_BUFFER(output, config_version);
      PUT_INT32_TO_BUFFER(output, code);
      
      pair<uint16_t, vector<data_entry*>*>* tmp_pair;
      int values_version_vec_len = (int)(values_version_vec.size());
      PUT_INT32_TO_BUFFER(output, values_version_vec_len);
      for(int i = 0; i < values_version_vec_len; i++)
      {
        tmp_pair = values_version_vec[i];
        PUT_INT16_TO_BUFFER(output, tmp_pair->first);
        PUT_DATAVECTOR_TO_BUFFER(output, *(tmp_pair->second)); 
      }
      
      return true;
    }

    bool decode(tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      GETKEY_FROM_INT32(input, config_version);
      GETKEY_FROM_INT32(input, code);

      uint16_t tmp_version;
      pair<uint16_t, vector<data_entry*>*>* tmp_pair;
      vector<data_entry*>* tmp_values;
      int values_version_vec_len;
      GETKEY_FROM_INT32(input, values_version_vec_len);
      for(int i = 0; i < values_version_vec_len; i++)
      {
        GETKEY_FROM_INT16(input, tmp_version);
        tmp_values = new vector<data_entry*>();
        if (tmp_values == NULL)
        {
          return false;
        }
        tmp_pair = new pair<uint16_t, vector<data_entry*>*>(tmp_version, tmp_values);
        if (tmp_pair == NULL)
        {
          return false;
        }
        values_version_vec.push_back(tmp_pair);
        GETKEY_FROM_DATAVECTOR(input, (*tmp_values));
      }

      return true;
    }

    void set_meta(uint32_t config_version, uint32_t code)
    {
      this->config_version = config_version;
      this->code = code;
    }

    int get_code()
    {
      return code;
    }

    void alloc_free(bool ifree)
    {
      sfree = ifree;
    }

    bool add_version_values_pair(uint16_t version, vector<data_entry*>* values)
    {
      pair<uint16_t, vector<data_entry*>*>* tmp_pair =
          new pair<uint16_t, vector<data_entry*>*>(version, values);
      if (tmp_pair == NULL)
      {
        return false;
      }
      values_version_vec.push_back(tmp_pair);
      value_num++;
      return true;
    }

    void set_version(uint16_t version) {}
    uint16_t get_version() {return 0;}
  public:
    uint32_t config_version;
    int32_t code;
    vector<pair<uint16_t, vector<data_entry*>*>* > values_version_vec;
    int value_num;
  private:
    int sfree;
  };
}				// end namespace 
#endif
