/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * hmset packet
 *
 * Version: $Id: hmset_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_HMSET_PACKET_H
#define TAIR_PACKET_HMSET_PACKET_H
#include "base_packet.hpp"
namespace tair
{
  class request_hmset:public base_packet
  {
  public:
    request_hmset()
    {
      setPCode(TAIR_REQ_HMSET_PACKET);
      server_flag = 0;
      area = 0;
      version = 0;
      sfree = 1;
      expire = 0;
    }

    request_hmset(const uint16_t iarea, const uint16_t iversion, const int iexpire,
            const data_entry &ikey, const vector<data_entry*> &ifield_values,
            const int ifree) {
      setPCode(TAIR_REQ_HMSET_PACKET);
      server_flag = 0;
      area = iarea;
      version = iversion;
      expire = iexpire;
      sfree = ifree;
      key = ikey;
      field_values = ifield_values;
    }

    request_hmset(request_hmset & packet)
    {
      setPCode (packet.getPCode ());
      server_flag = packet.server_flag;
      area = packet.area;
      
      version = packet.version;
      expire = packet.expire;
      key.clone (packet.key);
      CLEAR_DATA_VECTOR(field_values, sfree);
      sfree = 1;
      size_t packet_field_values_len = packet.field_values.size();
      for(size_t i = 0; i < packet_field_values_len; i++) {
        data_entry* tmp = new data_entry();
        if(tmp != NULL) {
            tmp->clone(*(packet.field_values[i]));
            field_values.push_back(tmp);
        }
      }
    }

    ~request_hmset()
    {
      CLEAR_DATA_VECTOR(field_values, sfree);
    }

    bool encode (tbnet::DataBuffer * output)
    {
      CREATE_HEADER;

      PUT_INT16_TO_BUFFER(output, version);
      PUT_INT32_TO_BUFFER(output, expire);
      PUT_DATAENTRY_TO_BUFFER(output, key);
      PUT_DATAVECTOR_TO_BUFFER(output, field_values);

      return true;
    }

    bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
    {
      HEADER_VERIFY;

      GETKEY_FROM_INT16 (input, version);
      GETKEY_FROM_INT32 (input, expire);
      GETKEY_FROM_DATAENTRY (input, key);
      GETKEY_FROM_DATAVECTOR (input, field_values);

      key.set_version(version);
      return true;
    }
  public:
    uint16_t area;
    uint16_t version;
    int expire;
    data_entry key;
    vector <data_entry *> field_values;
  private:
    int sfree;
  };

  class response_hmset:public base_packet
  {
  public:

    response_hmset ()
    {
      config_version = 0;
      setPCode (TAIR_RESP_HMSET_PACKET);
      code = 0;
      retvalue = 0;
    }

    bool encode (tbnet::DataBuffer * output)
    {

      output->writeInt32 (config_version);
      output->writeInt32 (code);
      output->writeInt32 (retvalue);
      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      GETKEY_FROM_INT32(input, config_version);
      GETKEY_FROM_INT32(input, code);
      GETKEY_FROM_INT32(input, retvalue);
      return true;
    }

    void set_meta (uint32_t config_version, uint32_t code)
    {
      this->config_version = config_version;
      //this->version           = version;
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
	//void set_version(uint16_t version) {}  //commented 6.26
	void set_version(uint16_t version)
    {
        this->version = version;
    }
  public:
    uint32_t config_version;
	uint16_t version;                   //added 6.26
    int retvalue;
  private:
    int32_t code;
    response_hmset (const response_hmset &);
  };
}
#endif
