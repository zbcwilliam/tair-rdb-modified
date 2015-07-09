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
#ifndef TAIR_PACKET_HGETALL_PACKET_H
#define TAIR_PACKET_HGETALL_PACKET_H
#include "base_packet.hpp"
#include <stdint.h>

#include "storage_manager.hpp"

namespace tair
{


  class request_hgetall:public base_packet
  {
  public:
    request_hgetall()
    {
      setPCode(TAIR_REQ_HGETALL_PACKET);
      server_flag = 0;
      area = 0;
    }

    request_hgetall(const int iarea, const data_entry &ikey)
    {
      setPCode(TAIR_REQ_HGETALL_PACKET);
      server_flag = 0;
      area = iarea;
      key = ikey;
    }

    request_hgetall(request_hgetall & packet)
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

    bool decode(tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {  
      HEADER_VERIFY;
      GETKEY_FROM_DATAENTRY (input, key);
      return true;
    }

  public:
    uint16_t area;
    data_entry key;
  };

  class response_hgetall:public base_packet
  {
#define RESPONSE_VALUES_MAXSIZE 32767
  public:
    response_hgetall()
    {
      setPCode (TAIR_RESP_HGETALL_PACKET);
      config_version = 0;
      version = 0;
      sfree = 1;
    }

    ~response_hgetall()
    {
      CLEAR_DATA_VECTOR(values, sfree);
    }

    bool encode (tbnet::DataBuffer * output)
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

    bool decode(tbnet::DataBuffer * input, tbnet::PacketHeader * header)
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

    void set_version(uint16_t version)
    {
        this->version = version;
    }

    void set_meta (uint32_t config_version, uint32_t code)
    {
      this->config_version = config_version;
      this->code = code;
    }

    void add_data (data_entry * data)
    {
      if (data == NULL)
	    return;
      values.push_back (data);
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
    uint16_t version;
    int32_t  code;
    vector<data_entry *> values;
  private:
    int sfree;
  };

}				// end namespace 
#endif
