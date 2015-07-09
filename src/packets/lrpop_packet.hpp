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
#ifndef TAIR_PACKET_LPOP_PACKET_H
#define TAIR_PACKET_LPOP_PACKET_H
#include "base_packet.hpp"
#include <stdint.h>

#include "storage_manager.hpp"

namespace tair
{


  class request_lrpop:public base_packet
  {
  public:

    request_lrpop (LeftOrRight lr)
    {
      if (lr == IS_LEFT)
	    setPCode (TAIR_REQ_LPOP_PACKET);
      else
	    setPCode (TAIR_REQ_RPOP_PACKET);

      server_flag = 0;
      area = 0;
      version = 0;
      expire = 0;
      count = 1;
    }

    request_lrpop (int pcode)
    {
        setPCode (pcode);
        server_flag = 0;
        area = 0;
        version = 0;
        expire = 0;
        count = 1;
    }

    request_lrpop (request_lrpop & packet)
    {
      setPCode (packet.getPCode ());
      server_flag = packet.server_flag;
      area = packet.area;

      version = packet.version;
      expire = packet.expire;
      count = packet.count;
      key.clone (packet.key);
    }

    bool encode (tbnet::DataBuffer * output)
    {
        output->writeInt8(server_flag);
        output->writeInt16(area);
        output->writeInt16(version);
        output->writeInt32(expire);
        output->writeInt32(count);
        output->writeInt32(key.get_size());
        output->writeBytes(key.get_data(), key.get_size());
        return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
        HEADER_VERIFY;

        GETKEY_FROM_INT16 (input, version);
        GETKEY_FROM_INT32 (input, expire);
        GETKEY_FROM_INT32 (input, count);
        GETKEY_FROM_DATAENTRY (input, key);

        key.set_version(version);

        return true;
    }

    void setCount(int cnt)
    {
        count = cnt;
    }
  public:
    uint16_t area;
    uint16_t version;
    int32_t expire;
    data_entry key;
    int count;
  };

  class response_lrpop:public base_packet
  {
#define RESPONSE_VALUES_MAXSIZE 32767
  public:
    response_lrpop (int pcode)
    {
      setPCode (pcode);
      config_version = 0;
      values.clear ();
    }

    response_lrpop (LeftOrRight lr)
    {
      if (lr == IS_LEFT) {
	    setPCode (TAIR_RESP_LPOP_PACKET);
      } else {
	    setPCode (TAIR_RESP_RPOP_PACKET);
      }

      config_version = 0;
      values.clear ();
    }

    ~response_lrpop () {
      for (size_t i = 0; i < values.size (); ++i) {
	data_entry *entry = values[i];
	if (entry != NULL)
	  delete (entry);
      }
      values.clear ();
    }

    bool encode (tbnet::DataBuffer * output)
    {
      if (values.size () > RESPONSE_VALUES_MAXSIZE)
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
      return true;
    }

    void set_meta (uint32_t config_version, uint32_t code)
    {
      this->config_version = config_version;
      this->code = code;
    }

    void set_version(uint16_t version)
    {
        this->version = version;
    }

    void add_data(data_entry * data)
    {
      if (data == NULL)
	    return;
      values.push_back (data);
    }

    int get_code()
    {
        return code;
    }
  public:
    uint32_t config_version;
    uint16_t version;
    int32_t code;
    vector<data_entry *> values;
  };

}				// end namespace
#endif
