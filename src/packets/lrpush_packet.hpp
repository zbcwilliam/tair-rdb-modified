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
#ifndef TAIR_PACKET_LPUSH_PACKET_H
#define TAIR_PACKET_LPUSH_PACKET_H
#include "base_packet.hpp"
#include <stdint.h>
#include "lrpop_packet.hpp"

namespace tair
{

  class request_lrpush:public base_packet
  {
  public:
    request_lrpush (ExistOrNot en, LeftOrRight lr)
    {
      if (en == IS_NOT_EXIST) {
	    if (lr == IS_LEFT)
	        setPCode (TAIR_REQ_LPUSH_PACKET);
	    else
	        setPCode (TAIR_REQ_RPUSH_PACKET);
	  } else {
	    if (lr == IS_LEFT)
	        setPCode (TAIR_REQ_LPUSHX_PACKET);
	    else
	        setPCode (TAIR_REQ_RPUSHX_PACKET);
	  }

      server_flag = 0;
      area = 0;
      version = 0;
      expire = 0;
      values.clear ();
    }

    request_lrpush (int pcode)
    {
      setPCode(pcode);
      server_flag = 0;
      area = 0;
      version = 0;
      expire = 0;
      values.clear();
    }

    request_lrpush (request_lrpush & packet)
    {
      setPCode (packet.getPCode ());
      server_flag = packet.server_flag;
      area = packet.area;

      version = packet.version;
      expire = packet.expire;
      key.clone (packet.key);
      clear_values ();
      size_t values_len = packet.values.size();
      for(size_t i = 0; i < values_len; i++) {
        data_entry* tmp = new data_entry();
        if(tmp != NULL) {
            tmp->clone(*(packet.values[i]));
            values.push_back(tmp);
        }
      }
    }

    ~request_lrpush ()
    {
      clear_values ();
    }

    bool encode (tbnet::DataBuffer * output)
    {
        output->writeInt8(server_flag);
        output->writeInt16(area);
        output->writeInt16(version);
        output->writeInt32(expire);

        output->writeInt32(key.get_size());
        output->writeBytes(key.get_data(), key.get_size());

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
        HEADER_VERIFY;

        GETKEY_FROM_INT16 (input, version);
        GETKEY_FROM_INT32 (input, expire);
        GETKEY_FROM_DATAENTRY (input, key);
        GETKEY_FROM_DATAVECTOR (input, values);

        key.set_version(version);

        return true;
    }

    void addValue(data_entry *value) {
        data_entry *vl = new data_entry(*value);
        values.push_back(vl);
    }

  private:
    void clear_values ()
    {
      data_entry *entry = NULL;
      for (size_t i = 0; i < values.size (); ++i) {
	    entry = values[i];
	    if (entry != NULL)
	        delete entry;
	  }
      values.clear ();
    }

  public:
    uint16_t area;
    uint16_t version;
    int expire;
    data_entry key;
    vector<data_entry *> values;
  };

  class request_lrpush_limit:public request_lrpush
  {
  public:
    request_lrpush_limit (ExistOrNot en, LeftOrRight lr)
        : request_lrpush(en, lr)
    {
      if (en == IS_NOT_EXIST) {
	    if (lr == IS_LEFT)
	        setPCode (TAIR_REQ_LPUSH_LIMIT_PACKET);
	    else
	        setPCode (TAIR_REQ_RPUSH_LIMIT_PACKET);
	  } else {
	    if (lr == IS_LEFT)
	        setPCode (TAIR_REQ_LPUSHX_LIMIT_PACKET);
	    else
	        setPCode (TAIR_REQ_RPUSHX_LIMIT_PACKET);
	  }
    }

    request_lrpush_limit (request_lrpush_limit & packet)
        : request_lrpush(packet)
    {
      max_count = packet.max_count;
    }

    ~request_lrpush_limit ()
    {
      clear_values ();
    }

    bool encode (tbnet::DataBuffer * output)
    {
        output->writeInt8(server_flag);
        output->writeInt16(area);
        output->writeInt16(version);
        output->writeInt32(expire);
        output->writeInt32(max_count);

        output->writeInt32(key.get_size());
        output->writeBytes(key.get_data(), key.get_size());

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
        HEADER_VERIFY;

        GETKEY_FROM_INT16 (input, version);
        GETKEY_FROM_INT32 (input, expire);
        GETKEY_FROM_INT32 (input, max_count);
        GETKEY_FROM_DATAENTRY (input, key);
        GETKEY_FROM_DATAVECTOR (input, values);

        key.set_version(version);

        return true;
    }

    void addValue(data_entry *value) {
        data_entry *vl = new data_entry(*value);
        values.push_back(vl);
    }

  private:
    void clear_values ()
    {
      data_entry *entry = NULL;
      for (size_t i = 0; i < values.size (); ++i) {
	    entry = values[i];
	    if (entry != NULL)
	        delete entry;
	  }
      values.clear ();
    }

  public:
    uint16_t area;
    uint16_t version;
    int expire;
    int max_count;
    data_entry key;
    vector<data_entry *> values;
  };

  class response_lrpush:public base_packet
  {
  public:
    response_lrpush (int pcode)
    {
      setPCode (pcode);
      config_version = 0;
      code = 0;

      pushed_num = 0;
      list_len = 0;
    }

    response_lrpush (ExistOrNot en, LeftOrRight lr)
    {
      if (en == IS_NOT_EXIST)
	{
	  if (lr == IS_LEFT)
	    setPCode (TAIR_RESP_LPUSH_PACKET);
	  else
	    setPCode (TAIR_RESP_RPUSH_PACKET);
	}
      else
	{
	  if (lr == IS_LEFT)
	    setPCode (TAIR_RESP_LPUSHX_PACKET);
	  else
	    setPCode (TAIR_RESP_RPUSHX_PACKET);
	}

      config_version = 0;
      code = 0;

      pushed_num = 0;
      list_len = 0;
    }

    bool encode (tbnet::DataBuffer * output)
    {
      output->writeInt32 (config_version);
      output->writeInt32 (code);

      output->writeInt32 (pushed_num);
      output->writeInt32 (list_len);

      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
        GETKEY_FROM_INT32(input, config_version);
        GETKEY_FROM_INT32(input, code);
        GETKEY_FROM_INT32(input, pushed_num);
        GETKEY_FROM_INT32(input, list_len);
        return true;
    }

    void set_meta (uint32_t config_version, uint32_t code)
    {
      this->config_version = config_version;
      this->code = code;
    }

    int get_code()
    {
      return code;
    }
	//not used
	void set_version(uint16_t version) {}
  public:
    uint32_t config_version;
    int32_t code;

    uint32_t pushed_num;
    uint32_t list_len;
  };

}				// end namespace
#endif
