/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * hmget packet
 *
 * Version: $Id: hmget_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_HMGET_PACKET_H
#define TAIR_PACKET_HMGET_PACKET_H
#include "base_packet.hpp"
namespace tair
{
  class request_hmget:public base_packet
  {
  public:
    request_hmget()
    {
      setPCode(TAIR_REQ_HMGET_PACKET);
      server_flag = 0;
      area = 0;
      sfree = 1;
    }

    request_hmget(const uint16_t iarea, const data_entry &ikey,
            const vector<data_entry *> &ifields, const int ifree)
    {
      setPCode(TAIR_REQ_HMGET_PACKET);
      server_flag = 0;
      area = iarea;
      key = ikey;
      sfree = ifree;
      fields = ifields;
    }

    request_hmget(request_hmget & packet)
    {
      setPCode(packet.getPCode ());
      server_flag = packet.server_flag;
      area = packet.area;
      key.clone(packet.key);
      CLEAR_DATA_VECTOR(fields, sfree);
      sfree = 0;
      fields = packet.fields;
    }

    ~request_hmget()
    {
      CLEAR_DATA_VECTOR(fields, sfree);
    }

    bool encode (tbnet::DataBuffer * output)
    {
      CREATE_HEADER;
      PUT_DATAENTRY_TO_BUFFER(output, key);
      PUT_DATAVECTOR_TO_BUFFER(output, fields);

      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      HEADER_VERIFY;
      GETKEY_FROM_DATAENTRY (input, key);
      GETKEY_FROM_DATAVECTOR (input, fields);
      return true;
    }
  public:
    uint16_t area;
    data_entry key;
    vector<data_entry *> fields;
  private:
    int sfree;
  };

  class response_hmget:public base_packet
  {
  public:
    response_hmget()
    {
      config_version = 0;
      setPCode(TAIR_RESP_HMGET_PACKET);
      version = 0;
      code = 0;
      sfree = 1;
    }

    ~response_hmget ()
    {
      CLEAR_DATA_VECTOR(values, sfree);
    }

    bool encode (tbnet::DataBuffer * output)
    {
      output->writeInt32 (config_version);
      output->writeInt16 (version);
      output->writeInt32 (code);
      output->writeInt32 (values.size());
      data_entry *entry = NULL;
      for (size_t i = 0; i < values.size(); i++)
      {
	    entry = values[i];
	    output->writeInt32 (entry->get_size());
	    output->writeBytes (entry->get_data(), entry->get_size());
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

    void set_version(uint16_t version)
    {
        this->version = version;
    }

    void alloc_free(int ifree)
    {
        sfree = ifree;
    }
  public:
    uint32_t config_version;
    uint16_t version;
    vector<data_entry *> values;
  private:
    int sfree;
    int32_t code;
    response_hmget (const response_hmget &);
  };
}
#endif
