/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * lindex packet
 *
 * Version: $Id: lindex_packet.hpp 28 2011-10-22 05:18:09Z choutian.xmm@taobao.com $
 *
 * Authors:
 *   ruohai <choutian.xmm@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_LINDEX_PACKET_H
#define TAIR_PACKET_LINDEX_PACKET_H
#include "base_packet.hpp"
namespace tair
{
  class request_lindex:public base_packet
  {
  public:
    request_lindex ()
    {
      setPCode (TAIR_REQ_LINDEX_PACKET);
      server_flag = 0;
      area = 0;
      index = 0;
    }

    request_lindex (request_lindex & packet)
    {
      setPCode (TAIR_REQ_LINDEX_PACKET);
      server_flag = packet.server_flag;
      area = packet.area;
      index = packet.index;
      key.clone (packet.key);
    }

    bool encode (tbnet::DataBuffer * output)
    {
      output->writeInt8(server_flag);
      output->writeInt16(area);
      output->writeInt32(index);
      
      output->writeInt32(key.get_size());
      output->writeBytes(key.get_data(), key.get_size());

      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
        HEADER_VERIFY;

        GETKEY_FROM_INT32 (input, index);
        GETKEY_FROM_DATAENTRY (input, key);
    
        return true;
    }

  public:
    uint16_t area;
    data_entry key;
    int32_t index;
  };
  class response_lindex:public base_packet
  {
  public:
    response_lindex ()
    {
      setPCode (TAIR_RESP_LINDEX_PACKET);
      config_version = 0;
      version = 0;
      code = 0;
    }

    ~response_lindex(){}

    bool encode (tbnet::DataBuffer * output)
    {
      output->writeInt32 (config_version);
      output->writeInt16 (version);
      output->writeInt32 (code);
	    
      output->writeInt32 (value.get_size ());
	  output->writeBytes (value.get_data (), value.get_size ());
      
      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      GETKEY_FROM_INT32(input, config_version);
      GETKEY_FROM_INT16(input, version);
      GETKEY_FROM_INT32(input, code);
      GETKEY_FROM_DATAENTRY(input, value);
      return true;
    }

    void set_version(uint16_t version)
    {
        this->version = version;
    }

    void set_meta (uint32_t config_version, uint32_t code)
    {
      this->config_version = config_version;
      //this->version           = version;
      this->code = code;
    }

    int get_code()
    {
        return code;
    }
  public:
    uint32_t config_version;
    uint16_t version;
    int32_t code;
    data_entry value;
  };
}

#endif
