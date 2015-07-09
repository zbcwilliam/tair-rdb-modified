/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * spop packet
 *
 * Version: $Id: spop_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_SPOP_PACKET_H
#define TAIR_PACKET_SPOP_PACKET_H
#include "base_packet.hpp"
namespace tair
{
  class request_spop:public base_packet
  {
  public:
    request_spop ()
    {
      setPCode (TAIR_REQ_SPOP_PACKET);
      server_flag = 0;
      area = 0;
      version = 0;
      expire = 0;
    }

    request_spop (request_spop & packet)
    {
      setPCode (packet.getPCode ());
      server_flag = packet.server_flag;
      area = packet.area;

      version = packet.version;
      expire = packet.expire;
      key.clone (packet.key);
    }

    bool encode (tbnet::DataBuffer * output)
    {
      assert (0);
      //cpp in futrue
      return false;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {

      HEADER_VERIFY;

      GETKEY_FROM_INT16 (input, version);
      GETKEY_FROM_INT32 (input, expire);
      GETKEY_FROM_DATAENTRY (input, key);

      key.set_version(version);

      return true;
    }

  public:
    uint16_t area;
    uint16_t version;
    int32_t expire;
    data_entry key;
  };

  class response_spop:public base_packet
  {
  public:

    response_spop ()
    {
      config_version = 0;
      setPCode (TAIR_RESP_SPOP_PACKET);
      code = 0;
      version = 0;
    }

    bool encode (tbnet::DataBuffer * output)
    {
      output->writeInt32 (config_version);
      output->writeInt16 (version);
      output->writeInt32 (code);

      output->writeInt32 (value.get_size());
      output->writeBytes (value.get_data(), value.get_size());
      
      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      assert (0);
      //cpp in future
      return false;
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

    void set_version(uint16_t version)
    {
        this->version = version;
    }

  public:
    uint32_t config_version;
    uint16_t version;

    data_entry value;
  private:
    int32_t code;
    response_spop (const response_spop &);
  };
}
#endif
