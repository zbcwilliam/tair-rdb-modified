/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * expireat packet
 *
 * Version: $Id: expireat_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_EXPIREAT_PACKET_H
#define TAIR_PACKET_EXPIREAT_PACKET_H
#include "base_packet.hpp"
namespace tair
{
  class request_expireat:public base_packet
  {
  public:
    request_expireat ()
    {
      setPCode (TAIR_REQ_EXPIREAT_PACKET);
      server_flag = 0;
      area = 0;
    }

    request_expireat (request_expireat & packet)
    {
      setPCode (packet.getPCode ());
      server_flag = packet.server_flag;
      area = packet.area;
      expiretime = packet.expiretime;
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

      GETKEY_FROM_INT32 (input, expiretime);
      GETKEY_FROM_DATAENTRY (input, key);

      return true;
    }

  public:
    uint16_t area;
    data_entry key;
    int32_t expiretime;
  };

  class response_expireat:public base_packet
  {
  public:

    response_expireat ()
    {
      config_version = 0;
      setPCode (TAIR_RESP_EXPIREAT_PACKET);
      code = 0;
    }

     ~response_expireat ()
    {
    }

    bool encode (tbnet::DataBuffer * output)
    {
      output->writeInt32 (config_version);
      output->writeInt32 (code);

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

	void set_version(uint16_t version) {}
  public:
    uint32_t config_version;
  private:
    int32_t code;
    response_expireat (const response_expireat &);
  };
}
#endif
