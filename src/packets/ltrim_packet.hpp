/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ltrim packet
 *
 * Version: $Id: ltrim_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_LTRIM_PACKET_H
#define TAIR_PACKET_LTRIM_PACKET_H
#include "base_packet.hpp"
namespace tair
{
  class request_ltrim:public base_packet
  {
  public:
    request_ltrim ()
    {
      setPCode (TAIR_REQ_LTRIM_PACKET);
      server_flag = 0;
      area = 0;
      version = 0;
      expire = 0;
      start = 0;
      end = 0;
    }

    request_ltrim (request_ltrim & packet)
    {
      setPCode (packet.getPCode ());
      server_flag = packet.server_flag;
      area = packet.area;

      version = packet.version;
      expire = packet.expire;
      start = packet.start;
      end = packet.end;

      key.clone (packet.key);
    }

    bool encode (tbnet::DataBuffer * output)
    {
      CREATE_HEADER;

      PUT_INT16_TO_BUFFER (output, version);
      PUT_INT32_TO_BUFFER (output, expire);
      PUT_INT32_TO_BUFFER (output, start);
      PUT_INT32_TO_BUFFER (output, end);
      PUT_DATAENTRY_TO_BUFFER (output, key);

      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {

      HEADER_VERIFY;

      GETKEY_FROM_INT16 (input, version);
      GETKEY_FROM_INT32 (input, expire);
      GETKEY_FROM_INT32 (input, start);
      GETKEY_FROM_INT32 (input, end);
      GETKEY_FROM_DATAENTRY (input, key);

      key.set_version(version);

      return true;
    }

  public:
    uint16_t area;
    uint16_t version;
    int32_t expire;
    int32_t start;
    int32_t end;
    data_entry key;
  };

  class response_ltrim:public base_packet
  {
  public:

    response_ltrim ()
    {
      config_version = 0;
      setPCode (TAIR_RESP_LTRIM_PACKET);
      code = 0;
    }

    ~response_ltrim () {}

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
    response_ltrim (const response_ltrim &);
  };
}
#endif
