/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * hset packet
 *
 * Version: $Id: hset_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_HSET_PACKET_H
#define TAIR_PACKET_HSET_PACKET_H
#include "base_packet.hpp"
namespace tair
{
  class request_hset:public base_packet
  {
  public:
    request_hset()
    {
      setPCode(TAIR_REQ_HSET_PACKET);
      server_flag = 0;
      area = 0;
      version = 0;
      expire = 0;
    }

    request_hset(const uint16_t iarea, const uint16_t iversion, const int iexpire,
            const data_entry &ikey, const data_entry &ifield, const data_entry &ivalue)
    {
      setPCode(TAIR_REQ_HSET_PACKET);
      server_flag = 0;
      area = iarea;
      version = iversion;
      expire = iexpire;
      key = ikey;
      field = ifield;
      value = ivalue;
    }

    request_hset(request_hset & packet)
    {
      setPCode(packet.getPCode ());
      server_flag = packet.server_flag;
      area = packet.area;

      version = packet.version;
      expire = packet.expire;
      key.clone(packet.key);
      field.clone(packet.field);
      value.clone(packet.value);
    }

    bool encode(tbnet::DataBuffer * output)
    {
      CREATE_HEADER;

      PUT_INT16_TO_BUFFER(output, version);
      PUT_INT32_TO_BUFFER(output, expire);
      PUT_DATAENTRY_TO_BUFFER(output, key);
      PUT_DATAENTRY_TO_BUFFER(output, field);
      PUT_DATAENTRY_TO_BUFFER(output, value);

      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {

      HEADER_VERIFY;

      GETKEY_FROM_INT16 (input, version);
      GETKEY_FROM_INT32 (input, expire);
      GETKEY_FROM_DATAENTRY (input, key);
      GETKEY_FROM_DATAENTRY (input, field);
      GETKEY_FROM_DATAENTRY (input, value);

      key.set_version(version);

      return true;
    }

  public:
    uint16_t area;
    uint16_t version;
    int expire;
    data_entry key;
    data_entry field;
    data_entry value;
  };

  class response_hset:public base_packet
  {
  public:

    response_hset ()
    {
      config_version = 0;
      setPCode (TAIR_RESP_HSET_PACKET);
      code = 0;
    }

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
	void set_version(uint16_t version) {}
  public:
    uint32_t config_version;
  private:
    int32_t code;
    response_hset (const response_hset &);
  };
}
#endif
