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
#ifndef TAIR_PACKET_LREM_PACKET_H
#define TAIR_PACKET_LREM_PACKET_H
#include "base_packet.hpp"
#include <stdint.h>

#include "storage_manager.hpp"

namespace tair
{


  class request_lrem:public base_packet
  {
  public:
    request_lrem ()
    {
      setPCode (TAIR_REQ_LREM_PACKET);

      server_flag = 0;
      area = 0;
      count = 0;
      version = 0;
      expire = 0;
    }

    request_lrem (request_lrem & packet)
    {
      setPCode (packet.getPCode ());
      server_flag = packet.server_flag;
      area = packet.area;

      version = packet.version;
      expire = packet.expire;
      count = packet.count;
      key.clone (packet.key);
	  value.clone (packet.key);
    }

    bool encode (tbnet::DataBuffer * output)
    {
      CREATE_HEADER;

      PUT_INT16_TO_BUFFER(output, version);
      PUT_INT32_TO_BUFFER(output, expire);
      PUT_INT32_TO_BUFFER(output, count);

      PUT_DATAENTRY_TO_BUFFER(output, key);
      PUT_DATAENTRY_TO_BUFFER(output, value);

      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      HEADER_VERIFY;

      GETKEY_FROM_INT16 (input, version);
      GETKEY_FROM_INT32 (input, expire);
      GETKEY_FROM_INT32 (input, count);

      GETKEY_FROM_DATAENTRY (input, key);
      GETKEY_FROM_DATAENTRY (input, value);

      key.set_version(version);

      return true;
    }

  public:
    uint16_t area;
    uint16_t version;
    int expire;
    data_entry key;
    data_entry value;
    int count;
  };

  class response_lrem:public base_packet
  {
#define RESPONSE_VALUES_MAXSIZE 32767
  public:
    response_lrem (int pcode)
    {
      setPCode (pcode);
      config_version = 0;
    }

    response_lrem ()
    {
      setPCode (TAIR_RESP_LREM_PACKET);

      config_version = 0;
    }

    bool encode (tbnet::DataBuffer * output)
    {
      output->writeInt32 (config_version);
      output->writeInt32 (code);
      output->writeInt64 (retnum);

      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      GETKEY_FROM_INT32(input, config_version);
      GETKEY_FROM_INT32(input, code);
      GETKEY_FROM_INT64(input, retnum);

      return false;
    }

    void set_meta (uint32_t config_version, uint32_t code)
    {
      this->config_version = config_version;
      //this->version           = version;
      this->code = code;
    }

	//not used
	void set_version(uint16_t version) {}
  public:
    uint32_t config_version;
    int32_t code;
    long long retnum;
  };

}				// end namespace
#endif
