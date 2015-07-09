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
#ifndef TAIR_PACKET_SREM_PACKET_H
#define TAIR_PACKET_SREM_PACKET_H
#include <stdint.h>

#include "base_packet.hpp"
#include "sadd_packet.hpp"

#include "storage_manager.hpp"

namespace tair
{


  class request_srem:public base_packet
  {
  public:
    request_srem()
    {
      setPCode(TAIR_REQ_SREM_PACKET);

      server_flag = 0;
      area = 0;
      version = 0;
      expire = 0;
    }

    request_srem(const uint16_t iarea, const uint16_t iversion,
            const int iexpire, const data_entry &ikey,
            const data_entry &ivalue)
    {
      setPCode(TAIR_REQ_SREM_PACKET);
      server_flag = 0;
      area = iarea;
      version = iversion;
      expire = iexpire;
      key = ikey;
      value = ivalue;
    }

    request_srem(request_srem & packet)
    {
      setPCode (packet.getPCode ());
      server_flag = packet.server_flag;
      area = packet.area;

      version = packet.version;
      expire = packet.expire;
      key.clone (packet.key);
      value.clone (packet.value);
    }

    bool encode(tbnet::DataBuffer * output)
    {
      CREATE_HEADER;

      PUT_INT16_TO_BUFFER(output, version);
      PUT_INT32_TO_BUFFER(output, expire);

      PUT_DATAENTRY_TO_BUFFER(output, key);
      PUT_DATAENTRY_TO_BUFFER(output, value);

      return true;
    }

    bool decode(tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      HEADER_VERIFY;

      GETKEY_FROM_INT16(input, version);
      GETKEY_FROM_INT32(input, expire);

      GETKEY_FROM_DATAENTRY(input, key);
      GETKEY_FROM_DATAENTRY(input, value);

      key.set_version(version);

      return true;
    }

  public:
    uint16_t area;
    uint16_t version;
    int expire;
    data_entry key;
    data_entry value;
  };

  class request_srem_multi:public request_sadd_multi
  {
  public:
    request_srem_multi():request_sadd_multi()
    {
      setPCode(TAIR_REQ_SREMMULTI_PACKET);
    }

    request_srem_multi(const uint16_t iarea, const int32_t iexpire,
            const map<data_entry*, vector<data_entry*>*> &keys_values_map, bool ifree)
        :request_sadd_multi(iarea, iexpire, keys_values_map, ifree)
    {
      setPCode(TAIR_REQ_SREMMULTI_PACKET);
    }

    request_srem_multi(const uint16_t iarea, const int32_t iexpire,
            const data_entry* key, const vector<data_entry*>* values)
        :request_sadd_multi(iarea, iexpire, key, values)
    {
      setPCode(TAIR_REQ_SREMMULTI_PACKET);
    }

    request_srem_multi(request_srem_multi &packet)
        :request_sadd_multi(packet)
    {
      setPCode(TAIR_REQ_SREMMULTI_PACKET);
    }
  };

  class response_srem:public base_packet
  {
#define RESPONSE_VALUES_MAXSIZE 32767
  public:
    response_srem ()
    {
      setPCode (TAIR_RESP_SREM_PACKET);

      config_version = 0;
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
  };

}				// end namespace
#endif
