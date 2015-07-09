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
#ifndef TAIR_PACKET_ZREMRANGEBYSCORE_PACKET_H
#define TAIR_PACKET_ZREMRANGEBYSCORE_PACKET_H
#include "base_packet.hpp"
#include <stdint.h>

#include "storage_manager.hpp"

namespace tair
{


  class request_zremrangebyscore:public base_packet
  {
  public:
    request_zremrangebyscore ()
    {
      setPCode (TAIR_REQ_ZREMRANGEBYSCORE_PACKET);

      server_flag = 0;
      area = 0;
      version = 0;
      expire = 0;
	  start = 0.0;
	  end = 0.0;
    }

    request_zremrangebyscore (request_zremrangebyscore & packet)
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
      PUT_DOUBLE_TO_BUFFER (output, start);
      PUT_DOUBLE_TO_BUFFER (output, end);
      PUT_DATAENTRY_TO_BUFFER (output, key);

      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
        HEADER_VERIFY;

        GETKEY_FROM_INT16 (input, version);
        GETKEY_FROM_INT32 (input, expire);
        GETKEY_FROM_DOUBLE (input, start);
		GETKEY_FROM_DOUBLE (input, end);

        GETKEY_FROM_DATAENTRY (input, key);

        key.set_version(version);

        return true;
    }

  public:
    uint16_t area;
    uint16_t version;
    int expire;
    data_entry key;
    double start;
	double end;
  };

  class response_zremrangebyscore:public base_packet
  {
#define RESPONSE_VALUES_MAXSIZE 32767
  public:
    response_zremrangebyscore (int pcode)
    {
      setPCode (pcode);
      config_version = 0;
    }

    response_zremrangebyscore ()
    {
      setPCode (TAIR_RESP_ZREMRANGEBYSCORE_PACKET);

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
      GETKEY_FROM_INT32 (input, config_version);
      GETKEY_FROM_INT32 (input, code);
      GETKEY_FROM_INT64 (input, retnum);


      // not support in cpp api;
      return false;
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
    int32_t code;
    long long retnum;
  };

}				// end namespace
#endif
