/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * zcount packet
 *
 * Version: $Id: zcount_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_ZCOUNT_PACKET_H
#define TAIR_PACKET_ZCOUNT_PACKET_H
#include "base_packet.hpp"
#include <stdint.h>

#include "storage_manager.hpp"

namespace tair
{


  class request_zcount:public base_packet
  {
  public:
    request_zcount ()
    {
      setPCode (TAIR_REQ_ZCOUNT_PACKET);

      server_flag = 0;
      area = 0;
      start = 0.0;
      end = 0.0;
    }

    request_zcount (request_zcount & packet)
    {
      setPCode (packet.getPCode ());
      server_flag = packet.server_flag;
      area = packet.area;
      start = packet.start;
      end = packet.end;
      key.clone (packet.key);
    }

    bool encode (tbnet::DataBuffer * output)
    {
      assert (false);
      // not support cpp api now
      return false;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
        HEADER_VERIFY;

        GETKEY_FROM_DOUBLE (input, start);
        GETKEY_FROM_DOUBLE (input, end);
        GETKEY_FROM_DATAENTRY (input, key);
        
        return true;
    }

  public:
    uint16_t area;
    data_entry key;
    double start;
    double end;
  };

  class response_zcount:public base_packet
  {
#define RESPONSE_VALUES_MAXSIZE 32767
  public:
    response_zcount ()
    {
      setPCode (TAIR_RESP_ZCOUNT_PACKET);
      config_version = 0;
	  retnum = 0;
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
      // not support in cpp api;
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
