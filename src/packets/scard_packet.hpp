/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * scard packet
 *
 * Version: $Id: scard_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_SCARD_PACKET_H
#define TAIR_PACKET_SCARD_PACKET_H
#include "base_packet.hpp"
namespace tair
{
  class request_scard:public base_packet
  {
  public:
    request_scard ()
    {
      setPCode (TAIR_REQ_SCARD_PACKET);
      server_flag = 0;
      area = 0;
    }

    request_scard (request_scard & packet)
    {
      setPCode (packet.getPCode ());
      server_flag = packet.server_flag;
      area = packet.area;

      key.clone (packet.key);
    }

    bool encode (tbnet::DataBuffer * output)
    {
      //assert (0);
      //cpp in futrue//commented 6.30
      CREATE_HEADER;
	  PUT_DATAENTRY_TO_BUFFER (output, key);
      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {

      HEADER_VERIFY;
      GETKEY_FROM_DATAENTRY (input, key);
      return true;
    }

  public:
    uint16_t area;
    data_entry key;
  };

  class response_scard:public base_packet
  {
  public:

    response_scard ()
    {
      config_version = 0;
      setPCode (TAIR_RESP_SCARD_PACKET);
      code = 0;
    }

    bool encode (tbnet::DataBuffer * output)
    {
      output->writeInt32 (config_version);
	  output->writeInt16 (version);
      output->writeInt32 (code);
      output->writeInt64 (retnum);

      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      //assert (0);
      //cpp in future//commented 6.30
      GETKEY_FROM_INT32(input, config_version); 
      GETKEY_FROM_INT16(input, version);
      GETKEY_FROM_INT32(input, code);
	  GETKEY_FROM_INT64(input, retnum);
	  //cout << "decode retnum is: " << retnum <<endl;
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
	void set_version(uint16_t version) 
	{
	  this->version = version;//added 6.30
	}
  public:
    uint32_t config_version;
	uint16_t version;
    long long retnum;
  private:
    int32_t code;
    response_scard (const response_scard &);
  };
}
#endif
