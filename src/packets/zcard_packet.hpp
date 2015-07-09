/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * zcard packet
 *
 * Version: $Id: zcard_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_ZCARD_PACKET_H
#define TAIR_PACKET_ZCARD_PACKET_H
#include "base_packet.hpp"
namespace tair
{
  class request_zcard:public base_packet
  {
  public:
    request_zcard ()
    {
      setPCode (TAIR_REQ_ZCARD_PACKET);
      server_flag = 0;
      area = 0;
    }

    request_zcard (request_zcard & packet)
    {
      setPCode (packet.getPCode ());
      server_flag = packet.server_flag;
      area = packet.area;

      key.clone (packet.key);
    }

    bool encode (tbnet::DataBuffer * output)
    {
      //assert (0);  //commented 6.29
      //cpp in futrue   //commented 6.29
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

  class response_zcard:public base_packet
  {
  public:

    response_zcard ()
    {
      config_version = 0;
      setPCode (TAIR_RESP_ZCARD_PACKET);
      code = 0;
    }

    bool encode (tbnet::DataBuffer * output)
    {
      output->writeInt32 (config_version);
	  output->writeInt16 (version);
      output->writeInt32 (code);
      output->writeInt64 (retnum);//added 6.29
      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      //assert (0);//commented 6.29
      //cpp in future//commented 6.29
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
        this->version = version;//added 6.29
    }
	
  public:
    uint32_t config_version;
	uint16_t version;
    long long retnum;
  private:
    int32_t code;
    response_zcard (const response_zcard &);
  };
}
#endif
