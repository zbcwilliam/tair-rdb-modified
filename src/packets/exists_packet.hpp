/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * hget packet
 *
 * Version: $Id: hget_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_EXISTS_PACKET_H
#define TAIR_PACKET_EXISTS_PACKET_H
#include "base_packet.hpp"
namespace tair
{
  class request_exists:public base_packet
  {
  public:
    request_exists()
    {
      setPCode(TAIR_REQ_EXISTS_PACKET);
      server_flag = 0;
      area = 0;
    }

    request_exists(const uint16_t iarea, const data_entry &ikey)
    {
      setPCode(TAIR_REQ_EXISTS_PACKET);
      server_flag = 0;
      area = iarea;
      key = ikey;
    }

    request_exists(request_exists & packet)
    {
      setPCode(packet.getPCode());
      server_flag = packet.server_flag;
      area = packet.area;

      key.clone(packet.key);
    }

    bool encode(tbnet::DataBuffer * output)
    {
      CREATE_HEADER;

      PUT_DATAENTRY_TO_BUFFER(output, key);

      return true;
    }

    bool decode(tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {

      HEADER_VERIFY;

      GETKEY_FROM_DATAENTRY (input, key);

      return true;
    }

  public:
    uint16_t area;
    data_entry key;
  };
}
#endif
