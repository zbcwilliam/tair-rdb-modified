/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * hexists packet
 *
 * Version: $Id: hexists_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_HEXISTS_PACKET_H
#define TAIR_PACKET_HEXISTS_PACKET_H
#include "base_packet.hpp"
namespace tair
{
  class request_hexists:public base_packet
  {
  public:
    request_hexists()
    {
      setPCode(TAIR_REQ_HEXISTS_PACKET);
      server_flag = 0;
      area = 0;
    }

    request_hexists(const uint16_t iarea, const data_entry &ikey, const data_entry &ifield)
    {
      setPCode(TAIR_REQ_HEXISTS_PACKET);
      server_flag = 0;
      area = iarea;
      key = ikey;
      field = ifield;
    }

    request_hexists(request_hexists & packet)
    {
      setPCode(packet.getPCode());
      server_flag = packet.server_flag;
      area = packet.area;

      key.clone(packet.key);
      field.clone(packet.field);
    }

    bool encode(tbnet::DataBuffer * output)
    {
      CREATE_HEADER;

      PUT_DATAENTRY_TO_BUFFER(output, key);
      PUT_DATAENTRY_TO_BUFFER(output, field);

      return true;
    }

    bool decode(tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {

      HEADER_VERIFY;

      GETKEY_FROM_DATAENTRY (input, key);
      GETKEY_FROM_DATAENTRY (input, field);

      return true;
    }

  public:
    uint16_t area;
    data_entry key;
    data_entry field;
  };
}
#endif
