/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * this packet is for remove one particular area
 *
 * Version: $Id: remove_area_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_DUMP_AREA_PACKET_H
#define TAIR_PACKET_DUMP_AREA_PACKET_H
#include "base_packet.hpp"
namespace tair {
   class request_dump_area : public base_packet {
   public:

      request_dump_area()
      {
         setPCode(TAIR_REQ_DUMP_AREA_PACKET);
         area = 0;
      }


      request_dump_area(request_dump_area &packet)
      {
         setPCode(TAIR_REQ_DUMP_AREA_PACKET);
         area = packet.area;
      }


      ~request_dump_area()
      {
      }


      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt32(area);
         return true;
      }


      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         HEADER_VERIFY;
         return true;
      }

   public:
      int area;
   };
}
#endif
