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
#ifndef TAIR_PACKET_ADD_FILTER_PACKET_H
#define TAIR_PACKET_ADD_FILTER_PACKET_H
#include "base_packet.hpp"
namespace tair {
   class request_addfilter : public base_packet {
   public:

      request_addfilter()
      {
         setPCode(TAIR_REQ_ADD_FILTER_PACKET);
         area = 0;
      }


      request_addfilter(request_addfilter &packet)
      {
         setPCode(TAIR_REQ_ADD_FILTER_PACKET);
         area = packet.area;
      }


      ~request_addfilter()
      {
      }


      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt32(area);
         PUT_DATAENTRY_TO_BUFFER(output, key);
         PUT_DATAENTRY_TO_BUFFER(output, field);
         PUT_DATAENTRY_TO_BUFFER(output, value);

         return true;
      }


      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         HEADER_VERIFY;

         GETKEY_FROM_DATAENTRY (input, key);
         GETKEY_FROM_DATAENTRY (input, field);
         GETKEY_FROM_DATAENTRY (input, value);

         return true;
      }

   public:
      int area;
      data_entry key;
      data_entry field;
      data_entry value;
   };
}
#endif
