/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * put packet
 *
 * Version: $Id: put_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_PUT_PACKET_H
#define TAIR_PACKET_PUT_PACKET_H
#include "base_packet.hpp"
namespace tair {
   class request_put : public base_packet {
   public:
      request_put()
      {
         setPCode(TAIR_REQ_PUT_PACKET);
         server_flag = 0;
         area = 0;
         version = 0;
         expired = 0;
      }


      request_put(request_put &packet)
      {
         setPCode(TAIR_REQ_PUT_PACKET);
         server_flag = packet.server_flag;
         area = packet.area;
         version = packet.version;
         expired = packet.expired;
         key.clone(packet.key);
         data.clone(packet.data);
      }

      ~request_put()
      {
      }

      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt8(server_flag);
         output->writeInt16(area);
         output->writeInt16(version);
         output->writeInt32(expired);
         key.encode(output);
         data.encode(output);

         return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         HEADER_VERIFY;
         GETKEY_FROM_INT16(input,version);
         GETKEY_FROM_INT32(input,expired);
         key.decode(input);
         data.decode(input);
         key.data_meta.version = version;
         if (server_flag & HAS_HASH_CODE) {
            GETKEY_FROM_INT32(input,hash_code);
            server_flag = server_flag & (~HAS_HASH_CODE);
         }

         return true;
      }

   public:
      uint16_t        area;
      uint16_t        version;
      int32_t         expired;
      data_entry    key;
      data_entry    data;
   };

   class request_putnx : public request_put {
   public:
      request_putnx()
      {
         setPCode(TAIR_REQ_PUTNX_PACKET);
         server_flag = 0;
         area = 0;
         version = 0;
         expired = 0;
      }

      request_putnx(request_putnx &packet)
      {
         setPCode(TAIR_REQ_PUTNX_PACKET);
         server_flag = packet.server_flag;
         area = packet.area;
         version = packet.version;
         expired = packet.expired;
         key.clone(packet.key);
         data.clone(packet.data);
      }
   };
}
#endif
