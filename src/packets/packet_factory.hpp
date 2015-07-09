/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * packet factory creates packet according to packet code
 *
 * Version: $Id: packet_factory.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_FACTORY_H
#define TAIR_PACKET_FACTORY_H
#include "base_packet.hpp"
namespace tair {
   class tair_packet_factory : public tbnet::IPacketFactory {
   public:
      tair_packet_factory() {
         tbnet::DefaultPacketStreamer::setPacketFlag(TAIR_PACKET_FLAG);
      }
      ~tair_packet_factory();

      tbnet::Packet *createPacket(int pcode);
      static tbnet::Packet *_createPacket(int pcode);

      static int set_return_packet(base_packet *packet, int code,const char *msg, uint32_t version);
   };
}
#endif
