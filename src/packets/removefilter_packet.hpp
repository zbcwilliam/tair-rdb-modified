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
#ifndef TAIR_PACKET_REMOVE_FILTER_PACKET_H
#define TAIR_PACKET_REMOVE_FILTER_PACKET_H
#include "base_packet.hpp"
namespace tair {
   class request_removefilter : public request_addfilter {
   public:

      request_removefilter() : request_addfilter()
      {
         setPCode(TAIR_REQ_REMOVE_FILTER_PACKET);
      }


      request_removefilter(request_removefilter &packet)
          : request_addfilter(packet)
      {
         setPCode(TAIR_REQ_REMOVE_FILTER_PACKET);
      }

      ~request_removefilter()
      {
      }
   };
}
#endif
