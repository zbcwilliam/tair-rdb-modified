/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * getset packet
 *
 * Version: $Id: getset_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_GETSET_PACKET_H
#define TAIR_PACKET_GETSET_PACKET_H
#include "hget_packet.hpp"
#include "sadd_packet.hpp"

namespace tair
{
  class request_getset:public request_sadd
  {
  public:
    request_getset() : request_sadd()
    {
      setPCode(TAIR_REQ_GETSET_PACKET);
    }

    request_getset(const uint16_t iarea, const uint16_t iversion,
            const int32_t iexpire, const data_entry &ikey,
            const data_entry &ivalue) : request_sadd(iarea, iversion, iexpire, ikey, ivalue)
    {
      setPCode(TAIR_REQ_GETSET_PACKET);
    }
  };

  class response_getset:public response_hget
  {
  public:
    response_getset () : response_hget()
    {
      setPCode (TAIR_RESP_GETSET_PACKET);
    }
  };
}
#endif
