/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_plugin.hpp 523 2012-01-06 10:05:19Z xinshu.wzx@taobao.com $
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#ifndef BASE_PLUGIN_H
#define BASE_PLUGIN_H

#include "data_entry.hpp"

namespace tair {
   class base_packet;
   namespace plugin {
      using namespace tair::common;

      const int HOOK_POINT_REQUEST  = 1;
      const int HOOK_POINT_RESPONSE = 2;
      const int HOOK_POINT_ALL      = HOOK_POINT_REQUEST | HOOK_POINT_RESPONSE;

      const int PLUGIN_TYPE_SYSTEM  = 1;
      const int PLUGIN_TYPE_REMOTE_SYNC = 2;

      class base_plugin {
      public:
         virtual ~base_plugin()
         {
            return;
         }

         /*
          * return value 1 hook request 2 hook response 3 hook request and response
          */
         virtual int get_hook_point() = 0;

         /*
          * larger property will be excuted earier.
          */
         virtual int get_property() = 0;

         /*
          * reserved
          */
         virtual int get_plugin_type() = 0;

         virtual int do_request(uint32_t hashcode,
                 base_packet* packet, void* exv = NULL) = 0;

         virtual void do_response(int ret, uint32_t hashcode,
                 base_packet* packet, void* exv = NULL) = 0;

         virtual bool init()
         {
            return true;
         }

         virtual bool init(const std::string& para)
	     {
            return true;
         }

         virtual void clean()
         {
            return;
         }
      };
      typedef base_plugin* (create_t)();
      typedef void (destroy_t)(base_plugin*);
   }
}
#endif
