/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sample_plugin.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#ifndef SAMPLE_PLUGIN_H
#define SAMPLE_PLUGIN_H
#include "base_plugin.hpp"
namespace tair {
   namespace plugin {
      class sample_plugin: public base_plugin {
      public:
         int get_hook_point();
         int get_property();
         int get_plugin_type();

         int do_request(uint32_t hashcode, base_packet* packet, void* exv = NULL);
         void do_response(int ret, uint32_t hashcode, base_packet* packet, void* exv = NULL);

         bool init();
         void clean();
      };
   }
}
#endif
