/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * sample_plugin.cpp is a example to show how to make a plugin
 *
 * Version: $Id: sample_plugin.cpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#include "sample_plugin.hpp"
#include "tbsys.h"
namespace tair{
   namespace plugin{

      int sample_plugin::get_hook_point() {
         return HOOK_POINT_REQUEST;
      }

      int sample_plugin::get_property() {
         return 1;
      }

      int sample_plugin::get_plugin_type() {
         return PLUGIN_TYPE_SYSTEM;
      }

      int sample_plugin::do_request(uint32_t hashcode, base_packet* packet, void* exv) {
         log_debug("sample_plugin::do_request TairDataEntry key hashcode:%d TairPacket packet:%p",
                 hashcode, packet);
         return 0;
      }

      void sample_plugin::do_response(int ret, uint32_t hashcode, base_packet* packet, void* exv) {
         log_debug("sample_plugin::do_request ret:%d TairDataEntry key hashcode:%d TairPacket packet:%p",
                 ret, hashcode, packet);
      }

      bool sample_plugin::init()
      {
         log_debug("sample_plugin::init()");
         return true;
      }

      void sample_plugin::clean()
      {
         log_debug("sample_plugin::clean()");
         return;
      }

      extern "C" base_plugin* create()
      {
         return new sample_plugin();
      }

      extern "C" void destroy (base_plugin* p)
      {
         delete p;
      }
   }
}
