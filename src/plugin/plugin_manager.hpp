/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: plugin_manager.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#ifndef PLUGIN_MANAGER_H
#define PLUGIN_MANAGER_H

#include "tbsys.h"
#include "base_plugin.hpp"
#include "atomic.h"
#include <string>
#include <map>

namespace tair{
   namespace plugin {
      //do not derived from this class
      class plugin_handler {
      public:
         explicit plugin_handler(const std::string& dll_name);
         base_plugin* get_instance() const;
         std::string get_dll_name() const {
            return dll_name;
         }
         // the next 3 func need be protected by mutex
         bool load_dll();
         bool unload_dll();
         ~plugin_handler();
      private:
         std::string dll_name;
         void* dll_handler;
         create_t* create;
         destroy_t* destroy;
         base_plugin* instance;

      };

      /* now tair only can support 32 kinds of plugins,
       * and all kinds has its pugin number, for example,
       * PLUGIN_TYPE_SYSTEM  = 1, PLUGIN_TYPE_REMOTE_SYNC = 2,
       * we consider that we didn't need more than 32 kinds of plugins,
       * and use simple array, we can do faster for use plugins
       * oterwise plugins_root is not thread-safe*/
      class plugins_root { // this class for copy on write
#define MAX_PLUGINS   32
#define MAX_PROPERTYS  8
      public:
          plugins_root() {
              memset(request_plugins_start, 0, sizeof(request_plugins_start));
              memset(request_plugins_end, 0, sizeof(request_plugins_end));
              memset(response_plugins_start, 0, sizeof(response_plugins_start));
              memset(response_plugins_end, 0, sizeof(response_plugins_end));
              memset(request_plugins, 0, sizeof(request_plugins));
              memset(response_plugins, 0, sizeof(response_plugins));
          }

          plugin_handler* add_request_plugin_handler(int type, int property, plugin_handler* handler) {
              if (type >= MAX_PLUGINS || type < 0 || property >= MAX_PROPERTYS || property < 0) {
                  return NULL;
              }
              request_plugins[type][property] = handler;
              if (property >= request_plugins_end[type]) {
                  request_plugins_end[type] = property + 1;
              }
              if (property < request_plugins_start[type]) {
                  request_plugins_start[type] = property;
              }

              return handler;
          }

          plugin_handler* add_response_plugin_handler(int type, int property, plugin_handler* handler) {
              if (type >= MAX_PLUGINS || type < 0 || property >= MAX_PROPERTYS || property < 0) {
                  return NULL;
              }
              response_plugins[type][property] = handler;
              if (property >= request_plugins_end[type]) {
                  response_plugins_end[type] = property + 1;
              }
              if (property < request_plugins_start[type]) {
                  response_plugins_start[type] = property;
              }

              return handler;
          }

          void clean_request_type_plugins(int type) {
              if (type >= MAX_PLUGINS || type < 0) {
                  return;
              }

              plugin_handler* handler = NULL;
              for(uint8_t i = request_plugins_start[type]; i < request_plugins_end[type]; i++) {
                  handler = request_plugins[type][i];
                  request_plugins[type][i] = NULL;
                  if (handler) {
                      handler->get_instance()->clean();
                      delete handler;
                  }
              }
          }

          void clean_response_type_plugins(int type) {
              if (type >= MAX_PLUGINS || type < 0) {
                  return;
              }

              plugin_handler* handler = NULL;
              for(uint8_t i = response_plugins_start[type]; i < response_plugins_end[type]; i++) {
                  handler = response_plugins[type][i];
                  response_plugins[type][i] = NULL;
                  if (handler) {
                      handler->get_instance()->clean();
                      delete handler;
                  }
              }
          }

          uint8_t get_request_plugins_start(int type) {
              return request_plugins_start[type];
          }

          uint8_t get_request_plugins_end(int type) {
              return request_plugins_end[type];
          }

          uint8_t get_response_plugins_start(int type) {
              return response_plugins_start[type];
          }

          uint8_t get_response_plugins_end(int type) {
              return response_plugins_end[type];
          }

          bool exist_type_request_plugins(int type) {
              return (request_plugins_start[type] != request_plugins_end[type]);
          }

          bool exist_type_response_plugins(int type) {
              return (response_plugins_start[type] != response_plugins_end[type]);
          }

      public:
          uint8_t request_plugins_start[MAX_PLUGINS];
          uint8_t request_plugins_end[MAX_PLUGINS];
          uint8_t response_plugins_start[MAX_PLUGINS];
          uint8_t response_plugins_end[MAX_PLUGINS];
          plugin_handler* request_plugins[MAX_PLUGINS][MAX_PROPERTYS];
          plugin_handler* response_plugins[MAX_PLUGINS][MAX_PROPERTYS];
      };

      class plugins_manager {
      public:
         plugins_manager(): root(NULL) {}
         ~plugins_manager();

         std::set<std::string>* get_dll_names();
         bool chang_plugins_to(const std::set<std::string>& dll_names);

         int do_request_plugins(int plugin_type, uint32_t hashcode, base_packet *packet,
                 plugins_root* &root);
         void do_response_plugins(int plugin_type, int ret, uint32_t hashcode, base_packet *packet,
                 plugins_root* &root);
      public:
         bool exist_type_request_plugins(int type) {
             return (root == NULL ? false : root->exist_type_request_plugins(type));
         }

         bool exist_type_response_plugins(int type) {
             return (root == NULL ? false : root->exist_type_response_plugins(type));
         }
         bool add_plugins(const std::set<std::string>& dll_names);
         void remove_plugins (const std::set<std::string>& dll_names);
      private:
         bool add_plugin(const std::string& dll_name, plugins_root* root);
         void clean_plugins(plugins_root* &); //delete point
      private:
         plugins_root* root;
         tbsys::CThreadMutex mutex;
      };
   }}
#endif
