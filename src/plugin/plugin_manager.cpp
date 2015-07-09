/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * plugin_manager.cpp organizes all plugins
 *
 * Version: $Id: plugin_manager.cpp 588 2012-02-27 08:29:52Z xinshu.wzx@taobao.com $
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#include "plugin_manager.hpp"
#include <dlfcn.h>
namespace tair {
   namespace plugin {
      using namespace std;
      using namespace tair;
      const int WAIT_SECONDS = 30000000;
      const string CREATE_FUNC_NAME = "create";
      const string DESTROY_FUNC_NAME = "destroy";
      plugin_handler::plugin_handler(const std::string& dll_name_value)
         :dll_name(dll_name_value), dll_handler(NULL), create(NULL),
          destroy(NULL), instance(NULL)
      {
      }

      bool plugin_handler::load_dll()
      {
        if (instance != NULL) return true;
        char* err_mesage;
        dll_handler = dlopen(dll_name.c_str(), RTLD_NOW|RTLD_GLOBAL);
        if (dll_handler == NULL) {
          err_mesage = dlerror();
          if (err_mesage == NULL) {
            log_error("load dll error name=%s", dll_name.c_str());
          } else {
            log_error("load dll error name=%s error=%s", dll_name.c_str(), err_mesage);
          }
          return false;
        }
        // reset errors
        dlerror();
        create = (create_t*)dlsym(dll_handler, CREATE_FUNC_NAME.c_str());
        err_mesage = dlerror();
        if (err_mesage != NULL) {
          log_error("find create func error message=%s", err_mesage);
          unload_dll();
          return false;
        }
        // reset errors
        dlerror();
        destroy = (destroy_t*)dlsym(dll_handler,DESTROY_FUNC_NAME.c_str());
        err_mesage = dlerror();
        if (err_mesage != NULL) {
          log_error("find destroy func error message=%s", err_mesage);
          unload_dll();
          return false;
        }
        instance = create();
        return true;
      }

      bool plugin_handler::unload_dll()
      {
         if (dll_handler != NULL) {
            if (instance != NULL ) {
               destroy(instance) ;
               instance = NULL;
            }
            dlclose(dll_handler);
            create = NULL;
            destroy = NULL;
         }
         return true;
      }

      plugin_handler::~plugin_handler()
      {
         unload_dll();
      }

      base_plugin* plugin_handler::get_instance() const
      {
         return dynamic_cast<base_plugin*>(instance);
      }

      bool plugins_manager::add_plugin(const std::string& _dll_value, plugins_root* root)
      {
        if (root == NULL) return false;

        //the syncplug.so come like below for compatiable.
        //libsync_plugin.so:{10.232.4.25,10.232.4.26,group_dup}
        string dll_name = _dll_value;
        string _para = _dll_value;
        std::string::size_type _pos = dll_name.find(".so:");
        if(std::string::npos != _pos) {
            dll_name = dll_name.substr(0, _pos+3);
            _para = _para.substr(_pos+4, std::string::npos);
        } else {
            _para = "";
        }


        plugin_handler *handler = new plugin_handler(dll_name);
        // The dll library maintains reference counts for library handles
        if ( handler->load_dll() == false) {
          delete handler;
          return false;
        }
        if (!(handler->get_instance()->get_hook_point() & HOOK_POINT_REQUEST) &&
            !(handler->get_instance()->get_hook_point() & HOOK_POINT_RESPONSE)) {
          log_error("HookPoint error %s ", dll_name.c_str());
          delete handler;
          return false;
        }

        int type = handler->get_instance()->get_plugin_type();
        int property = handler->get_instance()->get_property();
        if (handler->get_instance()->get_hook_point() & HOOK_POINT_REQUEST) {
            plugin_handler* org_handler = root->request_plugins[type][property];
            if (org_handler) {
                log_error("have same type and property in request %s",
                        org_handler->get_dll_name().c_str());
                delete handler;
                return false;
            }
            // we do not add p_handler to handlermap here so we can clean it very easy when something wrong
        }

        if (handler->get_instance()->get_hook_point() & HOOK_POINT_RESPONSE) {
            plugin_handler* org_handler = root->response_plugins[type][property];
            if (org_handler) {
                log_error("have same type and property in request %s",
                        org_handler->get_dll_name().c_str());
                delete handler;
                return false;
            }
            // we do not add p_handler to handlermap here so we can clean it very easy when something wrong
        }

        if (_para.size() <= 0) {
          //only for old plugin.so,althou we just have one.
          if (!handler->get_instance()->init()) {
            log_error("init error %s ", dll_name.c_str());
            delete handler;
            return false;
          }
        } else {
          if (!handler->get_instance()->init(_para)) {
            log_error("init error %s with %s", dll_name.c_str(),_para.c_str());
            delete handler;
            return false;
          } else {
            log_info("init %s with %s ", dll_name.c_str(),_para.c_str());
          }
        }

        if (handler->get_instance()->get_hook_point() & HOOK_POINT_REQUEST) {
            if (root->add_request_plugin_handler(type, property, handler) == NULL) {
                log_error("hook request point: %s is not match tair plugin",
                        handler->get_dll_name().c_str());
                return false;
            }
        }
        if (handler->get_instance()->get_hook_point() & HOOK_POINT_RESPONSE) {
            if (root->add_response_plugin_handler(type, property, handler) == NULL) {
                log_error("hook response point: %s is not mathc tair plugin",
                        handler->get_dll_name().c_str());
                return false;
            }
        }

        return true;
      }

      void plugins_manager::clean_plugins(plugins_root* &root)
      {
         log_debug("p_root = %p", root);
         if (root == NULL) return;
         for (int i = 0; i < MAX_PLUGINS; i++) {
             root->clean_request_type_plugins(i);
         }

         for (int i = 0; i < MAX_PLUGINS; i++) {
             root->clean_response_type_plugins(i);
         }

         delete root;
         root = NULL;
      }

      plugins_manager::~plugins_manager()
      {
         {
            tbsys::CThreadGuard guarder(&mutex);
            clean_plugins(root);
         }
      }

      bool plugins_manager::add_plugins(const set<string>& dll_names)
      {
         plugins_root* new_root = new plugins_root();
         plugins_root* old_root = NULL;
         tbsys::CThreadGuard guarder(&mutex);

         if (root != NULL) {
            (*new_root) = (*root);
         }

         for (set<string>::const_iterator it = dll_names.begin();
              it != dll_names.end(); it++) {
            if (add_plugin(*it, new_root) == false) {
               delete new_root;
               return false;
            }
         }

         old_root = root;
         root = new_root;
         usleep(WAIT_SECONDS);
         if (old_root != NULL) delete old_root;

         return true;
      }

      void plugins_manager::remove_plugins(const set<string>& dll_names)
      {
         plugins_root* new_root = new plugins_root();
         plugins_root* old_root = NULL;
         vector<plugin_handler*> vec_handler_will_delete;

         tbsys::CThreadGuard guarder(&mutex);

         if (root != NULL) {
            (*new_root) = (*root);
         }

         for (set<string>::const_iterator dll_name_it = dll_names.begin();
              dll_name_it != dll_names.end(); dll_name_it++) {
            plugin_handler *handler = NULL;
            for (int i = 0; i < MAX_PLUGINS; i++) {
                for(uint8_t j = 0; j < MAX_PROPERTYS; j++) {
                    bool flag = false;
                    handler = new_root->request_plugins[i][j];
                    if (handler != NULL && handler->get_dll_name() == (*dll_name_it)) {
                        new_root->request_plugins[i][j] = NULL;
                        vec_handler_will_delete.push_back(handler);
                        flag = true;
                    }
                    if (handler != NULL && handler->get_dll_name() == (*dll_name_it)) {
                        if (flag == true) {
                            new_root->response_plugins[i][j] = NULL;
                        } else {
                            handler = new_root->response_plugins[i][j];
                            new_root->response_plugins[i][j] = NULL;
                            vec_handler_will_delete.push_back(handler);
                        }
                    }
                }
            }
         }

         old_root = root;
         root = new_root;

         usleep(WAIT_SECONDS);

         for (vector<plugin_handler*>::iterator it = vec_handler_will_delete.begin();
              it != vec_handler_will_delete.end(); it++) {
            (*it)->get_instance()->clean();
            delete (*it);
         }

         if (old_root != NULL) delete old_root;
      }

      int plugins_manager::do_request_plugins(int plugin_type, uint32_t hashcode,
              base_packet *packet, plugins_root* &root)
      {
         root = this->root;   //copy on write so no lock here
         if (root == NULL) return 1;
         uint8_t start = root->get_request_plugins_start(plugin_type);
         uint8_t end = root->get_request_plugins_end(plugin_type);
         for (uint8_t i = start; i < end; i++) {
            if (root->request_plugins[plugin_type][i] != NULL) {
                base_plugin* p_plugin = root->request_plugins[plugin_type][i]->get_instance();
                if (p_plugin->get_plugin_type() == plugin_type) {
                    int ret = p_plugin->do_request(hashcode, packet);
                    if (ret < 0) {
                        return ret;
                    }
                }
            }
         }

         return 1;
      }

      void plugins_manager::do_response_plugins(int plugin_type, int ret, uint32_t hashcode,
              base_packet *packet, plugins_root* &root)
      {
         root = this->root;   //copy on write so no lock here
         if (root == NULL) return;
         uint8_t start = root->get_response_plugins_start(plugin_type);
         uint8_t end = root->get_response_plugins_end(plugin_type);
         for (uint8_t i = start; i < end; i++) {
            if (root->response_plugins[plugin_type][i] != NULL) {
                base_plugin* p_plugin = root->response_plugins[plugin_type][i]->get_instance();
                if (p_plugin->get_plugin_type() == plugin_type) {
                    p_plugin->do_response(ret, hashcode, packet);
                }
            }
         }
      }

      set<string>* plugins_manager::get_dll_names()
      {
         set<string>* res = new set<string>();
         if (res == NULL) {
             return NULL;
         }

         if (root == NULL) return res;
         tbsys::CThreadGuard guarder(&mutex);

         plugin_handler* handler = NULL;
         for(int i = 0; i < MAX_PLUGINS; i++) {
             uint8_t start = root->get_request_plugins_start(i);
             uint8_t end = root->get_request_plugins_end(i);
             for(uint8_t j = start; j < end; j++) {
                 handler = root->request_plugins[i][j];
                 if (handler) {
                     res->insert(handler->get_dll_name());
                 }
             }
         }

         for(int i = 0; i < MAX_PLUGINS; i++) {
             uint8_t start = root->get_response_plugins_start(i);
             uint8_t end = root->get_response_plugins_end(i);
             for(uint8_t j = start; j < end; j++) {
                 handler = root->response_plugins[i][j];
                 if (handler) {
                     res->insert(handler->get_dll_name());
                 }
             }
         }

         return res;
      }
//
//      set<int> plugins_manager::get_properties()
//      {
//         set<int> res;
//         if (root == NULL) return res;
//         tbsys::CThreadGuard guarder(&mutex);
//
//         plugin_handler* handler = NULL;
//         for(int i = 0; i < MAX_PLUGINS; i++) {
//             handler = root->request_plugins[i];
//             if (handler) {
//                 res.insert(handler->get_instance()->get_property());
//             }
//         }
//
//         for(int i = 0; i < MAX_PLUGINS; i++) {
//             handler = root->response_plugins[i];
//             if (handler) {
//                 res.insert(handler->get_instance()->get_property());
//             }
//         }
//
//         return res;
//      }

      bool plugins_manager::chang_plugins_to(const set<string>& dll_names)
      {
         set<string> should_delete_plugins;
         set<string> should_add_plugins;
         set<string>* org_plugins = get_dll_names();
         if (org_plugins == NULL) {
             return false;
         }

         set_difference(org_plugins->begin(), org_plugins->end(),
                        dll_names.begin(), dll_names.end(),
                        inserter(should_delete_plugins, should_delete_plugins.begin()));
         set_difference(dll_names.begin(), dll_names.end(),
                        org_plugins->begin(), org_plugins->end(),
                        inserter(should_add_plugins, should_add_plugins.begin()));

         if (!should_delete_plugins.empty()) {
            remove_plugins(should_delete_plugins);
         }
         if (!should_add_plugins.empty()) {
            delete org_plugins;
            return add_plugins(should_add_plugins);
         }

         delete org_plugins;
         return true;
      }
   }
}
