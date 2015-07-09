/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * get packet
 *
 * Version: $Id: get_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_GET_PACKET_H
#define TAIR_PACKET_GET_PACKET_H
#include "base_packet.hpp"
namespace tair {
   class request_get : public base_packet {
   public:
      request_get()
         {
         setPCode(TAIR_REQ_GET_PACKET);
         server_flag = 0;
         area = 0;
         key_count = 0;
         key = NULL;
         key_list = NULL;
      }

      request_get(uint16_t area, data_entry* key)
      {
         setPCode(TAIR_REQ_GET_PACKET);
         server_flag = 0;
         this->area = area;
         this->key = key;
         key_count = 0;
         key_list = NULL;
      }

      request_get(request_get &packet)
      {
         setPCode(TAIR_REQ_GET_PACKET);
         server_flag = packet.server_flag;
         area = packet.area;
         key_count = packet.key_count;
         key = NULL;
         key_list = NULL;
         if (packet.key != NULL) {
            key = new data_entry();
            key->clone(*packet.key);
         }
         if (packet.key_list != NULL) {
            key_list = new tair_dataentry_set();
            tair_dataentry_set::iterator it;
            for (it=packet.key_list->begin(); it!=packet.key_list->end(); ++it) {
               data_entry *pair = new data_entry(**it);
               key_list->insert(pair);
            }
         }
      }

      ~request_get()
        {
         if (key_list) {
            tair_dataentry_set::iterator it;
            for (it=key_list->begin(); it!=key_list->end(); ++it) {
               delete (*it);
            }
            delete key_list;
            key_list = NULL;
         }
         if (key) {
            delete key;
            key = NULL;
         }
      }

      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt8(server_flag);
         output->writeInt16(area);
         output->writeInt32(key_count);
         if (key_list != NULL) {
            tair_dataentry_set::iterator it;
            for (it=key_list->begin(); it!=key_list->end(); ++it) {
               data_entry *pair = (*it);
               pair->encode(output);
            }
         } else if (key != NULL) {
            key->encode(output);
         }

         return true;
      }

      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         HEADER_VERIFY;
         GETKEY_FROM_INT32(input,key_count);
         if (key_count == 1) {
            key = new data_entry();
            if (key == NULL) {
               return false;
            }
            key->decode(input);
         } else if (key_count > 1) {
            key_list = new tair_dataentry_set();
            pair<tair_dataentry_set::iterator,bool> ret;
            for(uint32_t i = 0; i < key_count; i++) {
               data_entry *pair = new data_entry();
               if (pair == NULL) {
                  return false;
               }
               pair->decode(input);
               ret = key_list->insert(pair);
               if (!ret.second) {
                  delete pair;
               }
            }
         }

         return true;
      }

      // add key
      void add_key(char *str_key, int str_size)
      {
         assert(str_size <= 2000);
         if (key_count == 0) {
            key = new data_entry(str_key, str_size);
            key_count ++;
         } else {
            if (key_list == NULL) {
               key_list = new tair_dataentry_set();
               key_list->insert(key);
               key = NULL;
            }
            data_entry *p = new data_entry(str_key, str_size);
            pair<tair_dataentry_set::iterator, bool> ret;
            ret = key_list->insert(p);
            if (!ret.second) {
               delete p;
            } else {
               key_count ++;
            }
         }
      }

   public:
      uint16_t           area;
      uint32_t           key_count;
      data_entry       *key;
      tair_dataentry_set  *key_list;

   };
   class response_get : public base_packet {
   public:

      response_get()
      {
         config_version = 0;
         setPCode(TAIR_RESP_GET_PACKET);
         key_count = 0;
         key = NULL;
         data = NULL;
         key_data_map = NULL;
         proxyed_key_list = NULL;
         code = 0;
      }


      ~response_get()
        {
         if (key_data_map != NULL) {
            vector<data_entry*> list;
            tair_keyvalue_map::iterator it;
            for (it=key_data_map->begin(); it!=key_data_map->end(); ++it) {
               list.push_back(it->first);
               if (it->second) {
                  list.push_back(it->second);
               }
            }
            for (uint32_t i=0; i<list.size(); i++) {
               delete list[i];
            }
            key_data_map->clear();
            delete key_data_map;
         }

         if (proxyed_key_list != NULL) {
            tair_dataentry_set::iterator it;
            for (it=proxyed_key_list->begin(); it!=proxyed_key_list->end(); ++it) {
               delete (*it);
            }
            delete proxyed_key_list;
            proxyed_key_list = NULL;
         }

         if (key) {
            delete key;
            key = NULL;
         }
         if (data) {
            delete data;
            data = NULL;
         }
      }


      bool encode(tbnet::DataBuffer *output)
      {
         output->writeInt32(config_version);
         output->writeInt32(code);
         output->writeInt32(key_count);
         if (key_data_map != NULL) {
            tair_keyvalue_map::iterator it;
            for (it=key_data_map->begin(); it!=key_data_map->end(); ++it) {
               data_entry *key = it->first;
               data_entry *data = it->second;
               key->encode(output);
               data->encode(output);

            }
            int pkc = 0;
            if (proxyed_key_list != NULL)
               proxyed_key_list->size();
            output->writeInt32(pkc);
            if (pkc > 0) {
               tair_dataentry_set::iterator it;
               for (it=proxyed_key_list->begin(); it!=proxyed_key_list->end(); ++it) {
                  data_entry *pk = (*it);
                  pk->encode(output);
               }
            }
         } else if (key != NULL && data != NULL) {
            key->encode(output);
            data->encode(output);
         }
         return true;
      }

      void set_code(int code)
      {
         this->code = code;
      }

      int get_code()
      {
         return code;
      }


      bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
      {
         if (header->_dataLen < 8) {
            log_warn( "buffer data too few.");
            return false;
         }
         config_version = input->readInt32();
         code = input->readInt32();
         key_count = input->readInt32();

         if (key_count > 1) {
            key_data_map = new tair_keyvalue_map();
            for (uint32_t i=0; i<key_count; i++) {
               data_entry *key = new data_entry();
               key->decode(input);
               // data
               data_entry *data = new data_entry();
               data->decode(input);

               key_data_map->insert(tair_keyvalue_map::value_type(key, data));
            }

            int pkc = input->readInt32();
            if (pkc > 0) {
               proxyed_key_list = new tair_dataentry_set();
               pair<tair_dataentry_set::iterator, bool> ret;
               for (int i=0; i<pkc; i++) {
                  data_entry *pair = new data_entry();
                  pair->decode(input);
                  ret = proxyed_key_list->insert(pair);
                  if (!ret.second)
                     delete pair;
               }
            }
         } else if (key_count == 1) {
            key = new data_entry();
            key->decode(input);

            data = new data_entry();
            data->decode(input);
         }

         return true;
      }

      // add key and data
      void add_key_data(data_entry *key, data_entry *data)
      {
         if (key_count == 0) {
            this->key = key;
            this->data = data;
            key_count ++;
         } else {
            if (key_data_map == NULL) {
               key_data_map = new tair_keyvalue_map;
               key_data_map->insert(tair_keyvalue_map::value_type(this->key, this->data));
               this->key = this->data = NULL;
            }
            tair_keyvalue_map::iterator it;
            it = key_data_map->find(key);
            if (it == key_data_map->end()) {
               key_data_map->insert(tair_keyvalue_map::value_type(key, data));
               key_count ++;
            } else {
               delete key;
               delete data;
            }
         }
      }

      void add_proxyed_key(data_entry *proxyed_Key_value)
      {
         if (proxyed_key_list == NULL) {
            proxyed_key_list = new tair_dataentry_set();
         }

         data_entry *p = new data_entry();
         p->clone(*proxyed_Key_value);
         pair<tair_dataentry_set::iterator, bool> ret;
         ret = proxyed_key_list->insert(p);
         if (ret.second == false)
            delete p;
      }

   public:
      uint32_t config_version;
      uint32_t key_count;
      data_entry *key;
      data_entry *data;
      tair_keyvalue_map *key_data_map;
      tair_dataentry_set  *proxyed_key_list;
   private:
      int code;
      response_get(const response_get&);
   };
}
#endif
