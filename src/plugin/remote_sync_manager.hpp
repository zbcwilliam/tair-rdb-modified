/*
 * (C) 2007-2011 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Authors:
 *   xinshu<xinshu.wzx@taobao.com>
 *
 */
#ifndef REMOTE_SYNC_MANAGER_H
#define REMOTE_SYNC_MANAGER_H

#include <sched.h>
#include <unistd.h>

#include <tbsys.h>
#include <tbnet.h>

#include "tair_client_api_impl.hpp"
#include "hash_packet_pair.hpp"
#include "binlog.hpp"
#include "local_queue.hpp"
#include "base_plugin.hpp"

namespace tair {

#define MAX_IP_PORT_LEN 22
#define MAX_GROUP_NAME 64
#define MAX_DUP_THREAD 7

    struct sync_conf
    {
        char master_ip[MAX_IP_PORT_LEN];
        char slave_ip[MAX_IP_PORT_LEN];
        char group_name[MAX_GROUP_NAME];
        char remote_data_dir[NAME_MAX];
        bool init(const char* _conf_str)
        {
            if(3 == sscanf(_conf_str,"{%[^,],%[^,],%[^}]}",master_ip,slave_ip,group_name))
            {
                if(strcmp(slave_ip,"nop") == 0) slave_ip[0] = '\0'; //slave_ip is not used.use nop.
                return true;
            }
            return false;
        }
    };

    class remote_sync_manager;
    class async_call_node
    {
        public:
            async_call_node(uint64_t _pkg_id, data_entry* _key,
                    remote_sync_manager* _pmanager)
            {
                pkg_id = _pkg_id;
                failed = 0;
                pkey = _key;
                pmanager = _pmanager;
            }
        public:
            uint64_t pkg_id;
            uint8_t failed;
            remote_sync_manager* pmanager;
            data_entry* pkey;
    };

    class remote_sync_manager :public plugin::base_plugin,public tbsys::CDefaultRunnable
    {
        public:
            remote_sync_manager(LocalQueue::serialize_func_ serializer,
                    LocalQueue::deserialize_func_ deserializer);
            ~remote_sync_manager();
        public:
            int get_hook_point();
            int get_property();
            int get_plugin_type();

            int do_request(uint32_t hashcode, base_packet* packet, void* exv = NULL) {return 0;}
            void do_response(int ret, uint32_t hashcode, base_packet* packet, void* exv = NULL);

            bool init();
            bool init(const string& para);
            void clean();
        public:
            void run(tbsys::CThread* thread, void* arg);
        private:
            const data_entry* try_get_key(base_packet *bpacket);
            void handle_send_queue();

            int do_send_packet(const uint32_t hashcode, base_packet* packet);

            bool init_tair_client(tair_client_impl& _client, struct sync_conf& _conf);
            bool parse_para(const char* para, struct sync_conf& _remote);

            static void callback_response(int error_code, void* arg);
        private:
            struct sync_conf _remote_conf;
            tair_client_impl _remote_tairclient;

            char m_base_home[NAME_MAX];
            char m_data_dir[NAME_MAX];
            bool m_inited;
        private:
            LocalQueue local_queue;
            BinLogWriter *log_push_failed;
            BinLogWriter *log_send_failed;
    };
}

#endif
