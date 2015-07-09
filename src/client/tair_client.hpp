/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tair_client.hpp 562 2012-02-08 09:44:00Z choutian.xmm@taobao.com $
 *
 * Authors:
 *   MaoQi <maoqi@taobao.com>
 *
 */
#ifndef TAIR_CLIENT_H
#define TAIR_CLIENT_H

#include <string>
#include <vector>
#include <map>
#include <signal.h>
#include <unistd.h>
#include <getopt.h>
#include <dirent.h>

#include <tbsys.h>
#include <tbnet.h>
#ifdef HAVE_LIBREADLINE
#include <readline/readline.h>
#include <readline/history.h>
#endif


#include "tair_client_api_impl.hpp"

using namespace std;

namespace tair {

#define CMD_MAX_LEN 4096

  typedef vector<char*> VSTRING;
  class tair_client;
  typedef void (tair_client::*cmd_call)(VSTRING &param);
  typedef map<string, cmd_call> str_cmdcall_map;
  typedef str_cmdcall_map::iterator str_cmdcall_map_iter;

  class tair_client {
    public:
      tair_client();
      ~tair_client();
      bool parse_cmd_line(int argc, char *const argv[]);
      bool start();
      void cancel();

    private:
      void print_usage(char *prog_name);
      uint64_t get_ip_address(char *str, int port);
      cmd_call parse_cmd(char *key, VSTRING &param);
      void print_help(const char *cmd);
      char *canonical_key(char *key, char **akey, int *size);

      void  do_cmd_quit(VSTRING &param);
      void  do_cmd_help(VSTRING &param);
      void  do_cmd_put(VSTRING &param);
      void  do_cmd_addcount(VSTRING &param);
      void  do_cmd_get(VSTRING &param);
      void  do_cmd_mget(VSTRING &param);
      void  do_cmd_remove(VSTRING &param);
      void  do_cmd_mremove(VSTRING &param);
      void  do_cmd_stat(VSTRING &param);
      void  do_cmd_remove_area(VSTRING &param);
      void  do_cmd_lazy_remove_area(VSTRING &param);
      void  do_cmd_dump_area(VSTRING &param);

      //list
      void do_cmd_lrpush(int pcode, VSTRING &param);
      void do_cmd_lrpop(int pcode, VSTRING &param);
      void do_cmd_lpush(VSTRING &param);
      void do_cmd_rpush(VSTRING &param);
      void do_cmd_lpop(VSTRING &param);
      void do_cmd_rpop(VSTRING &param);
      void do_cmd_lindex(VSTRING &param);

      //set
	  void do_cmd_scard(VSTRING &param);
      void do_cmd_sadd(VSTRING &param);
      void do_cmd_smembers(VSTRING &param);
      void do_cmd_srem(VSTRING &param);

      //hset
      void do_cmd_hset(VSTRING &param);
      void do_cmd_hget(VSTRING &param);
      void do_cmd_hmset(VSTRING &param);
      void do_cmd_hmget(VSTRING &param);
      void do_cmd_hgetall(VSTRING &param);
      void do_cmd_hdel(VSTRING &param);
	  
	  //sorted set
	  void do_cmd_zcard(VSTRING &param);//added 6.29
	  void do_cmd_zadd(VSTRING &param);
	  void do_cmd_zrem(VSTRING &param);//added 6.30
	  void do_cmd_zrange(VSTRING &param);
	  void do_cmd_zrevrange(VSTRING &param);//added 6.30
	  void do_cmd_zrangebyscore(VSTRING &param);
    private:
#ifdef HAVE_LIBREADLINE
      char *input(char *buffer, size_t size);
      void update_history(const char *line);
#endif
    private:
      str_cmdcall_map cmd_map;

      tair_client_impl client_helper;
      const char *server_addr;
      const char *slave_server_addr;
      char *group_name;
      char *cmd_line;
      char *cmd_file_name;
      bool is_config_server;
      bool is_cancel;
      int key_format;
      int default_area;
  };
}

#endif
///////////////////////END
