/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tair_client.cpp 562 2012-02-08 09:44:00Z choutian.xmm@taobao.com $
 *
 * Authors:
 *   MaoQi <maoqi@taobao.com>
 *
 */
#include "tair_client.hpp"
#include "define.hpp"
#include "util.hpp"
#include "dump_data_info.hpp"
#include "query_info_packet.hpp"

namespace tair {

   tair_client::tair_client()
   {
      is_config_server = false;
      //server_id = 0;
      //m_slaveServerId = 0;
      group_name = NULL;
      cmd_line = NULL;
      cmd_file_name = NULL;
      is_cancel = false;
      server_addr = NULL;
      slave_server_addr = NULL;

      key_format = 0;
      default_area = 0;

      cmd_map["help"] = &tair_client::do_cmd_help;
      cmd_map["quit"] = &tair_client::do_cmd_quit;
      cmd_map["exit"] = &tair_client::do_cmd_quit;
      cmd_map["put"] = &tair_client::do_cmd_put;
      cmd_map["incr"] = &tair_client::do_cmd_addcount;
      cmd_map["get"] = &tair_client::do_cmd_get;
      cmd_map["mget"] = &tair_client::do_cmd_mget;
      cmd_map["remove"] = &tair_client::do_cmd_remove;
      cmd_map["mremove"] = &tair_client::do_cmd_mremove;
      cmd_map["delall"] = &tair_client::do_cmd_remove_area;
      cmd_map["lazydelall"] = &tair_client::do_cmd_lazy_remove_area;
      cmd_map["dump"] = &tair_client::do_cmd_dump_area;
      cmd_map["stat"] = &tair_client::do_cmd_stat;

      cmd_map["lpush"] = &tair_client::do_cmd_lpush;
      cmd_map["rpush"] = &tair_client::do_cmd_rpush;
      cmd_map["lpop"] = &tair_client::do_cmd_lpop;
      cmd_map["rpop"] = &tair_client::do_cmd_rpop;
      cmd_map["lindex"] = &tair_client::do_cmd_lindex;

	  cmd_map["scard"] = &tair_client::do_cmd_scard;
	  cmd_map["sadd"] = &tair_client::do_cmd_sadd;
      cmd_map["srem"] = &tair_client::do_cmd_srem;
      cmd_map["smembers"] = &tair_client::do_cmd_smembers;

      cmd_map["hset"] = &tair_client::do_cmd_hset;
      cmd_map["hdel"] = &tair_client::do_cmd_hdel;
      cmd_map["hget"] = &tair_client::do_cmd_hget;
      cmd_map["hmset"] = &tair_client::do_cmd_hmset;
      cmd_map["hmget"] = &tair_client::do_cmd_hmget;
      cmd_map["hgetall"] = &tair_client::do_cmd_hgetall;

	  cmd_map["zcard"] = &tair_client::do_cmd_zcard;
	  cmd_map["zadd"] = &tair_client::do_cmd_zadd;
	  cmd_map["zrem"] = &tair_client::do_cmd_zrem;
	  cmd_map["zrange"] = &tair_client::do_cmd_zrange;
	  cmd_map["zrevrange"] = &tair_client::do_cmd_zrevrange;
	  cmd_map["zrangebyscore"] = &tair_client::do_cmd_zrangebyscore;
   }

   tair_client::~tair_client()
   {
      if (group_name) {
         free(group_name);
         group_name = NULL;
      }
      if (cmd_line) {
         free(cmd_line);
         cmd_line = NULL;
      }
      if (cmd_file_name) {
         free(cmd_file_name);
         cmd_file_name = NULL;
      }
    }

   void tair_client::print_usage(char *prog_name)
   {
      fprintf(stderr, "%s -s server:port\n OR\n%s -c configserver:port -g groupname\n\n"
              "    -s, --server           data server,default port:%d\n"
              "    -c, --configserver     default port: %d\n"
              "    -g, --groupname        group name\n"
              "    -l, --cmd_line         exec cmd\n"
              "    -q, --cmd_file         script file\n"
              "    -h, --help             print this message\n"
              "    -v, --verbose          print debug info\n"
              "    -V, --version          print version\n\n",
              prog_name, prog_name, TAIR_SERVER_DEFAULT_PORT, TAIR_CONFIG_SERVER_DEFAULT_PORT);
   }

   bool tair_client::parse_cmd_line(int argc, char *const argv[])
   {
      int opt;
      const char *optstring = "s:c:g:hVvl:q:";
      struct option longopts[] = {
         {"server", 1, NULL, 's'},
         {"configserver", 1, NULL, 'c'},
         {"groupname", 1, NULL, 'g'},
         {"cmd_line", 1, NULL, 'l'},
         {"cmd_file", 1, NULL, 'q'},
         {"help", 0, NULL, 'h'},
         {"verbose", 0, NULL, 'v'},
         {"version", 0, NULL, 'V'},
         {0, 0, 0, 0}
      };

      opterr = 0;
      while ((opt = getopt_long(argc, argv, optstring, longopts, NULL)) != -1) {
         switch (opt) {
            case 'c': {
               if (server_addr != NULL && is_config_server == false) {
                  return false;
               }
               //uint64_t id = tbsys::CNetUtil::strToAddr(optarg, TAIR_CONFIG_SERVER_DEFAULT_PORT);

               if (server_addr == NULL) {
                  server_addr = optarg;
               } else {
                  slave_server_addr = optarg;
               }
               is_config_server = true;
            }
               break;
            case 's': {
               if (server_addr != NULL && is_config_server == true) {
                  return false;
               }
               server_addr = optarg;
               //server_id = tbsys::CNetUtil::strToAddr(optarg, TAIR_SERVER_DEFAULT_PORT);
            }
               break;
            case 'g':
               group_name = strdup(optarg);
               break;
            case 'l':
               cmd_line = strdup(optarg);
               break;
            case 'q':
               cmd_file_name = strdup(optarg);
               break;
            case 'v':
               TBSYS_LOGGER.setLogLevel("DEBUG");
               break;
            case 'V':
               fprintf(stderr, "BUILD_TIME: %s %s\n", __DATE__, __TIME__);
               return false;
            case 'h':
               print_usage(argv[0]);
               return false;
         }
      }
      if (server_addr == NULL || (is_config_server == true && group_name == NULL)) {
         print_usage(argv[0]);
         return false;
      }
      return true;
   }

   cmd_call tair_client::parse_cmd(char *key, VSTRING &param)
   {
      cmd_call  cmdCall = NULL;
      char *token;
      while (*key == ' ') key++;
      token = key + strlen(key);
      while (*(token-1) == ' ' || *(token-1) == '\n' || *(token-1) == '\r') token --;
      *token = '\0';
      if (key[0] == '\0') {
         return NULL;
      }
      token = strchr(key, ' ');
      if (token != NULL) {
         *token = '\0';
      }
      str_cmdcall_map_iter it = cmd_map.find(tbsys::CStringUtil::strToLower(key));
      if (it == cmd_map.end()) {
         return NULL;
      } else {
         cmdCall = it->second;
      }
      if (token != NULL) {
         token ++;
         key = token;
      } else {
         key = NULL;
      }
      param.clear();
      while ((token = strsep(&key, " ")) != NULL) {
         if (token[0] == '\0') continue;
         param.push_back(token);
      }
      return cmdCall;
   }

   void tair_client::cancel()
   {
      is_cancel = true;
   }

   bool tair_client::start()
   {
      bool done = true;
      client_helper.set_timeout(5000);
      if (is_config_server) {
         done = client_helper.startup(server_addr, slave_server_addr, group_name);
      } else {
         //done = client_helper.startup(server_id);
         done = false;
      }
      if (done == false) {
         fprintf(stderr, "%s cann't connect.\n", server_addr);
         return false;
      }

      char buffer[CMD_MAX_LEN];
      VSTRING param;

      if (cmd_line != NULL) {
         strcpy(buffer, cmd_line);
         vector<char*> cmd_list;
         tbsys::CStringUtil::split(buffer, ";", cmd_list);
         for(uint32_t i=0; i<cmd_list.size(); i++) {
            cmd_call this_cmd_call = parse_cmd(cmd_list[i], param);
            if (this_cmd_call == NULL) {
               continue;
            }
            (this->*this_cmd_call)(param);
         }
      } else if (cmd_file_name != NULL) {
         FILE *fp = fopen(cmd_file_name, "rb");
         if (fp != NULL) {
            while(fgets(buffer, CMD_MAX_LEN, fp)) {
               cmd_call this_cmd_call = parse_cmd(buffer, param);
               if (this_cmd_call == NULL) {
                  fprintf(stderr, "unknown command.\n\n");
                  continue;
               }
               (this->*this_cmd_call)(param);
            }
            fclose(fp);
         } else {
            fprintf(stderr, "open failure: %s\n\n", cmd_file_name);
         }
      } else {
         while (done) {
#ifdef HAVE_LIBREADLINE
             char *line = input(buffer, CMD_MAX_LEN);
             if (line == NULL) break;
#else
             if (fprintf(stderr, "TAIR> ") < 0) break;
             if (fgets(buffer, CMD_MAX_LEN, stdin) == NULL) {
                 break;
             }
#endif
            cmd_call this_cmd_call = parse_cmd(buffer, param);
            if (this_cmd_call == NULL) {
               fprintf(stderr, "unknown command.\n\n");
               continue;
            }
            if (this_cmd_call == &tair_client::do_cmd_quit) {
               break;
            }
            (this->*this_cmd_call)(param);
            is_cancel = false;
         }
      }

      // stop
      client_helper.close();

      return true;
   }

#ifdef HAVE_LIBREADLINE
   char* tair_client::input(char *buffer, size_t size) {
     static const char *prompt = "[0;31;40mTAIR>[0;37;40m ";
     char *line = NULL;
     while((line = readline(prompt)) != NULL) {
         if (*line == '\0') {
             free(line);
             HIST_ENTRY *hist = previous_history();
             if (hist != NULL) {
                 strncpy(buffer, hist->line, size);
                 return buffer;
             }
             continue;
         }
         update_history(line);
         strncpy(buffer, line, size);
         free(line);
         return buffer;
     }
     return NULL;
   }
   void tair_client::update_history(const char *line) {
     int i = 0;
     HIST_ENTRY **the_history = history_list();
     for (; i < history_length; ++i) {
       HIST_ENTRY *hist = the_history[i];
       if (strcmp(hist->line, line) == 0) {
         break;
       }
     }
     if (i == history_length) {
       add_history(line);
       return ;
     }
     HIST_ENTRY *hist = the_history[i];
     for (; i < history_length - 1; ++i) {
       the_history[i] = the_history[i+1];
     }
     the_history[i] = hist;
   }
#endif

///////////////////////////////////////////////////////////////////////////////////////////////////
   void tair_client::do_cmd_quit(VSTRING &param)
   {
      return ;
   }

   void tair_client::print_help(const char *cmd)
   {
      if (cmd == NULL || strcmp(cmd, "put") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : put key data [area] [expired]\n"
                 "DESCRIPTION: area   - namespace , default: 0\n"
                 "             expired- in seconds, default: 0,never expired\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "incr") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : incr key [count] [initValue] [area]\n"
                 "DESCRIPTION: initValue , default: 0\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "get") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : get key [area]\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "zcard") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : zcard key [area]\n"
                 "DESCRIPTION: area   - namespace , default: 0\n"
            );
      }else if (cmd == NULL || strcmp(cmd, "zadd") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : zadd key score value [area] [version] [expired]\n"
                 "DESCRIPTION: area   - namespace , default: 0\n"
                 "             expired- in seconds, default: 0,never expired\n"
            );
      }else if (cmd == NULL || strcmp(cmd, "zrem") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : zrem key value [area] [version] [expired]\n"
            );
      }else if (cmd == NULL || strcmp(cmd, "zrange") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : zrange key start end [area] [withscore]\n"
            );
      }else if (cmd == NULL || strcmp(cmd, "zrevrange") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : zrevrange key start end [area] [withscore]\n"
            );
      }else if (cmd == NULL || strcmp(cmd, "zrangebyscore") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : zrangebyscore key start end [area] [withscore]\n"
            );
      }else if (cmd == NULL || strcmp(cmd, "mget") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : mget key1 ... keyn area\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "remove") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : remove key [area]\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "mremove") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : mremove key1 ... keyn area\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "lpush") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : lpush key count value1 ... valuen [area] [version] [expired]\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "rpush") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : rpush key count value1 ... valuen [area] [version] [expired]\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "lpop") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : lpop key count [area] [version] [expired]\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "rpop") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : rpop key count [area] [version] [expired]\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "lindex") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : lindex key index area\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "scard") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : scard key [area]\n"
                 "DESCRIPTION: area   - namespace , default: 0\n"
            );
      }else if (cmd == NULL || strcmp(cmd, "sadd") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : sadd key value [area] [version] [expired]\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "smembers") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : smembers key [area]\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "srem") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : srem key value [area] [version] [expired]\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "hset") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : hset key field value [area] [version] [expired]\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "hget") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : hget key field [area]\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "hmset") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : hmset key count field value... [area] [version] [expired]\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "hgetall") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : hgetall key [area]\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "hmget") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : hmget key count field... [area]\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "hdel") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : hdel key field [area] [version] [expired]\n"
            );
      } else if (cmd == NULL || strcmp(cmd, "delall") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : stat\n"
                 "DESCRIPTION: get stat info"
            );
      } else if (cmd == NULL || strcmp(cmd, "delall") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : stat\n"
                 "DESCRIPTION: get stat info"
            );
      } else if (cmd == NULL || strcmp(cmd, "stat") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : delall area\n"
                 "DESCRIPTION: delete all data of [area]"
            );
      } else if (cmd == NULL || strcmp(cmd, "dump") == 0) {
         fprintf(stderr,
                 "------------------------------------------------\n"
                 "SYNOPSIS   : dump dumpinfo.txt\n"
                 "DESCRIPTION: dumpinfo.txt is a config file of dump,syntax:\n"
                 "area start_time end_time,eg:"
                 "10 2008-12-09 12:08:07 2008-12-10 12:10:00"
            );
      }

      fprintf(stderr, "\n");
   }

   void tair_client::do_cmd_help(VSTRING &param)
   {
      if (param.size() == 0U) {
         print_help(NULL);
      } else {
         print_help(param[0]);
      }
      return;
   }

   char *tair_client::canonical_key(char *key, char **akey, int *size)
   {
      char *pdata = key;
      if (key_format == 1) { // java
         *size = strlen(key)+2;
         pdata = (char*)malloc(*size);
         pdata[0] = '\0';
         pdata[1] = '\4';
         memcpy(pdata+2, key, strlen(key));
         *akey = pdata;
      } else if (key_format == 2) { // raw
         pdata = (char*)malloc(strlen(key)+1);
         util::string_util::conv_raw_string(key, pdata, size);
         *akey = pdata;
      } else {
         *size = strlen(key)+1;
      }

      return pdata;
   }

   void tair_client::do_cmd_put(VSTRING &param)
   {
      if (param.size() < 2U || param.size() > 4U) {
         print_help("put");
         return;
      }
      int area = default_area;
      int expired = 0;
      if (param.size() > 2U) area = atoi(param[2]);
      if (param.size() > 3U) expired = atoi(param[3]);

      char *akey = NULL;
      int pkeysize = 0;
      char *pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);
      data_entry data(param[1], false);

      int ret = client_helper.put(area, key, data, expired, 0);
      fprintf(stderr, "put: %s\n", client_helper.get_error_msg(ret));

      if (akey) free(akey);
   }

   void tair_client::do_cmd_addcount(VSTRING &param)
   {
      if (param.size() < 1U) {
         print_help("incr");
         return ;
      }
      int count = 1;
      int initValue = 0;
      int area = default_area;
      if (param.size() > 1U) count = atoi(param[1]);
      if (param.size() > 2U) initValue = atoi(param[2]);
      if (param.size() > 3U) area = atoi(param[3]);

      char *akey = NULL;
      int pkeysize = 0;
      char *pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);

      // put
      int retCount = 0;
      int ret = client_helper.add_count(area, key, count, &retCount, initValue);

      if (ret != TAIR_RETURN_SUCCESS) {
         fprintf(stderr, "add failed:%s.\n", client_helper.get_error_msg(ret));
      } else {
         fprintf(stderr, "retCount: %d\n", retCount);
      }
      if (akey) free(akey);

      return;
   }

   void tair_client::do_cmd_get(VSTRING &param)
   {
      if (param.size() < 1U || param.size() > 2U ) {
         print_help("get");
         return;
      }
      int area = default_area;
      if (param.size() >= 2U) {
         char *p=param[1];
         if (*p == 'n') {   
            area |= TAIR_FLAG_NOEXP;  //    (0x0c000000)
            p ++;
         }
         area |= atoi(p);
      }
      char *akey = NULL;
      int pkeysize = 0;
      char *pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);
      data_entry *data = NULL;

      // get
      int ret = client_helper.get(area, key, data);
      if (ret != TAIR_RETURN_SUCCESS) {
         fprintf(stderr, "get failed: %s.\n",client_helper.get_error_msg(ret));
      } else if (data != NULL) {
         char *p = util::string_util::conv_show_string(data->get_data(), data->get_size());
         fprintf(stderr, "KEY: %s, LEN: %d\n raw data: %s, %s\n", param[0], data->get_size(), data->get_data(), p);
         free(p);
         delete data;
      }
      if (akey) free(akey);
      return ;
   }


  //added 6.29
  void tair_client::do_cmd_zcard(VSTRING &param) {
		if (param.size() < 1U || param.size() > 2U ) {
		   print_help("zcard");
		   return;
		}
		int area = default_area;
  
		if (param.size() >= 2U) {
			char *p=param[2];
			if (*p == 'n') {
				area |= TAIR_FLAG_NOEXP;
				p ++;
			}
			area |= atoi(p);
		}
  
		char *akey = NULL;
		int pkeysize = 0;
		char* pkey = canonical_key(param[0], &akey, &pkeysize);
		data_entry key(pkey, pkeysize, false);

		long long retnum=0;
  
		int ret = client_helper.zcard(area, key, retnum);
		cout << "the number of sorted set is: " << retnum <<std::endl;
		fprintf(stderr, "zcard: %s\n", client_helper.get_error_msg(ret));
		if (akey) free(akey);
		return ;
	 }

  //zadd
  /*int tair_client_impl::zadd(const int area, const data_entry &key, const double score, const data_entry &value,
          const int expire, const int version)

          "SYNOPSIS   : zadd key score value [area] [version] [expired]\n"
    */
   
  void tair_client::do_cmd_zadd(VSTRING &param) {
	 if (param.size() < 3U || param.size() > 6U ) {
		print_help("zadd");
		return;
	 }
	 
 	 double score = atof(param[1]);
	 
	 int area = default_area;
	 if (param.size() >= 4U) {
		 char *p=param[3];
		 if (*p == 'n') {
			 area |= TAIR_FLAG_NOEXP;
			 p ++;
		 }
		 area |= atoi(p);
	 }
  
	 char *akey = NULL;
	 int pkeysize = 0;
	 char* pkey = canonical_key(param[0], &akey, &pkeysize);
	 data_entry key(pkey, pkeysize, false);
	 data_entry value(param[2], false);   //monitor sadd;
  
	 int version = NOT_CARE_VERSION;
	 if (param.size() >= 5U) {
		 version = atoi(param[4]);
	 }
  
	 int expire = NOT_CARE_EXPIRE;
	 if (param.size() >= 6U) {
		 expire = atoi(param[5]);
	 }
  
	 int ret = client_helper.zadd(area, key, score, value, expire, version);
	 fprintf(stderr, "zadd: %s\n", client_helper.get_error_msg(ret));
	 if (akey) free(akey);
	 return ;
  }


  void tair_client::do_cmd_zrem(VSTRING &param) {
      if (param.size() < 2U || param.size() > 5U ) {
         print_help("zrem");
         return;
      }
      int area = default_area;

      if (param.size() >= 3U) {
          char *p=param[2];
          if (*p == 'n') {
              area |= TAIR_FLAG_NOEXP;
              p ++;
          }
          area |= atoi(p);
      }

      char *akey = NULL;
      int pkeysize = 0;
      char* pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);

      data_entry value(param[1], false);

      int version = NOT_CARE_VERSION;
      if (param.size() >= 4U) {
          version = atoi(param[3]);
      }

      int expire = NOT_CARE_EXPIRE;
      if (param.size() >= 5U) {
          expire = atoi(param[4]);
      }

      int ret = client_helper.zrem(area, key, value, expire, version);
      fprintf(stderr, "zrem: %s\n", client_helper.get_error_msg(ret));
      if (akey) free(akey);
      return ;
   }
   

   //zrange
   /*
   
   int zrange (const int area, const data_entry & key, int32_t start,int32_t end,
	   vector <data_entry *> &values, vector<double> &scores, int32_t withscore);

	   "SYNOPSIS   : zrange key start end [area] [withscore]\n"
    */

   
   void tair_client::do_cmd_zrange(VSTRING &param)
	  {
		 if (param.size() < 3U || param.size() > 5U ) {
			print_help("zrange");
			return;
		 }

		 int32_t start =atoi(param[1]);
		 int32_t end =atoi(param[2]);
		 
		 int area = default_area;
		 if (param.size() >= 4U) {
			char *p=param[3];
			if (*p == 'n') {
			   area |= TAIR_FLAG_NOEXP;
			   p ++;
			}
			area |= atoi(p);
		 }


		 //int limit = 0; // print_help does not have the indicator limit;
		 
		 int32_t withscore = 0;
		 if (param.size() >= 5U) {
		 	withscore = atoi(param[4]);
		 	}


		 char *akey = NULL;
		 int pkeysize = 0;
		 char *pkey = canonical_key(param[0], &akey, &pkeysize);
		 data_entry key(pkey, pkeysize, false);  //at this moment, key was given its' own param;
		 
		 vector<data_entry*> dataset; //param->values; 
		 vector<double> scoreset;  //param->scores
		 
		 //zrange
		 int ret = client_helper.zrange(area, key, start, end, dataset, scoreset, withscore);
		 /*for(size_t i=0; i<dataset.size();++i) {
		       printf("dataset[%d] is %s\n",i,dataset[i]->get_data());
         }*///added 6.25
		 if (ret != TAIR_RETURN_SUCCESS) {
			fprintf(stderr, "zrange failed: %s.\n",client_helper.get_error_msg(ret));
		 } else if (dataset.size() > 0) {
			fprintf(stderr, "KEY: %s\n", param[0]);
			//printf("dataset.size is: %d\n",dataset.size());
			for(size_t i = 0; i < dataset.size(); i++) {
				
			   data_entry* data = dataset[i];
			   //printf("data address is: %d\n",&data);
			   //printf("data->size is: %d\n",data->get_size());
			   //printf("data->size is: %s\n",data->get_data());
			   
			   if (data == NULL) {
				   continue;
			   }
			   char *p = util::string_util::conv_show_string(data->get_data(), data->get_size());
			   fprintf(stderr, "LEN: %d\n, raw data: %s, %s\n", data->get_size(), data->get_data(), p);
			   if (p) free(p);
			   delete data;
			   
			}
			dataset.clear();
		 }
		 if (akey) free(akey);
		 return ;
		 
		 
	  }

   //zrevrange added 6.30
   void tair_client::do_cmd_zrevrange(VSTRING &param)
	  {
		 if (param.size() < 3U || param.size() > 5U ) {
			print_help("zrevrange");
			return;
		 }

		 int32_t start =atoi(param[1]);
		 int32_t end =atoi(param[2]);
		 
		 int area = default_area;
		 if (param.size() >= 4U) {
			char *p=param[3];
			if (*p == 'n') {
			   area |= TAIR_FLAG_NOEXP;
			   p ++;
			}
			area |= atoi(p);
		 }


		 //int limit = 0; // print_help does not have the indicator limit;
		 
		 int32_t withscore = 0;
		 if (param.size() >= 5U) {
		 	withscore = atoi(param[4]);
		 	}


		 char *akey = NULL;
		 int pkeysize = 0;
		 char *pkey = canonical_key(param[0], &akey, &pkeysize);
		 data_entry key(pkey, pkeysize, false);  //at this moment, key was given its' own param;
		 
		 vector<data_entry*> dataset; //param->values; 
		 vector<double> scoreset;  //param->scores
		 
		 //zrange
		 int ret = client_helper.zrevrange(area, key, start, end, dataset, scoreset, withscore);
		 /*for(size_t i=0; i<dataset.size();++i) {
		       printf("dataset[%d] is %s\n",i,dataset[i]->get_data());
         }*///added 6.25
		 if (ret != TAIR_RETURN_SUCCESS) {
			fprintf(stderr, "zrevrange failed: %s.\n",client_helper.get_error_msg(ret));
		 } else if (dataset.size() > 0) {
			fprintf(stderr, "KEY: %s\n", param[0]);
			//printf("dataset.size is: %d\n",dataset.size());
			for(size_t i = 0; i < dataset.size(); i++) {
				
			   data_entry* data = dataset[i];
			   //printf("data address is: %d\n",&data);
			   //printf("data->size is: %d\n",data->get_size());
			   //printf("data->size is: %s\n",data->get_data());
			   
			   if (data == NULL) {
				   continue;
			   }
			   char *p = util::string_util::conv_show_string(data->get_data(), data->get_size());
			   fprintf(stderr, "LEN: %d\n, raw data: %s, %s\n", data->get_size(), data->get_data(), p);
			   if (p) free(p);
			   delete data;
			   
			}
			dataset.clear();
		 }
		 if (akey) free(akey);
		 return ;
		 
		 
	  }


   
   //zrangebyscore
   /*
   
   int zrangebyscore (const int area, const data_entry & key, const double start,const double end,
	   vector <data_entry *> &values, vector<double> &scores,const int limit,const int withscore);

	   "SYNOPSIS   : zrangebyscore key start end [area] [withscore]\n"
    */

   
   void tair_client::do_cmd_zrangebyscore(VSTRING &param)
	  {
		 if (param.size() < 3U || param.size() > 5U ) {
			print_help("zrangebyscore");
			return;
		 }

		 double start = atof(param[1]);
		 double end = atof(param[2]);
		 
		 int area = default_area;
		 if (param.size() >= 4U) {
			char *p=param[3];
			if (*p == 'n') {
			   area |= TAIR_FLAG_NOEXP;
			   p ++;
			}
			area |= atoi(p);
		 }


		 int limit = 0; // print_help does not have the indicator limit;
		 
		 int withscore = 0;
		 if (param.size() >= 5U) {
		 	withscore = atoi(param[4]);
		 	}


		 char *akey = NULL;
		 int pkeysize = 0;
		 char *pkey = canonical_key(param[0], &akey, &pkeysize);
		 data_entry key(pkey, pkeysize, false);
		 
		 vector<data_entry*> dataset; //param->values
		 vector<double> scoreset;  //param->scores
		 
		 //zrangebyscore
		 int ret = client_helper.zrangebyscore(area, key, start, end, dataset, scoreset, limit, withscore);
		 /*for(size_t i=0; i<dataset.size();++i) {
		       printf("dataset[%d] is %s\n",i,dataset[i]->get_data());
         }*///added 6.25
		 if (ret != TAIR_RETURN_SUCCESS) {
			fprintf(stderr, "zrangebyscore failed: %s.\n",client_helper.get_error_msg(ret));
		 } else if (dataset.size() > 0) {
			fprintf(stderr, "KEY: %s\n", param[0]);
			//printf("dataset.size is: %d\n",dataset.size());
			for(size_t i = 0; i < dataset.size(); i++) {
			   data_entry* data = dataset[i];
			   //printf("data address is: %d\n",&data);
			   //printf("data->size is: %d\n",data->get_size());
			   //printf("data->get_data() is: %s\n",data->get_data());
			   if (data == NULL) {
				   continue;
			   }
			   char *p = util::string_util::conv_show_string(data->get_data(), data->get_size());
			   fprintf(stderr, "LEN: %d\n, raw data: %s, %s\n", data->get_size(), data->get_data(), p);
			   if (p) free(p);
			   delete data;
			   
			}
			dataset.clear();
		 }
		 if (akey) free(akey);
		 return ;
	  }


   void tair_client::do_cmd_mget(VSTRING &param)
   {
      if (param.size() < 2U) {
         print_help("mget");
         return;
      }
      int area = default_area;
      char *p=param[param.size() -1];
      if (*p == 'n') {
        area |= TAIR_FLAG_NOEXP;
        p ++;
      }
      area |= atoi(p);
      fprintf(stderr, "mget area: %d\n", area);

      vector<data_entry*> keys;
      for (int i = 0; i < static_cast<int>(param.size() - 1); ++i)
      {
        char *akey = NULL;
        int pkeysize = 0;
        fprintf(stderr, "mget key index: %u, key: %s\n", i, param[i]);
        char *pkey = canonical_key(param[i], &akey, &pkeysize);
        data_entry* key = new data_entry(pkey, pkeysize, false);
        keys.push_back(key);
        if (akey) free(akey);
      }
      tair_keyvalue_map datas;

      // mget
      int ret = client_helper.mget(area, keys, datas);
      if (ret == TAIR_RETURN_SUCCESS || ret == TAIR_RETURN_PARTIAL_SUCCESS)
      {
        tair_keyvalue_map::iterator mit = datas.begin();
        for ( ; mit != datas.end(); ++mit)
        {
          char *key = util::string_util::conv_show_string(mit->first->get_data(), mit->first->get_size());
          char *data = util::string_util::conv_show_string(mit->second->get_data(), mit->second->get_size());
          fprintf(stderr, "KEY: %s, RAW VALUE: %s, VALUE: %s, LEN: %d\n",
              key, mit->second->get_data(), data, mit->second->get_size());
          free(key);
          free(data);
        }
        fprintf(stderr, "get success, ret: %d.\n", ret);
      }
      else
      {
         fprintf(stderr, "get failed: %s, ret: %d.\n", client_helper.get_error_msg(ret), ret);
      }

      vector<data_entry*>::iterator vit = keys.begin();
      for ( ; vit != keys.end(); ++vit)
      {
        delete *vit;
        (*vit) = NULL;
      }
      tair_keyvalue_map::iterator kv_mit = datas.begin();
      for ( ; kv_mit != datas.end(); )
      {
        data_entry* key = kv_mit->first;
        data_entry* value = kv_mit->second;
        datas.erase(kv_mit++);
        delete key;
        key = NULL;
        delete value;
        value = NULL;
      }
      return ;
   }

   
// remove
   void tair_client::do_cmd_remove(VSTRING &param)
   {
      if (param.size() < 2U) {
         print_help("remove");
         return;
      }

      int area = default_area;
      if (param.size() >= 2U) area = atoi(param[1]);

      char *akey = NULL;
      int pkeysize = 0;
      char *pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);

      int ret = client_helper.remove(area, key);
      fprintf(stderr, "remove: %s.\n", client_helper.get_error_msg(ret));
      if (akey) free(akey);
      return ;
   }

   void tair_client::do_cmd_mremove(VSTRING &param)
   {
      if (param.size() < 2U ) {
         print_help("mremove");
         return;
      }
      int area = default_area;
      char *p=param[param.size() -1];
      if (*p == 'n') {
        area |= TAIR_FLAG_NOEXP;
        p ++;
      }
      area |= atoi(p);
      fprintf(stderr, "mremove area: %d\n", area);

      vector<data_entry*> keys;
      for (int i = 0; i < static_cast<int>(param.size() - 1); ++i)
      {
        char *akey = NULL;
        int pkeysize = 0;
        fprintf(stderr, "mremove key index: %u, key: %s\n", i, param[i]);
        char *pkey = canonical_key(param[i], &akey, &pkeysize);
        data_entry* key = new data_entry(pkey, pkeysize, false);
        keys.push_back(key);
        if (akey) free(akey);
        //todo delete key
      }

      int ret = client_helper.mdelete(area, keys);
      fprintf(stderr, "mremove: %s, ret: %d\n", client_helper.get_error_msg(ret), ret);
      vector<data_entry*>::iterator vit = keys.begin();
      for ( ; vit != keys.end(); ++vit)
      {
        delete *vit;
        (*vit) = NULL;
      }
      return ;
   }

   void tair_client::do_cmd_stat(VSTRING &param) {

       map<string, string> out_info;
       map<string, string>::iterator it;
       string group = group_name;
       client_helper.query_from_configserver(request_query_info::Q_AREA_CAPACITY, group, out_info);
       fprintf(stderr,"%20s %20s\n","area","quota");
       for (it=out_info.begin(); it != out_info.end(); it++) {
           fprintf(stderr,"%20s %20s\n", it->first.c_str(), it->second.c_str());
       }
       fprintf(stderr,"\n");

       fprintf(stderr,"%20s %20s\n","server","status");
       client_helper.query_from_configserver(request_query_info::Q_DATA_SEVER_INFO, group, out_info);
       for (it=out_info.begin(); it != out_info.end(); it++) {
           fprintf(stderr,"%20s %20s\n", it->first.c_str(), it->second.c_str());
       }

       fprintf(stderr,"\n");

       for (it=out_info.begin(); it != out_info.end(); it++) {
           map<string, string> out_info2;
           map<string, string>::iterator it2;
           client_helper.query_from_configserver(request_query_info::Q_STAT_INFO, group,
                   out_info2, tbsys::CNetUtil::strToAddr(it->first.c_str(), 0));
           for (it2=out_info2.begin(); it2 != out_info2.end(); it2++) {
               fprintf(stderr,"%s : %s %s\n",it->first.c_str(), it2->first.c_str(), it2->second.c_str());
           }
       }
       map<string, string> out_info2;
       map<string, string>::iterator it2;
       client_helper.query_from_configserver(request_query_info::Q_STAT_INFO, group, out_info2, 0);
       for (it2=out_info2.begin(); it2 != out_info2.end(); it2++) {
           fprintf(stderr,"%s %s\n", it2->first.c_str(), it2->second.c_str());
       }
   }

   void tair_client::do_cmd_lazy_remove_area(VSTRING &param)
   {
      if (param.size() != 1U) {
         print_help("lazy delall");
         return;
      }
      int area = atoi(param[0]);
      if(area < 0){
         return;
      }
      // remove
      int ret = client_helper.remove_area(area, true);
      fprintf(stderr, "removeArea: area:%d,%s\n", area,client_helper.get_error_msg(ret));
      return;
   }

   void tair_client::do_cmd_remove_area(VSTRING &param)
   {
      if (param.size() != 1U) {
         print_help("delall");
         return;
      }
      int area = atoi(param[0]);
      if(area < 0){
         return;
      }
      // remove
      int ret = client_helper.remove_area(area, false);
      fprintf(stderr, "removeArea: area:%d,%s\n", area,client_helper.get_error_msg(ret));
      return;
   }

   void tair_client::do_cmd_dump_area(VSTRING &param)
   {
      if (param.size() != 1U) {
         print_help("dump");
         return;
      }

      FILE *fp = fopen(param[0],"r");
      if (fp == NULL){
         fprintf(stderr,"open file %s failed",param[0]);
         return;
      }

      char buf[1024];
      struct tm start_time;
      struct tm end_time;
      set<dump_meta_info> dump_set;
      while(fgets(buf,sizeof(buf),fp) != NULL){
         dump_meta_info info;
         if (sscanf(buf,"%d %4d-%2d-%2d %2d:%2d:%2d %4d-%2d-%2d %2d:%2d:%2d",\
                    &info.area,\
                    &start_time.tm_year,&start_time.tm_mon,\
                    &start_time.tm_mday,&start_time.tm_hour,\
                    &start_time.tm_min,&start_time.tm_sec,
                    &end_time.tm_year,&end_time.tm_mon,\
                    &end_time.tm_mday,&end_time.tm_hour,\
                    &end_time.tm_min,&end_time.tm_sec) != 13){
            fprintf(stderr,"syntax error : %s",buf);
            continue;
         }

         start_time.tm_year -= 1900;
         end_time.tm_year -= 1900;
         start_time.tm_mon -= 1;
         end_time.tm_mon -= 1;

         if (info.area < -1 || info.area >= TAIR_MAX_AREA_COUNT){
            fprintf(stderr,"ilegal area");
            continue;
         }

         time_t tmp_time = -1;
         if ( (tmp_time = mktime(&start_time)) == static_cast<time_t>(-1)){
            fprintf(stderr,"incorrect time");
            continue;
         }
         info.start_time = static_cast<uint32_t>(tmp_time);

         if ( (tmp_time = mktime(&end_time)) == static_cast<time_t>(-1)){
            fprintf(stderr,"incorrect time");
            continue;
         }
         info.end_time = static_cast<uint32_t>(tmp_time);

         if (info.start_time < info.end_time){
            std::swap(info.start_time,info.end_time);
         }
         dump_set.insert(info);
      }
      fclose(fp);

      if (dump_set.empty()){
         fprintf(stderr,"what do you want to dump?");
         return;
      }

      // dump
      int ret = client_helper.dump_area(dump_set);
      fprintf(stderr, "dump : %s\n",client_helper.get_error_msg(ret));
      return;
   }

   void tair_client::do_cmd_lrpush(int pcode, VSTRING &param) {
      if (param.size() < 3U) {
        if (pcode == 0) {
          print_help("lpush");
        } else {
          print_help("rpush");
        }
        return;
      }

      int count = atoi(param[1]);
      if (count < 1) {
        if (pcode == 0) {
          print_help("lpush");
        } else {
          print_help("rpush");
        }
        return;
      }

      int area = default_area;
      int expire = 0;
      int version = 0;
      size_t icount = count + 2;
      if (++icount <= param.size()) {
        char *p=param[icount - 1];
        if (*p == 'n') {
          area |= TAIR_FLAG_NOEXP;
          p ++;
        }
        area |= atoi(p);
      }
      if (++icount <= param.size()) {
        char *p=param[icount - 1];
        version = atoi(p);
      }
      if (++icount <= param.size()) {
        char *p=param[icount - 1];
        expire = atoi(p);
      }

      char *akey = NULL;
      int pkeysize = 0;
      char *pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);
      std::vector<data_entry*> values;

      icount = 2 + count;
      for(size_t i = 2; i < icount; i++) {
        data_entry* data = new data_entry(param[i], strlen(param[i]), false);
        values.push_back(data);
      }

      // push
      int ret = 0;
      long successlen = 0;
      if (pcode == 0) {
        client_helper.lpush(area, key, values, expire, version, successlen);
      } else {
        client_helper.rpush(area, key, values, expire, version, successlen);
      }

      if (ret != TAIR_RETURN_SUCCESS) {
         if (pcode == 0) {
            fprintf(stderr, "lpop failed: %s.\n",client_helper.get_error_msg(ret));
         } else {
            fprintf(stderr, "rpop failed: %s.\n",client_helper.get_error_msg(ret));
         }
      } {
         fprintf(stderr, "SUCCESS INSERT %ld\n", successlen);
      }
      if (akey) free(akey);
      for(size_t i = 0; i < values.size(); i++) {
         delete values[i];
      }
      values.clear();
      return ;
   }

   void tair_client::do_cmd_lpush(VSTRING &param) {
      do_cmd_lrpush(0, param);
   }

   void tair_client::do_cmd_rpush(VSTRING &param) {
      do_cmd_lrpush(1, param);
   }

   void tair_client::do_cmd_lrpop(int pcode, VSTRING &param) {
      if (param.size() < 2U || param.size() > 5U ) {
        if (pcode == 0) {
          print_help("lpop");
        } else {
          print_help("rpop");
        }
        return;
      }

      int area = default_area;
      int expire = NOT_CARE_EXPIRE;
      int version = NOT_CARE_VERSION;
      size_t icount = 2;
      if (++icount <= param.size()) {
        char *p=param[icount - 1];
        if (*p == 'n') {
          area |= TAIR_FLAG_NOEXP;
          p ++;
        }
        area |= atoi(p);
      }
      if (++icount <= param.size()) {
        char *p=param[icount - 1];
        version = atoi(p);
      }
      if (++icount <= param.size()) {
        char *p=param[icount - 1];
        expire = atoi(p);
      }

      char *akey = NULL;
      int pkeysize = 0;
      char *pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);
      vector<data_entry*> values;

      int count = atoi(param[1]);

      // pop
      int ret = 0;
      if (pcode == 0) {
        client_helper.lpop(area, key, count, values, expire, version);
      } else {
        client_helper.rpop(area, key, count, values, expire, version);
      }

      if (ret != TAIR_RETURN_SUCCESS) {
         if (pcode == 0) {
            fprintf(stderr, "lpop failed: %s.\n",client_helper.get_error_msg(ret));
         } else {
            fprintf(stderr, "rpop failed: %s.\n",client_helper.get_error_msg(ret));
         }
      } {
         for (size_t i = 0; i < values.size(); i++) {
            data_entry* data = values[i];
            if (data == NULL) {
                continue;
            }
            char *p = util::string_util::conv_show_string(data->get_data(), data->get_size());
            fprintf(stderr, "KEY: %s, COUNT: %d, LEN: %d\n raw data: %s, %s\n",
                 param[0], count, data->get_size(), data->get_data(), p);
            free(p);
            delete data;
         }
         values.clear();
      }
      if (akey) free(akey);
      return ;
   }

   void tair_client::do_cmd_lpop(VSTRING &param) {
      do_cmd_lrpop(0, param);
   }

   void tair_client::do_cmd_rpop(VSTRING &param) {
      do_cmd_lrpop(1, param);
   }

   void tair_client::do_cmd_lindex(VSTRING &param) {
      if (param.size() < 2U || param.size() > 3U ) {
         print_help("lindex");
         return;
      }
      int area = default_area;

      if (param.size() == 3U) {
          char *p=param[param.size() -1];
          if (*p == 'n') {
              area |= TAIR_FLAG_NOEXP;
              p ++;
          }
          area |= atoi(p);
      }

      char *akey = NULL;
      int pkeysize = 0;
      char *pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);
      data_entry data;

      int index = atoi(param[1]);

      // get
      int ret = client_helper.lindex(area, key, index, data);
      if (ret != TAIR_RETURN_SUCCESS) {
         fprintf(stderr, "lindex failed: %s.\n",client_helper.get_error_msg(ret));
      } else {
         char *p = util::string_util::conv_show_string(data.get_data(), data.get_size());
         fprintf(stderr, "KEY: %s, INDEX: %d, LEN: %d\n raw data: %s, %s\n",
                 param[0], index, data.get_size(), data.get_data(), p);
         free(p);
      }
      if (akey) free(akey);
      return ;
   }

   //set
   
   //scard
   void tair_client::do_cmd_scard(VSTRING &param) {
		 if (param.size() < 1U || param.size() > 2U ) {
			print_help("scard");
			return;
		 }
		 int area = default_area;
   
		 if (param.size() >= 2U) {
			 char *p=param[2];
			 if (*p == 'n') {
				 area |= TAIR_FLAG_NOEXP;
				 p ++;
			 }
			 area |= atoi(p);
		 }
   
		 char *akey = NULL;
		 int pkeysize = 0;
		 char* pkey = canonical_key(param[0], &akey, &pkeysize);
		 data_entry key(pkey, pkeysize, false);
   
		 long long retnum=0;
   
		 int ret = client_helper.scard(area, key, retnum);
		 cout << "the number of set is: " << retnum <<std::endl;
		 fprintf(stderr, "scard: %s\n", client_helper.get_error_msg(ret));
		 if (akey) free(akey);
		 return ;
	  }


   //sadd
   void tair_client::do_cmd_sadd(VSTRING &param) {
      if (param.size() < 2U || param.size() > 5U ) {
         print_help("sadd");
         return;
      }
      int area = default_area;

      if (param.size() >= 3U) {
          char *p=param[2];
          if (*p == 'n') {
              area |= TAIR_FLAG_NOEXP;
              p ++;
          }
          area |= atoi(p);
      }

      char *akey = NULL;
      int pkeysize = 0;
      char* pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);

      data_entry value(param[1], false);

      int version = NOT_CARE_VERSION;
      if (param.size() >= 4U) {
          version = atoi(param[3]);
      }

      int expire = NOT_CARE_EXPIRE;
      if (param.size() >= 5U) {
          expire = atoi(param[4]);
      }

      int ret = client_helper.sadd(area, key, value, expire, version);
      fprintf(stderr, "sadd: %s\n", client_helper.get_error_msg(ret));
      if (akey) free(akey);
      return ;
   }

   //int smembers(const int area, const data_entry &key, vector<data_entry*> &values);
   
   /*"SYNOPSIS   : smembers key [area]\n"*/
   
   void tair_client::do_cmd_smembers(VSTRING &param) {
      if (param.size() < 1U || param.size() > 2U ) {
         print_help("smembers");
         return;
      }
      int area = default_area;
      if (param.size() >= 2U) {
         char *p=param[1];
         if (*p == 'n') {
            area |= TAIR_FLAG_NOEXP;
            p ++;
         }
         area |= atoi(p);
      }

      char *akey = NULL;
      int pkeysize = 0;
      char *pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);
      vector<data_entry *> dataset;

      // smembers
      int ret = client_helper.smembers(area, key, dataset);
      if (ret != TAIR_RETURN_SUCCESS) {
         fprintf(stderr, "smembers failed: %s.\n",client_helper.get_error_msg(ret));
      } else if (dataset.size() > 0) {
         fprintf(stderr, "KEY: %s\n", param[0]);
         for(size_t i = 0; i < dataset.size(); i++) {
            data_entry* data = dataset[i];
            if (data == NULL) {
                continue;
            }
            char *p = util::string_util::conv_show_string(data->get_data(), data->get_size());
            fprintf(stderr, "LEN: %d\n raw data: %s, %s\n", data->get_size(), data->get_data(), p);
            if (p) free(p);
            delete data;
         }
         dataset.clear();
      }
      if (akey) free(akey);
      return ;
   }

   void tair_client::do_cmd_srem(VSTRING &param) {
      if (param.size() < 2U || param.size() > 5U ) {
         print_help("srem");
         return;
      }
      int area = default_area;

      if (param.size() >= 3U) {
          char *p=param[2];
          if (*p == 'n') {
              area |= TAIR_FLAG_NOEXP;
              p ++;
          }
          area |= atoi(p);
      }

      char *akey = NULL;
      int pkeysize = 0;
      char* pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);

      data_entry value(param[1], false);

      int version = NOT_CARE_VERSION;
      if (param.size() >= 4U) {
          version = atoi(param[3]);
      }

      int expire = NOT_CARE_EXPIRE;
      if (param.size() >= 5U) {
          expire = atoi(param[4]);
      }

      int ret = client_helper.srem(area, key, value, expire, version);
      fprintf(stderr, "srem: %s\n", client_helper.get_error_msg(ret));
      if (akey) free(akey);
      return ;
   }

   //hset  
   //"SYNOPSIS   : hset key field value [area] [version] [expired]\n"
   
   void tair_client::do_cmd_hset(VSTRING &param) {
      if (param.size() < 3U || param.size() > 6U ) {
         print_help("hset");
         return;
      }
      int area = default_area;

      if (param.size() >= 4U) {
          char *p=param[3];
          if (*p == 'n') {
              area |= TAIR_FLAG_NOEXP;
              p ++;
          }
          area |= atoi(p);
      }

      char *akey = NULL;
      int pkeysize = 0;
      char* pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);

      data_entry field(param[1], false);

      data_entry value(param[2], false);

      int version = NOT_CARE_VERSION;
      if (param.size() >= 5U) {
          version = atoi(param[4]);
      }

      int expire = NOT_CARE_EXPIRE;
      if (param.size() >= 6U) {
          expire = atoi(param[5]);
      }

      int ret = client_helper.hset(area, key, field, value, expire, version);
      fprintf(stderr, "hset: %s\n", client_helper.get_error_msg(ret));
      if (akey) free(akey);
      return ;
   }

   void tair_client::do_cmd_hget(VSTRING &param) {
      if (param.size() < 2U || param.size() > 3U ) {
         print_help("hget");
         return;
      }
      int area = default_area;

      if (param.size() >= 3U) {
          char *p=param[2];
          if (*p == 'n') {
              area |= TAIR_FLAG_NOEXP;
              p ++;
          }
          area |= atoi(p);
      }

      char *akey = NULL;
      int pkeysize = 0;
      char* pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);

      data_entry field(param[1], false);

      data_entry value;

      int ret = client_helper.hget(area, key, field, value);
      if (ret != TAIR_RETURN_SUCCESS) {
         fprintf(stderr, "hget failed: %s.\n",client_helper.get_error_msg(ret));
      } else {
         fprintf(stderr, "KEY: %s\n", param[0]);
         fprintf(stderr, "FIELD: %s\n", param[1]);

         char *p = util::string_util::conv_show_string(value.get_data(), value.get_size());
         fprintf(stderr, "LEN: %d\n raw data: %s, %s\n", value.get_size(), value.get_data(), p);
         free(p);
      }
      if (akey) free(akey);
   }

   void tair_client::do_cmd_hmset(VSTRING &param) {
      if (param.size() < 4U) {
         print_help("hmset");
         return;
      }
      int area = default_area;

      int count = atoi(param[1]);
      if (count <= 0 || count * 2 > (int)(param.size()) - 2) {
         print_help("hmset");
         return;
      }

      char *akey = NULL;
      int pkeysize = 0;
      char* pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);

      map<data_entry*, data_entry*> field_values;
      for(int i = 0; i < count; i++) {
         data_entry* field = new data_entry(param[2 + 2 * i], false);
         data_entry* value = new data_entry(param[2 + 2 * i + 1], false);
         if (field == NULL || value == NULL) {
            print_help("hmset");
            if (akey) free(akey);
            return;
         }
         field_values.insert(make_pair(field, value));
      }

      if (2 * count + 2 < (int)(param.size())) {
         char *p=param[2 * count + 2];
         if (*p == 'n') {
            area |= TAIR_FLAG_NOEXP;
            p ++;
         }
         area |= atoi(p);
      }

      int version = NOT_CARE_VERSION;
      if (2 * count + 2 + 1 < (int)(param.size())) {
         version = atoi(param[2 * count + 2 + 1]);
      }

      int expire = NOT_CARE_EXPIRE;
      if (2 * count + 2 + 2 < (int)(param.size())) {
         expire = atoi(param[2 * count + 2 + 2]);
      }

      map<data_entry*, data_entry*> successed_field_values;
      int ret = client_helper.hmset(area, key, field_values,
              successed_field_values, expire, version);
      fprintf(stderr, "hset: %s\n", client_helper.get_error_msg(ret));
      if (successed_field_values.size() > 0) {
         map<data_entry*, data_entry*>::iterator iter;
         for(iter = successed_field_values.begin();
                 iter != successed_field_values.end(); iter++) {
            data_entry* field = iter->first;
            data_entry* value = iter->second;

            char *fieldp = util::string_util::conv_show_string(field->get_data(), field->get_size());
            char *valuep = util::string_util::conv_show_string(value->get_data(), field->get_size());
            fprintf(stderr, "LEN: %d\n raw field: %s, %s\n", field->get_size(), field->get_data(), fieldp);
            fprintf(stderr, "LEN: %d\n raw value: %s, %s\n", value->get_size(), value->get_data(), valuep);
            free(fieldp);
            free(valuep);
         }
         successed_field_values.clear();
      }

      map<data_entry*, data_entry*>::iterator iter;
      for(iter = field_values.begin(); iter != field_values.end(); iter++) {
         data_entry* field = iter->first;
         data_entry* value = iter->second;
         if (field) delete field;
         if (value) delete value;
      }
      field_values.clear();

      if (akey) free(akey);
   }

   void tair_client::do_cmd_hmget(VSTRING &param) {
/*
      int hmget(const int area, const data_entry &key, const vector<data_entry*> &fields,
      vector<data_entry*> &values);

      "SYNOPSIS   : hmget key count field... [area]\n"
   */
      if (param.size() < 3U) {
         print_help("hmget");
         return;
      }
      int area = default_area;

      int count = atoi(param[1]);
      if (count < 1 || count > (int)(param.size()) - 2) {
         print_help("hmget");
         return;
      }

      vector<data_entry*> fields;
      for(int i = 0; i < count; i++) {
         data_entry* field = new data_entry(param[i + 2], false);
         fields.push_back(field);
      }

      if (count + 2 < (int)(param.size())) {
         char *p = param[count + 2];
         if (*p == 'n') {
            area |= TAIR_FLAG_NOEXP;
            p++;
         }
         area |= atoi(p);
      }

      char *akey = NULL;
      int pkeysize = 0;
      char *pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);

      vector<data_entry*> values;
      int ret = client_helper.hmget(area, key, fields, values);
      if (ret != TAIR_RETURN_SUCCESS) {
         fprintf(stderr, "hmset failed: %s.\n",client_helper.get_error_msg(ret));
      } else if (values.size() > 0) {
         fprintf(stderr, "KEY: %s\n", param[0]);
         for(size_t i = 0; i < fields.size(); i++) {
            data_entry *field = fields[i];
            data_entry *value = values[i];

            char *fieldp = util::string_util::conv_show_string(field->get_data(), field->get_size());
            fprintf(stderr, "LEN: %d\n raw field: %s, %s\n", field->get_size(), field->get_data(), fieldp);
            if (fieldp) free(fieldp);
            if (value == NULL) {
               fprintf(stderr, "(data is null)\n");
            } else {
               char *valuep = util::string_util::conv_show_string(value->get_data(), value->get_size());
               fprintf(stderr, "LEN: %d\n raw data: %s, %s\n", value->get_size(), value->get_data(), valuep);
               if (valuep) free(valuep);
            }
            if (field) delete field;
            if (value) delete value;
         }
         fields.clear();
         values.clear();
      }
      if (akey) free(akey);
      return ;
   }

   void tair_client::do_cmd_hgetall(VSTRING &param) {
      if (param.size() < 1U || param.size() > 2U ) {
         print_help("hgetall");
         return;
      }
      int area = default_area;
      if (param.size() >= 2U) {
         char *p = param[1];
         if (*p == 'n') {
            area |= TAIR_FLAG_NOEXP;
            p ++;
         }
         area |= atoi(p);
      }

      char *akey = NULL;
      int pkeysize = 0;
      char *pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);
      map<data_entry*, data_entry*> datamap;

      // hgetall
      int ret = client_helper.hgetall(area, key, datamap);
      if (ret != TAIR_RETURN_SUCCESS) {
         fprintf(stderr, "hgetall failed: %s.\n",client_helper.get_error_msg(ret));
      } else if (datamap.size() > 0) {
         fprintf(stderr, "KEY: %s\n", param[0]);
         map<data_entry*, data_entry*>::iterator iter;
         for(iter = datamap.begin(); iter != datamap.end(); iter++) {
            data_entry *field = iter->first;
            data_entry *value = iter->second;
            if (field == NULL || value == NULL) {
                continue;
            }
            char *fieldp = util::string_util::conv_show_string(field->get_data(), field->get_size());
            char *valuep = util::string_util::conv_show_string(value->get_data(), value->get_size());
            fprintf(stderr, "LEN: %d\n raw field: %s, %s\n", field->get_size(), field->get_data(), fieldp);
            fprintf(stderr, "LEN: %d\n raw data: %s, %s\n", value->get_size(), value->get_data(), valuep);
            if (fieldp) free(fieldp);
            if (valuep) free(valuep);
            if (field) delete field;
            if (value) delete value;
         }
         datamap.clear();
      }
      if (akey) free(akey);
      return ;
   }

   void tair_client::do_cmd_hdel(VSTRING &param) {
      if (param.size() < 2U || param.size() > 5U ) {
         print_help("hdel");
         return;
      }
      int area = default_area;

      if (param.size() >= 3U) {
          char *p=param[2];
          if (*p == 'n') {
              area |= TAIR_FLAG_NOEXP;
              p ++;
          }
          area |= atoi(p);
      }

      char *akey = NULL;
      int pkeysize = 0;
      char* pkey = canonical_key(param[0], &akey, &pkeysize);
      data_entry key(pkey, pkeysize, false);

      data_entry field(param[1], false);

      int version = NOT_CARE_VERSION;
      if (param.size() >= 4U) {
          version = atoi(param[3]);
      }

      int expire = NOT_CARE_EXPIRE;
      if (param.size() >= 5U) {
          expire = atoi(param[4]);
      }

      int ret = client_helper.hdel(area, key, field, expire, version);
      fprintf(stderr, "hdel: %s\n", client_helper.get_error_msg(ret));
      if (akey) free(akey);
      return ;
   }
} // namespace tair


/*-----------------------------------------------------------------------------
 *  main
 *-----------------------------------------------------------------------------*/

tair::tair_client _globalClient;
void sign_handler(int sig)
{
   switch (sig) {
      case SIGTERM:
      case SIGINT:
         _globalClient.cancel();
   }
}
int main(int argc, char *argv[])
{
   signal(SIGPIPE, SIG_IGN);
   signal(SIGHUP, SIG_IGN);
   signal(SIGINT, sign_handler);
   signal(SIGTERM, sign_handler);

   //TBSYS_LOGGER.setLogLevel("DEBUG");
   TBSYS_LOGGER.setLogLevel("ERROR");
   if (_globalClient.parse_cmd_line(argc, argv) == false) {
      return EXIT_FAILURE;
   }
   if (_globalClient.start()) {
      return EXIT_SUCCESS;
   } else {
      return EXIT_FAILURE;
   }
}
