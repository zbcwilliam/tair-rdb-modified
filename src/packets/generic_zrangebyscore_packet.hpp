/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * zrangebyscore packet
 *
 * Version: $Id: zrangebyscore_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_GENERIC_ZRANGEBYSCORE_PACKET_H
#define TAIR_PACKET_GENERIC_ZRANGEBYSCORE_PACKET_H
#include "base_packet.hpp"
#include <stdint.h>

#include "storage_manager.hpp"

namespace tair
{
  class request_generic_zrangebyscore:public base_packet
  {
  public:
    request_generic_zrangebyscore()
    {
      setPCode(TAIR_REQ_GENERIC_ZRANGEBYSCORE_PACKET);

      server_flag = 0;
      area = 0;
      start = 0.0;
      end = 0.0;
      limit = -1;  //mean not set limit
      withscore = 0;
    }

    request_generic_zrangebyscore(request_generic_zrangebyscore & packet)
    {
      setPCode(packet.getPCode());
      server_flag = packet.server_flag;
      area = packet.area;
      start = packet.start;
      end = packet.end;
      limit = packet.limit;
      withscore = packet.withscore;
      key.clone (packet.key);
    }

    bool encode(tbnet::DataBuffer * output)
    {
      CREATE_HEADER;

      PUT_DOUBLE_TO_BUFFER (output, start);
      PUT_DOUBLE_TO_BUFFER (output, end);
      PUT_DATAENTRY_TO_BUFFER (output, key);
      PUT_INT32_TO_BUFFER (output, reverse);
      PUT_INT32_TO_BUFFER (output, limit);
      PUT_INT32_TO_BUFFER (output, withscore);

      return true;
    }

    bool decode(tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      HEADER_VERIFY;

      GETKEY_FROM_DOUBLE(input, start);
      GETKEY_FROM_DOUBLE(input, end);
      GETKEY_FROM_DATAENTRY(input, key);
      GETKEY_FROM_INT32(input, reverse);
      GETKEY_FROM_INT32(input, limit);
      GETKEY_FROM_INT32(input, withscore);

      return true;
    }

  public:
    uint16_t    area;
    data_entry  key;
    double      start;
    double      end;
    int         reverse;
    int         limit;
    int         withscore;
  };

  class response_generic_zrangebyscore:public base_packet
  {
#define RESPONSE_VALUES_MAXSIZE 32767
  public:
    response_generic_zrangebyscore()
    {
      setPCode(TAIR_RESP_GENERIC_ZRANGEBYSCORE_PACKET);

      config_version = 0;
      values.clear();
    }

    ~response_generic_zrangebyscore()
    {
      for (size_t i = 0; i < values.size (); ++i)
      {
	    data_entry *entry = values[i];
	    if (entry != NULL)
	        delete (entry);
      }
      values.clear ();
    }

    bool encode(tbnet::DataBuffer * output)
    {
      if (values.size() > RESPONSE_VALUES_MAXSIZE)
	    return false;

      double score;
      char buffer[8];
      output->writeInt32(config_version);
      output->writeInt16(version);
      output->writeInt32(code);
      output->writeInt32(scores.size()); //to flag packet if this not zero, mean have scores
      output->writeInt32 (values.size());
      data_entry *entry = NULL;
      for (size_t i = 0; i < values.size(); ++i)
      {
	    entry = values[i];
	    output->writeInt32(entry->get_size());
	    output->writeBytes(entry->get_data(), entry->get_size());

        if (i < scores.size()) {
            score = scores[i];
	        DOUBLE_TO_BYTES(score, buffer);
	        output->writeBytes(buffer, 8);
        }
      }
      return true;
    }

    bool decode(tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      // not support in cpp api;
      return false;
    }

    void set_meta(uint32_t config_version, uint32_t code)
    {
      this->config_version = config_version;
      this->code = code;
    }

    void set_version(uint16_t version)
    {
        this->version = version;
    }

    void add_data(data_entry * data)
    {
      if (data == NULL)
	    return;
      values.push_back(data);
    }
  public:
    uint32_t config_version;
    uint16_t version;
    int32_t code;
    vector<data_entry *> values;
    vector<double> scores;
  };
}				// end namespace
#endif
