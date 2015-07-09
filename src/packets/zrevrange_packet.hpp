/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * zrevrange packet
 *
 * Version: $Id: zrevrange_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_ZREVRANGE_PACKET_H
#define TAIR_PACKET_ZREVRANGE_PACKET_H
#include "base_packet.hpp"
#include <stdint.h>

#include "storage_manager.hpp"

namespace tair
{


  class request_zrevrange:public base_packet
  {
  public:
    request_zrevrange (int pcode)
    {
      setPCode (pcode);
      server_flag = 0;
      area = 0;
      start = 0;
      end = 0;
    }

    request_zrevrange ()
    {
      setPCode (TAIR_REQ_ZREVRANGE_PACKET);

      server_flag = 0;
      area = 0;
      start = 0;
      end = 0;
      withscore = 0;
    }

    request_zrevrange (request_zrevrange & packet)
    {
      setPCode (packet.getPCode ());
      server_flag = packet.server_flag;
      area = packet.area;
      start = packet.start;
      end = packet.end;
      withscore = packet.withscore;
      key.clone (packet.key);
    }

    bool encode (tbnet::DataBuffer * output)
    {
      //assert (false);
      // not support cpp api now//commented 6.30
      CREATE_HEADER;

      PUT_INT32_TO_BUFFER (output, start);
      PUT_INT32_TO_BUFFER (output, end);
      PUT_INT32_TO_BUFFER (output, withscore);
      PUT_DATAENTRY_TO_BUFFER (output, key);

      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
        HEADER_VERIFY;

        GETKEY_FROM_INT32 (input, start);
        GETKEY_FROM_INT32 (input, end);
        GETKEY_FROM_INT32 (input, withscore);
        GETKEY_FROM_DATAENTRY (input, key);
        
        return true;
    }

  public:
    uint16_t area;
    data_entry key;
    int32_t start;
    int32_t end;
    int32_t withscore;
  };

  class response_zrevrange:public base_packet
  {
#define RESPONSE_VALUES_MAXSIZE 32767
  public:
    response_zrevrange (int pcode)
    {
      setPCode (pcode);
      config_version = 0;
      values.clear ();
    }

    response_zrevrange ()
    {
      setPCode (TAIR_RESP_ZREVRANGE_PACKET);

      config_version = 0;
      values.clear ();
    }

    ~response_zrevrange () {
      /*for (size_t i = 0; i < values.size(); ++i)
      {
    	data_entry *entry = values[i];
    	if (entry != NULL)
    	  delete (entry);
      }
      values.clear ();*/
      CLEAR_DATA_VECTOR(values,sfree);//added 6.30
    }

    bool encode (tbnet::DataBuffer * output)
    {
      if (values.size() > RESPONSE_VALUES_MAXSIZE)
      {
    	return false;
      }
      output->writeInt32 (config_version);
      output->writeInt16 (version);
      output->writeInt32 (code);
      output->writeInt32 (values.size ());
      data_entry *entry = NULL;
      for (size_t i = 0; i < values.size (); ++i)
      {
    	entry = values[i];
    	output->writeInt32 (entry->get_size ());
    	output->writeBytes (entry->get_data (), entry->get_size ());
      }
      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      // not support in cpp api;//commented 6.30
      GETKEY_FROM_INT32(input, config_version); 
      GETKEY_FROM_INT16(input, version);
      GETKEY_FROM_INT32(input, code);
      GETKEY_FROM_DATAVECTOR(input, values);
	  
      for(size_t i = 0; i < values.size(); i++) {
          values[i]->set_version(version);
		  //printf("values[%d] is %s\n",i,hexStr(values[i]->get_data(),values[i]->get_size()).c_str());
      }
      return true;
    }

    void set_meta (uint32_t config_version, uint32_t code)
    {
      this->config_version = config_version;
      //this->version           = version;
      this->code = code;
    }

	int get_code ()
    {
        return code;
    }

    void set_code (int cde)
    {
        code = cde;
    }

    void set_version(uint16_t version)
    {
        this->version = version;
    }

    void add_data (data_entry * data)
    {
      if (data == NULL)
    	return;
      values.push_back (data);
    }

	void alloc_free(int ifree)
    {
        sfree = ifree;
    }
	
  public:
    uint32_t config_version;
    uint16_t version;
    uint32_t code;
	int sfree;
    vector <data_entry *> values;
    vector<double> scores;
  };

  class response_zrevrangewithscore:public base_packet
  {
#define RESPONSE_VALUES_MAXSIZE 32767
  public:
    response_zrevrangewithscore (int pcode)
    {
      setPCode (pcode);
      config_version = 0;
      values.clear ();
    }

    response_zrevrangewithscore ()
    {
      setPCode (TAIR_RESP_ZREVRANGEWITHSCORE_PACKET);

      config_version = 0;
      values.clear ();
    }

    ~response_zrevrangewithscore () {
      for (size_t i = 0; i < values.size(); ++i)
      {
    	data_entry *entry = values[i];
    	if (entry != NULL)
    	  delete (entry);
      }
      values.clear ();
    }

    bool encode (tbnet::DataBuffer * output)
    {
      if (values.size() > RESPONSE_VALUES_MAXSIZE)
      {
    	return false;
      }
      output->writeInt32 (config_version);
      output->writeInt16 (version);
      output->writeInt32 (code);
      output->writeInt32 (values.size ());
      data_entry *entry = NULL;
      double score = 0;
      char buffer[8];
      for (size_t i = 0; i < values.size (); ++i)
      {
    	entry = values[i];
    	output->writeInt32 (entry->get_size ());
    	output->writeBytes (entry->get_data (), entry->get_size ());

        score = 0;
        if (i < scores.size()) {
            score = scores[i];
        }
	    DOUBLE_TO_BYTES(score, buffer);
	    output->writeBytes(buffer, 8);
      }
      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      // not support in cpp api;
      return false;
    }

    void set_meta (uint32_t config_version, uint32_t code)
    {
      this->config_version = config_version;
      //this->version           = version;
      this->code = code;
    }

    void set_version(uint16_t version)
    {
        this->version = version;
    }

    void add_data (data_entry * data, double score)
    {
      if (data == NULL)
    	return;
      values.push_back (data);
      scores.push_back (score);
    }
  public:
    uint32_t config_version;
    uint16_t version;
    int32_t code;
    vector< data_entry * > values;
    vector<double> scores;
  };
}				// end namespace 
#endif
