/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * zrange packet
 *
 * Version: $Id: zrange_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_ZRANGE_PACKET_H
#define TAIR_PACKET_ZRANGE_PACKET_H
#include "base_packet.hpp"
#include <stdint.h>

#include "storage_manager.hpp"

namespace tair
{
  inline std::string hexStr(char *data, int len);
  class request_zrange:public base_packet
  {
  public:
    request_zrange (int pcode)
    {
      setPCode (pcode);
      server_flag = 0;
      area = 0;
      start = 0;
      end = 0;
      withscore = 0;
    }

    request_zrange ()
    {
      setPCode (TAIR_REQ_ZRANGE_PACKET);

      server_flag = 0;
      area = 0;
      start = 0;
      end = 0;
      withscore = 0;
    }

    request_zrange (request_zrange & packet)
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

  class response_zrange:public base_packet
  {
#define RESPONSE_VALUES_MAXSIZE 32767
  public:
    response_zrange ()
    {
      setPCode (TAIR_RESP_ZRANGE_PACKET);

      config_version = 0;
      values.clear ();
    }

    ~response_zrange () {
      /*for (size_t i = 0; i < values.size (); ++i)
      {
	    data_entry *entry = values[i];
	    if (entry != NULL)
	        delete (entry);
      }
      values.clear ();*/
      CLEAR_DATA_VECTOR(values,sfree);//added 6.25
    }

    bool encode (tbnet::DataBuffer * output)
    {
      if (values.size () > RESPONSE_VALUES_MAXSIZE)
      {
	    log_warn("zrange values_size = %d, larger than RESPONSE_VALUES_MAXSZIE", values.size());
		return false;
      }
      output->writeInt32 (config_version);
      output->writeInt16 (version);
      output->writeInt32 (code);
      output->writeInt32 (values.size ());
      data_entry *entry = NULL;
      for (size_t i = 0; i < values.size(); ++i)
      {
	    entry = values[i];
    	output->writeInt32 (entry->get_size ());
    	output->writeBytes (entry->get_data (), entry->get_size ());
      }
      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
	  //printf("databuffer is %s\n",hexStr(input->getData(),input->getDataLen()).c_str());
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

    void set_version(uint16_t version)
    {
        this->version = version;
    }

	int get_code ()
    {
        return code;
    }

    void set_code (int cde)
    {
        code = cde;
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
    vector<data_entry *> values;
    vector<double> scores;
	int sfree;
  };

  class response_zrangewithscore : public base_packet
  {
#define RESPONSE_VALUES_MAXSIZE 32767
  public:
    response_zrangewithscore ()
    {
      setPCode (TAIR_RESP_ZRANGEWITHSCORE_PACKET);

      config_version = 0;
      values.clear ();
      scores.clear ();
    }

    ~response_zrangewithscore () {
      for (size_t i = 0; i < values.size (); ++i)
      {
	    data_entry *entry = values[i];
	    if (entry != NULL)
	        delete (entry);
      }
      values.clear ();
      scores.clear ();
    }

    bool encode (tbnet::DataBuffer * output)
    {
      if (values.size () > RESPONSE_VALUES_MAXSIZE)
      {
	    log_warn("zrange withscore values_size = %d, larger than RESPONSE_VALUES_MAXSZIE", values.size());
		return false;
      }

      output->writeInt32 (config_version);
      output->writeInt16 (version);
      output->writeInt32 (code);
      output->writeInt32 (values.size ());

      char buffer[8];
      data_entry *entry = NULL;
      double score = 0.0;
      for (size_t i = 0; i < values.size(); ++i)
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
      GETKEY_FROM_INT32(input, config_version); 
      GETKEY_FROM_INT16(input, version);
      GETKEY_FROM_INT32(input, code);
      GETKEY_FROM_DATAVECTOR(input, values);

      for(size_t i = 0; i < values.size(); i++) {
          values[i]->set_version(version);
      }
      return true;
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

	int get_code ()
    {
        return code;
    }

    void set_code (int cde)
    {
        code = cde;
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
    vector<data_entry *> values;
    vector<double> scores;
  };
}				// end namespace
#endif
