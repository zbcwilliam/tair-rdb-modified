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
#ifndef TAIR_PACKET_HVALS_PACKET_H
#define TAIR_PACKET_HVALS_PACKET_H
#include "base_packet.hpp"
#include <stdint.h>

#include "storage_manager.hpp"

namespace tair
{


  class request_hvals:public base_packet
  {
  public:
    request_hvals (int pcode)
    {
      setPCode (pcode);
      server_flag = 0;
      area = 0;
    }

    request_hvals ()
    {
      setPCode (TAIR_REQ_HVALS_PACKET);
      server_flag = 0;
      area = 0;
    }

    request_hvals (request_hvals & packet)
    {
      setPCode (packet.getPCode ());
      server_flag = packet.server_flag;
      area = packet.area;
      key.clone (packet.key);
    }

    bool encode (tbnet::DataBuffer * output)
    {
      assert (false);
      // not support cpp api now
      return false;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      if (header->_dataLen < 7) {
	log_warn ("buffer data too few.");
	return false;
      }
      server_flag = input->readInt8 ();	// 1 byte server_flag
      area = input->readInt16 ();	// 2 byte area

      if (input->getDataLen () < 4) {
	log_warn ("buffer data too few.");
	return false;
      }
      int klen = input->readInt32 ();	// 4 byte key length 
      if (input->getDataLen () < klen) {
	log_warn ("buffer data too few.");
	return false;
      }
      key.set_data (NULL, klen);	// other field set 0;
      if (input->readBytes (key.get_data (), klen) == false) {
	log_warn ("buffer data too few.");
	return false;
      }

      return true;
    }

  public:
    uint16_t area;
    data_entry key;
  };

  class response_hvals:public base_packet
  {
#define RESPONSE_VALUES_MAXSIZE 32767
  public:
    response_hvals (int pcode)
    {
      setPCode (pcode);
      config_version = 0;
      values.clear ();
    }

    response_hvals ()
    {
      setPCode (TAIR_RESP_HVALS_PACKET);
      config_version = 0;
      values.clear ();
    }

    ~response_hvals () {
      for (size_t i = 0; i < values.size (); ++i) {
		data_entry *entry = values[i];
		if (entry != NULL)
	  		delete (entry);
      }
      values.clear ();
    }

    bool encode (tbnet::DataBuffer * output)
    {
      if (values.size () > RESPONSE_VALUES_MAXSIZE) {
		return false;
      }
      output->writeInt32 (config_version);
      output->writeInt16 (version);
      output->writeInt32 (code);
      output->writeInt32 (values.size ());
      data_entry *entry = NULL;
      for (size_t i = 0; i < values.size (); ++i) {
		entry = values[i];
		output->writeInt32 (entry->get_size ());
		output->writeBytes (entry->get_data (), entry->get_size ());
      }
      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      // not support in cpp api;
      return false;
    }

    void set_version(uint16_t version)
    {
        this->version = version;
    }

    void set_meta (uint32_t config_version, uint32_t code)
    {
      this->config_version = config_version;
      //this->version           = version;
      this->code = code;
    }

    void add_data (data_entry * data)
    {
      if (data == NULL)
    	return;
      values.push_back (data);
    }
  public:
    uint32_t config_version;
    uint16_t version;
    int32_t code;
    vector < data_entry * >values;
  };

}				// end namespace 
#endif
