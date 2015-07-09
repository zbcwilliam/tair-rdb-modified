/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * hget packet
 *
 * Version: $Id: hget_packet.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_PACKET_HGET_PACKET_H
#define TAIR_PACKET_HGET_PACKET_H
#include "base_packet.hpp"
namespace tair
{
  class request_hget:public base_packet
  {
  public:
    request_hget()
    {
      setPCode(TAIR_REQ_HGET_PACKET);
      server_flag = 0;
      area = 0;
    }

    request_hget(const uint16_t iarea, const data_entry &ikey, const data_entry &ifield)
    {
      setPCode(TAIR_REQ_HGET_PACKET);
      server_flag = 0;
      area = iarea;
      key = ikey;
      field = ifield;
    }

    request_hget(request_hget & packet)
    {
      setPCode(packet.getPCode());
      server_flag = packet.server_flag;
      area = packet.area;

      key.clone(packet.key);
      field.clone(packet.field);
    }

    bool encode(tbnet::DataBuffer * output)
    {
      CREATE_HEADER;

      PUT_DATAENTRY_TO_BUFFER(output, key);
      PUT_DATAENTRY_TO_BUFFER(output, field);

      return true;
    }

    bool decode(tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {

      HEADER_VERIFY;

      GETKEY_FROM_DATAENTRY (input, key);
      GETKEY_FROM_DATAENTRY (input, field);

      return true;
    }

  public:
    uint16_t area;
    data_entry key;
    data_entry field;
  };

  class response_hget:public base_packet
  {
  public:

    response_hget ()
    {
      config_version = 0;
      setPCode (TAIR_RESP_HGET_PACKET);
      code = 0;
      version = 0;
    }

    bool encode (tbnet::DataBuffer * output)
    {
      output->writeInt32 (config_version);
      output->writeInt16 (version);
      output->writeInt32 (code);

      output->writeInt32 (value.get_size());
      output->writeBytes (value.get_data(), value.get_size());

      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      GETKEY_FROM_INT32(input, config_version);
      GETKEY_FROM_INT16(input, version);
      GETKEY_FROM_INT32(input, code);
      GETKEY_FROM_DATAENTRY(input, value);

      value.set_version(version);
      return true;
    }

    void set_meta (uint32_t config_version, uint32_t code)
    {
      this->config_version = config_version;
      this->code = code;
    }

    void set_version (uint16_t version)
    {
      this->version = version;
    }

    void set_code (int cde)
    {
      code = cde;
    }

    int get_code ()
    {
      return code;
    }

  public:
    uint32_t config_version;
    uint16_t version;

    data_entry value;
  private:
    int32_t code;
    response_hget (const response_hget &);
  };
}
#endif
