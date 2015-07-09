#ifndef TAIR_PACKET_INFO_PACKET_H
#define TAIR_PACKET_INFO_PACKET_H

#include "base_packet.hpp"

namespace tair
{
  class request_info:public base_packet
  {
  public:
    request_info()
    {
      setPCode(TAIR_REQ_INFO_PACKET);
      server_flag = 0;
    }

    request_info(request_info & packet)
    {
      setPCode(packet.getPCode());
      server_flag = packet.server_flag;
    }

    bool encode(tbnet::DataBuffer * output)
    {
      output->writeInt8(server_flag);
      return true;
    }

    bool decode(tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      if ((header)->_dataLen < 1) {
        log_warn( "buffer data too few.");
        return false;
      }
      server_flag = input->readInt8();
      return true;
    }
  };

  class response_info:public base_packet
  {
  public:

    response_info ()
    {
      config_version = 0;
      setPCode (TAIR_RESP_INFO_PACKET);
      code = 0;
    }

    bool encode (tbnet::DataBuffer * output)
    {
      output->writeInt32 (config_version);
      output->writeInt32 (code);

      output->writeInt32 (info.get_size());
      output->writeBytes (info.get_data(), info.get_size());

      return true;
    }

    bool decode (tbnet::DataBuffer * input, tbnet::PacketHeader * header)
    {
      GETKEY_FROM_INT32(input, config_version);
      GETKEY_FROM_INT32(input, code);
      GETKEY_FROM_DATAENTRY(input, info);

      return true;
    }

    void set_meta (uint32_t config_version, uint32_t code)
    {
      this->config_version = config_version;
      this->code = code;
    }

    int get_code ()
    {
      return code;
    }

  public:
    uint32_t config_version;

    data_entry info;
  private:
    int32_t code;
    response_info (const response_info &);
  };
}
#endif
