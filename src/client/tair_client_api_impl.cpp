/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tair_client_api_impl.cpp 581 2012-02-16 02:41:02Z choutian.xmm@taobao.com $
 *
 * Authors:
 *   MaoQi <maoqi@taobao.com>
 *
 */
#include <string>
#include <map>
#include <vector>
#include <ext/hash_map>
#include <signal.h>
#include <unistd.h>
#include <getopt.h>
#include <dirent.h>

#include <tbsys.h>
#include <tbnet.h>

#include "define.hpp"
#include "tair_client_api.hpp"
#include "wait_object.hpp"
#include "tair_client_api_impl.hpp"
#include "put_packet.hpp"
#include "remove_area_packet.hpp"
#include "lazy_remove_area_packet.hpp"
#include "remove_packet.hpp"
#include "inc_dec_packet.hpp"
#include "dump_packet.hpp"
#include "get_packet.hpp"
#include "get_group_packet.hpp"
#include "query_info_packet.hpp"
#include "get_migrate_machine.hpp"
#include "data_server_ctrl_packet.hpp"


namespace tair {


  using namespace std;
  using namespace __gnu_cxx;
  using namespace tair::common;


  /*-----------------------------------------------------------------------------
   *  tair_client_impl
   *-----------------------------------------------------------------------------*/

  tair_client_impl::tair_client_impl():inited(false),is_stop(false),packet_factory(0),streamer(0),\
                                       transport(0),connmgr(0),timeout(2000),\
                                       config_version(0),new_config_version(0),\
                                       send_fail_count(0),this_wait_object_manager(0),bucket_count(0),\
                                       copy_count(0)
  {
  }

  tair_client_impl::~tair_client_impl()
  {
    close();
  }

  bool tair_client_impl::startup(const char *master_addr,const char *slave_addr,const char *group_name)
  {
    if(master_addr == NULL || group_name == NULL){
      return false;
    }

    if(inited){
      return true;
    }
    is_stop = false;

    if( !initialize() ){
      return false;
    }

    uint64_t master_cfgsvr = tbsys::CNetUtil::strToAddr(master_addr,TAIR_CONFIG_SERVER_DEFAULT_PORT);
    uint64_t slave_cfgsvr = slave_addr  == NULL ? 0 : tbsys::CNetUtil::strToAddr(slave_addr,TAIR_CONFIG_SERVER_DEFAULT_PORT);
    return startup(master_cfgsvr,slave_cfgsvr,group_name);
  }

  bool tair_client_impl::startup(uint64_t master_cfgsvr, uint64_t slave_cfgsvr, const char *group_name)
  {

    if(master_cfgsvr == 0 || group_name == NULL){
      return false;
    }

    TBSYS_LOG(DEBUG,"master_cfg:%s,group:%s \n", tbsys::CNetUtil::addrToString(master_cfgsvr).c_str(),group_name);

    if (master_cfgsvr) {
      config_server_list.push_back(master_cfgsvr);
    }
    if (slave_cfgsvr) {
      config_server_list.push_back(slave_cfgsvr);
    }

    this->group_name = group_name;

    start_tbnet();

    if (!retrieve_server_addr()) {
      TBSYS_LOG(ERROR,"retrieve_server_addr falied.\n");
      close();
      return false;
    }
    inited = true;
    return true;
  }

  bool tair_client_impl::initialize()
  {

    this_wait_object_manager = new wait_object_manager();
    if(this_wait_object_manager == NULL) {
      return false;
    }

    packet_factory = new tair_packet_factory();
    if( packet_factory == NULL) {
      goto FAIL_1;
    }

    streamer = new tbnet::DefaultPacketStreamer();
    if(streamer == NULL) {
      goto FAIL_2;
    }

    streamer->setPacketFactory(packet_factory);
    transport = new tbnet::Transport();
    if(transport == NULL) {
      goto FAIL_3;
    }

    connmgr = new tbnet::ConnectionManager(transport, streamer, this);
    if(connmgr == NULL) {
      goto FAIL_4;
    }

    return true;

FAIL_4:
    delete transport;
    transport = 0;
FAIL_3:
    delete streamer;
    streamer = 0;
FAIL_2:
    delete packet_factory;
    packet_factory = 0;
FAIL_1:
    delete this_wait_object_manager;
    this_wait_object_manager = 0;
    return false;
  }

  // startup, server
  bool tair_client_impl::startup(uint64_t dataserver)
  {
    ds_server_list.push_back(dataserver);
    bucket_count = 1;
    copy_count = 1;
    if(!initialize()){
      return false;
    }
    start_tbnet();
    inited = true;
    return tbnet::ConnectionManager::isAlive(dataserver);
  }

  void tair_client_impl::close()
  {
    if (is_stop || !inited)
      return;
    is_stop = true;
    stop_tbnet();
    wait_tbnet();
    thread.join();
    if (connmgr)
      delete connmgr;
    if (transport)
      delete transport;
    if (streamer)
      delete streamer;
    if (packet_factory)
      delete packet_factory;
    if (this_wait_object_manager)
      delete this_wait_object_manager;
    reset();
  }

  int tair_client_impl::send_packet(const uint32_t hash, base_packet* spacket)
  {
    base_packet* rpacket = NULL;
    int ret = -1;
    vector<uint64_t> server_list;
    if (!get_server_id(hash, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return ret;
    }

    TBSYS_LOG(DEBUG,"send packet to server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    wait_object *cwo = this_wait_object_manager->create_wait_object();

    if((ret = send_request(server_list[0], spacket, cwo->get_id())) < 0) {
      this_wait_object_manager->destroy_wait_object(cwo);
      return ret;
    }

    if((ret = get_response(cwo,1,rpacket)) < 0) {
      this_wait_object_manager->destroy_wait_object(cwo);
      return ret;
    }

    ret = rpacket->get_code();
    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
  }

  int tair_client_impl::send_packet(const data_entry &key, base_packet* spacket)
  {
    uint32_t hash = util::string_util::mur_mur_hash(key.get_data(), key.get_size());
    return send_packet(hash, spacket);
  }

  int tair_client_impl::put(int area,
      const data_entry &key,
      const data_entry &data,
      int expired, int version )
  {

    if( !(key_entry_check(key)) || (!data_entry_check(data))){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if(key.get_size() + data.get_size() > (TAIR_MAX_DATA_SIZE + TAIR_MAX_KEY_SIZE)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if(area < 0 || area >= TAIR_MAX_AREA_COUNT || version < 0 || expired < 0){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }

    TBSYS_LOG(DEBUG,"put to server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());


    wait_object *cwo = this_wait_object_manager->create_wait_object();

    request_put *packet = new request_put();
    packet->area = area;
    packet->key = key;
    packet->data = data;
    packet->expired = expired;
    packet->version = version;
    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = 0;
    response_return *resp = 0;


    if( (ret = send_request(server_list[0],packet,cwo->get_id())) < 0){

      delete packet;
      goto FAIL;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0){
      goto FAIL;
    }

    if(tpacket == 0 || tpacket->getPCode() != TAIR_RESP_RETURN_PACKET){
      goto FAIL;
    }

    resp = (response_return*)tpacket;
    new_config_version = resp->config_version;
    ret = resp->get_code();
    if (ret != TAIR_RETURN_SUCCESS){

      if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK || ret == TAIR_RETURN_WRITE_NOT_ON_MASTER) {
        //update server table immediately
        send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
      }

      goto FAIL;
    }

    this_wait_object_manager->destroy_wait_object(cwo);

    return 0;

FAIL:

    this_wait_object_manager->destroy_wait_object(cwo);
    TBSYS_LOG(INFO, "put failure: %s ",get_error_msg(ret));

    return ret;
  }



  int tair_client_impl::get(int area, const data_entry &key, data_entry* &data )
  {

    if( !key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"get from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());


    wait_object *cwo = 0;

    base_packet *tpacket = 0;
    response_get *resp  = 0;
    int ret = TAIR_RETURN_SEND_FAILED;
    int send_success = 0;

    for(vector<uint64_t>::iterator it=server_list.begin(); it != server_list.end(); ++it){

      request_get *packet = new request_get();
      packet->area = area;

      packet->add_key(key.get_data(), key.get_size());
      cwo = this_wait_object_manager->create_wait_object();

      if( send_request(*it,packet,cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);

        //need delete packet
        delete packet;
        continue;
      }

      if( (ret = get_response(cwo,1,tpacket)) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        continue;
      }else{
        ++send_success;
        break;
      }
    }

    if( send_success < 1 ){
      TBSYS_LOG(ERROR,"all requests are failed");
      return ret;
    }
    if (tpacket == 0 || tpacket->getPCode() != TAIR_RESP_GET_PACKET){
      goto FAIL;
    }
    resp = (response_get*)tpacket;
    ret = resp->get_code();
    if(ret != TAIR_RETURN_SUCCESS){
      goto FAIL;
    }
    assert(resp->data != NULL);
    if (resp->data) {
      data = resp->data;
      resp->data = 0;
      ret = TAIR_RETURN_SUCCESS;
    }
    TBSYS_LOG(DEBUG,"end get:ret:%d",ret);

    new_config_version = resp->config_version;
    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
FAIL:
    if(tpacket && tpacket->getPCode() == TAIR_RESP_RETURN_PACKET){
      response_return *r = (response_return *)tpacket;
      ret = r->get_code();
    }
    if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK){
      new_config_version = resp->config_version;
      send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
    }

    this_wait_object_manager->destroy_wait_object(cwo);


    TBSYS_LOG(INFO, "get failure: %s:%s",
        tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
        get_error_msg(ret));

    return ret;
  }

  int tair_client_impl::lrpush(int pcode, int area, data_entry &key, vector<data_entry*> &values,
          int expire, int version, long &successlen)
  {
    if(!key_entry_check(key)) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    int len = 0;
    for(size_t i = 0; i < values.size(); i++) {
        data_entry* data = values[i];
        if(!data_entry_check(*data)) {
            return TAIR_RETURN_ITEMSIZE_ERROR;
        }
        len += data->get_size();
    }

    if(key.get_size() + len > (TAIR_MAX_DATA_SIZE + TAIR_MAX_KEY_SIZE)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if(area < 0 || area >= TAIR_MAX_AREA_COUNT || version < 0 ){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }

    TBSYS_LOG(DEBUG,"lpush to server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());


    wait_object *cwo = this_wait_object_manager->create_wait_object();

    request_lrpush *packet = new request_lrpush(pcode);
    packet->area = area;
    packet->key = key;
    for(size_t i = 0; i < values.size(); i++) {
        packet->addValue(values[i]);
    }
    packet->expire = expire;
    packet->version = version;
    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = 0;
    response_lrpush *resp = 0;


    if( (ret = send_request(server_list[0],packet,cwo->get_id())) < 0){
      delete packet;
      goto FAIL;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0){
      goto FAIL;
    }

    if(tpacket == 0 || tpacket->getPCode() != TAIR_RESP_LPUSH_PACKET) {
      goto FAIL;
    }

    resp = (response_lrpush*)tpacket;
    new_config_version = resp->config_version;
    ret = resp->get_code();
    successlen = resp->pushed_num;
    if (ret != TAIR_RETURN_SUCCESS){

      if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK || ret == TAIR_RETURN_WRITE_NOT_ON_MASTER) {
        //update server table immediately
        send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
      }

      goto FAIL;
    }

    this_wait_object_manager->destroy_wait_object(cwo);

    return 0;

FAIL:

    this_wait_object_manager->destroy_wait_object(cwo);
    TBSYS_LOG(INFO, "lrpush failure: %s ",get_error_msg(ret));

    return ret;
  }

  int tair_client_impl::lrpop(int pcode, int area, data_entry &key, int count, vector<data_entry*> &values,
          int expire, int version)
  {
    if( !key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"lrpop from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    values.clear();
    wait_object *cwo = 0;

    base_packet *tpacket = 0;
    response_lrpop *resp  = 0;
    int ret = TAIR_RETURN_SEND_FAILED;

    request_lrpop *packet = new request_lrpop(pcode);
    packet->area = area;
    packet->count = count;
    packet->key = key;
    cwo = this_wait_object_manager->create_wait_object();

    if( send_request(server_list[0],packet,cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        //need delete packet
        delete packet;
        return ret;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");
        return ret;
    }

    if (tpacket == 0 || (tpacket->getPCode() != TAIR_RESP_RPOP_PACKET &&
            tpacket->getPCode() != TAIR_RESP_LPOP_PACKET)){
      goto FAIL;
    }
    resp = (response_lrpop*)tpacket;
    ret = resp->get_code();
    if(ret != TAIR_RETURN_SUCCESS){
      goto FAIL;
    }

    for(size_t i = 0; i < resp->values.size(); i++) {
      data_entry *data = new data_entry(*(resp->values[i]));
      values.push_back(data);
    }

    TBSYS_LOG(DEBUG,"end lrpop:ret:%d",ret);

    new_config_version = resp->config_version;
    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
FAIL:
    if(tpacket && tpacket->getPCode() == TAIR_RESP_RETURN_PACKET){
      response_return *r = (response_return *)tpacket;
      ret = r->get_code();
    }
    if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK){
      new_config_version = resp->config_version;
      send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
    }

    this_wait_object_manager->destroy_wait_object(cwo);


    TBSYS_LOG(INFO, "lrpop failure: %s:%s",
        tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
        get_error_msg(ret));

    return ret;
  }

  int tair_client_impl::lpush(int area, data_entry &key, vector<data_entry*> &values,
          int expire, int version, long &successlen)
  {
      return lrpush(TAIR_REQ_LPUSH_PACKET, area, key, values, expire, version, successlen);
  }

  int tair_client_impl::rpush(int area, data_entry &key, vector<data_entry*> &values,
          int expire, int version, long &successlen)
  {
      return lrpush(TAIR_REQ_RPUSH_PACKET, area, key, values, expire, version, successlen);
  }

  int tair_client_impl::lpop(int area, data_entry &key, int count, vector<data_entry*> &values,
          int expire, int version)
  {
      return lrpop(TAIR_REQ_LPOP_PACKET, area, key, count, values, expire, version);
  }

  int tair_client_impl::rpop(int area, data_entry &key, int count, vector<data_entry*> &values,
          int expire, int version)
  {
      return lrpop(TAIR_REQ_RPOP_PACKET, area, key, count, values, expire, version);
  }

  int tair_client_impl::lindex(int area, data_entry &key, int index, data_entry &value)
  {
    if(!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"lindex from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());


    wait_object *cwo = 0;

    base_packet *tpacket = 0;
    response_lindex *resp  = 0;
    int ret = TAIR_RETURN_SEND_FAILED;

    request_lindex *packet = new request_lindex();
    packet->area = area;
    packet->index = index;
    packet->key = key;
    cwo = this_wait_object_manager->create_wait_object();

    if( send_request(server_list[0],packet,cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        //need delete packet
        delete packet;
        return ret;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");
        return ret;
    }

    if (tpacket == 0 || tpacket->getPCode() != TAIR_RESP_LINDEX_PACKET){
      goto FAIL;
    }
    resp = (response_lindex*)tpacket;
    ret = resp->get_code();
    if(ret != TAIR_RETURN_SUCCESS){
      goto FAIL;
    }

    value = resp->value;
    TBSYS_LOG(DEBUG,"end lindex:ret:%d",ret);

    new_config_version = resp->config_version;
    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
FAIL:
    if(tpacket && tpacket->getPCode() == TAIR_RESP_RETURN_PACKET){
      response_return *r = (response_return *)tpacket;
      ret = r->get_code();
    }
    if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK){
      new_config_version = resp->config_version;
      send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
    }

    this_wait_object_manager->destroy_wait_object(cwo);


    TBSYS_LOG(INFO, "lindex failure: %s:%s",
        tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
        get_error_msg(ret));

    return ret;
  }

  //scard added by zbcwilliam at 6.30; imitate zcard
  int tair_client_impl::scard(const int area, const data_entry &key, long long &retnum) {
    //check
    if(!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //get server list
    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"scard from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    //init packet
    int ret = TAIR_RETURN_SEND_FAILED;
    wait_object *cwo = 0;
    base_packet *tpacket = 0;
    request_scard*request = new request_scard();
	request->area = area;
	request->key = key;
	TBSYS_LOG(DEBUG,"scard key=%s",key.get_data());  //get_data called
	

    cwo = this_wait_object_manager->create_wait_object();

    //send request
    if( send_request(server_list[0],request,cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        delete request;
        return ret;
    }

    //get response
    if( (ret = get_response(cwo,1,tpacket)) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");
        return ret;
    }

    assert(tpacket != NULL);

    if (tpacket->getPCode() == TAIR_RESP_SCARD_PACKET) {
        response_scard*response = (response_scard*)tpacket;
        ret = response->get_code();
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end scard:ret:%d",ret);

        new_config_version = response->config_version;

		//cout << "response->retnum is: " << response->retnum <<std::endl;
		retnum = response->retnum;//added 6.29
		
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }


  //sadd
  int tair_client_impl::sadd(const int area, const data_entry &key, const data_entry &value,
          const int expire, const int version) {
    //check
    if(!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if(!data_entry_check(value)) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //get server list
    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"sadd from server:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    //init packet
    int ret = TAIR_RETURN_SEND_FAILED;
    wait_object *cwo = 0;
    base_packet *tpacket = 0;
    request_sadd *packet = new request_sadd(area, version, expire, key, value);

    cwo = this_wait_object_manager->create_wait_object();

    //send request
    if( send_request(server_list[0], packet,cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        delete packet;
        return ret;
    }

    //get response
    if( (ret = get_response(cwo, 1, tpacket)) < 0 ) {
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");
        return ret;
    }

    assert(tpacket != NULL);

    if (tpacket->getPCode() == TAIR_RESP_SADD_PACKET) {
        response_sadd* response = (response_sadd*)tpacket;
        ret = response->get_code();
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end sadd:ret:%d",ret);

        new_config_version = response->config_version;
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }

  //smembers
  int tair_client_impl::smembers(const int area, const data_entry &key, vector<data_entry*> &values) {
    //check
    if(!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //get server list
    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"smembers from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    int ret = TAIR_RETURN_SEND_FAILED;
    wait_object *cwo = NULL;
    base_packet *tpacket = NULL;
    request_smembers *request = new request_smembers(area, key);
    cwo = this_wait_object_manager->create_wait_object();

    //send request
    if( send_request(server_list[0], request, cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        delete request;
        return ret;
    }

    //get response
    if( (ret = get_response(cwo, 1, tpacket)) < 0 ) {
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");
        return ret;
    }

    assert(tpacket != NULL);

    if (tpacket->getPCode() == TAIR_RESP_SMEMBERS_PACKET) {
        response_smembers *response = (response_smembers *)tpacket;
        ret = response->get_code();
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end smembers:ret:%d",ret);

        response->alloc_free(false);
        values = response->values;
        new_config_version = response->config_version;
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }

  //srem
  int tair_client_impl::srem(const int area, const data_entry &key, const data_entry &value,
          const int expire, const int version) {
    //check
    if(!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //get server list
    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"srem from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    //init packet
    int ret = TAIR_RETURN_SEND_FAILED;
    wait_object *cwo = 0;
    base_packet *tpacket = 0;
    request_srem *packet = new request_srem(area, version, expire, key, value);

    cwo = this_wait_object_manager->create_wait_object();

    //send request
    if( send_request(server_list[0],packet,cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        delete packet;
        return ret;
    }

    //get response
    if( (ret = get_response(cwo,1,tpacket)) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");
        return ret;
    }

    assert(tpacket != NULL);

    if (tpacket->getPCode() == TAIR_RESP_SREM_PACKET) {
        response_srem *response = (response_srem *)tpacket;
        ret = response->get_code();
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end srem:ret:%d",ret);

        new_config_version = response->config_version;
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }

  //hset
  int tair_client_impl::hset(const int area, const data_entry &key, const data_entry &field,
          const data_entry &value, const int expire, const int version) {
    //check
    if (!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if (!data_entry_check(field)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if (!data_entry_check(value)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if ( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //get server list
    vector<uint64_t> server_list;
    if (!get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"hset from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    //init packet
    int ret = TAIR_RETURN_SEND_FAILED;
    wait_object *cwo = 0;
    base_packet *tpacket = 0;
    request_hset *packet = new request_hset(area, version, expire, key, field, value);

    cwo = this_wait_object_manager->create_wait_object();

    //send request
    if(send_request(server_list[0],packet,cwo->get_id()) < 0){
        this_wait_object_manager->destroy_wait_object(cwo);
        delete packet;
        return ret;
    }

    //get response
    if((ret = get_response(cwo,1,tpacket)) < 0){
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");
        return ret;
    }

    assert(tpacket != NULL);

    if (tpacket->getPCode() == TAIR_RESP_HSET_PACKET) {
        response_hset *response = (response_hset *)tpacket;
        ret = response->get_code();
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end hset:ret:%d",ret);

        new_config_version = response->config_version;
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }

  //hget
  int tair_client_impl::hget(const int area, const data_entry &key, const data_entry &field,
          data_entry &value) {
    //check
    if (!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if (!data_entry_check(field)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if ( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //get server list
    vector<uint64_t> server_list;
    if (!get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"hget from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    //init packet
    int ret = TAIR_RETURN_SEND_FAILED;
    wait_object *cwo = 0;
    base_packet *tpacket = 0;
    request_hget *packet = new request_hget(area, key, field);

    cwo = this_wait_object_manager->create_wait_object();

    //send request
    if(send_request(server_list[0],packet,cwo->get_id()) < 0){
        this_wait_object_manager->destroy_wait_object(cwo);
        delete packet;
        return ret;
    }

    //get response
    if((ret = get_response(cwo,1,tpacket)) < 0){
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");
        return ret;
    }

    assert(tpacket != NULL);

    if (tpacket->getPCode() == TAIR_RESP_HGET_PACKET) {
        response_hget *response = (response_hget *)tpacket;
        ret = response->get_code();
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end hset:ret:%d",ret);

        value = response->value;
        new_config_version = response->config_version;
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }

  //hmset
  int tair_client_impl::hmset(const int area, const data_entry &key, const map<data_entry*, data_entry*> &field_values,
          map<data_entry*, data_entry*> &field_values_success,
          const int expire, const int version) {
    field_values_success.clear();
    //check
    if(!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if(!map_data_check(field_values)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //get server list
    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"hmset from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    //init packet
    int ret = TAIR_RETURN_SEND_FAILED;
    wait_object *cwo = 0;
    base_packet *tpacket = 0;
    vector<data_entry*> field_values_v;
    map_to_vector(field_values, field_values_v);
    request_hmset *packet = new request_hmset(area, version,
            expire, key, field_values_v, 0);

    cwo = this_wait_object_manager->create_wait_object();

    //send request
    if( send_request(server_list[0],packet,cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        delete packet;
        return ret;
    }

    //get response
    if( (ret = get_response(cwo,1,tpacket)) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");
        return ret;
    }

    assert(tpacket != NULL);

    if (tpacket->getPCode() == TAIR_RESP_HMSET_PACKET) {
        response_hmset *response = (response_hmset *)tpacket;
        ret = response->get_code();
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end hmset:ret:%d",ret);

        if(response->retvalue != (int)(field_values.size())) {
            int index;
            map<data_entry*, data_entry*>::const_iterator iter;
            for(index = 0, iter = field_values.begin();
                    (index < response->retvalue) && (iter != field_values.end());
                    index++, iter++) {
                field_values_success.insert(make_pair(iter->first, iter->second));
            }
        }

        new_config_version = response->config_version;
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }

  //hgetall
  int tair_client_impl::hgetall(const int area, const data_entry &key,
          map<data_entry*, data_entry*> &field_values) {
    field_values.clear();
    //check
    if(!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //get server list
    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"hgetall from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    //init packet
    wait_object *cwo = 0;
    base_packet *tpacket = 0;
    int ret = TAIR_RETURN_SEND_FAILED;
    request_hgetall *packet = new request_hgetall(area, key);

    cwo = this_wait_object_manager->create_wait_object();

    //send request
    if( send_request(server_list[0],packet,cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        delete packet;
        return ret;
    }

    //get response
    if( (ret = get_response(cwo,1,tpacket)) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");
        return ret;
    }

    assert(tpacket != NULL);

    if (tpacket->getPCode() == TAIR_RESP_HGETALL_PACKET) {
        response_hgetall *response = (response_hgetall *)tpacket;
        ret = response->get_code();
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end hmset:ret:%d",ret);

        //return values
        response->alloc_free(false);
        if (!vector_to_map(response->values, field_values)) {
            field_values.clear();
            response->alloc_free(true);
            ret = TAIR_RETURN_SERIALIZE_ERROR;
        }

        new_config_version = response->config_version;
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }

  //hmget
  int tair_client_impl::hmget(const int area, const data_entry &key, const vector<data_entry*> &fields,
          vector<data_entry*> &values) {
    values.clear();
    //check
    if(!key_entry_check(key)) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if(!vector_data_check(fields)) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //get server list
    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"hmget from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    //init packet
    int ret = TAIR_RETURN_SEND_FAILED;
    wait_object *cwo = 0;
    base_packet *tpacket = 0;
    request_hmget *packet = new request_hmget(area, key, fields, 0);

    cwo = this_wait_object_manager->create_wait_object();

    //send request
    if(send_request(server_list[0],packet,cwo->get_id()) < 0) {
      this_wait_object_manager->destroy_wait_object(cwo);
      delete packet;
      return ret;
    }

    //get response
    if((ret = get_response(cwo,1,tpacket)) < 0) {
      this_wait_object_manager->destroy_wait_object(cwo);
      TBSYS_LOG(ERROR, "all requests are failed");
      return ret;
    }

    assert(tpacket != NULL);

    if (tpacket->getPCode() == TAIR_RESP_HMGET_PACKET) {
        response_hmget *response = (response_hmget *)tpacket;
        ret = response->get_code();
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end hmset:ret:%d",ret);

        //return values
        response->alloc_free(false);
        values = response->values;
        new_config_version = response->config_version;
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }

  //hdel
  int tair_client_impl::hdel(const int area, const data_entry &key, const data_entry &field,
          const int expire, const int version) {
    if(!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if(!data_entry_check(field)) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"hdel from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());


    wait_object *cwo = 0;

    base_packet *tpacket = 0;
    int ret = TAIR_RETURN_SEND_FAILED;

    request_hdel *packet = new request_hdel(area, version, expire, key, field);
    cwo = this_wait_object_manager->create_wait_object();

    if( send_request(server_list[0],packet,cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        //need delete packet
        delete packet;
        return ret;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");
        return ret;
    }

    assert(tpacket != NULL);

    if (tpacket->getPCode() == TAIR_RESP_HDEL_PACKET) {
        response_hdel *response = (response_hdel *)tpacket;
        ret = response->get_code();
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end hdel:ret:%d",ret);

        new_config_version = response->config_version;
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }


  int tair_client_impl::init_request_map(int area, vector<data_entry *>& keys,
          request_get_map &request_gets, int server_select)
  {
    if ( area < 0 || area >= TAIR_MAX_AREA_COUNT || server_select < 0 )
    {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    int ret = TAIR_RETURN_SUCCESS;

    request_get_map::iterator rq_iter;

    vector<data_entry *>::iterator key_iter;
    for (key_iter = keys.begin(); key_iter != keys.end(); ++key_iter)
    {
      if ( !key_entry_check(**key_iter))
      {
        ret = TAIR_RETURN_ITEMSIZE_ERROR;
        break;
      }

      vector<uint64_t> server_list;
      if ( !get_server_id( **key_iter, server_list))
      {
        TBSYS_LOG(DEBUG, "can not find serverId, return false");
        ret = -1;
        break;
      }
      uint32_t ser_idx = server_select;
      if (server_list.size() <= ser_idx)
      {
        TBSYS_LOG(DEBUG, "select:%d not in server_list,size %d",server_select, server_list.size());
        ser_idx = 0;
      }
      request_get *packet = NULL;
      rq_iter = request_gets.find(server_list[ser_idx]);
      if ( rq_iter != request_gets.end() )
      {
        packet = rq_iter->second;
      } else {
        packet = new request_get();
        request_gets[server_list[ser_idx]] = packet;
      }

      packet->area = area;
      packet->add_key((*key_iter)->get_data(), (*key_iter)->get_size());
      TBSYS_LOG(DEBUG,"get from server:%s",tbsys::CNetUtil::addrToString(server_list[ser_idx]).c_str());
    }

    if (ret < 0)
    {
      rq_iter = request_gets.begin();
      while (rq_iter != request_gets.end())
      {
        request_get* req = rq_iter->second;
        ++rq_iter;
        delete req;
      }
      request_gets.clear();
    }
    return ret;
  }

  int tair_client_impl::mget_impl(int area, vector<data_entry*> &keys, tair_keyvalue_map &data, int server_select)
  {
    request_get_map request_gets;
    int ret = TAIR_RETURN_SUCCESS;
    if ((ret = init_request_map(area, keys, request_gets, server_select)) < 0)
    {
      return ret;
    }

    wait_object* cwo = this_wait_object_manager->create_wait_object();
    request_get_map::iterator rq_iter = request_gets.begin();
    while (rq_iter != request_gets.end())
    {
      if (send_request(rq_iter->first, rq_iter->second, cwo->get_id()) < 0)
      {
        delete rq_iter->second;
        request_gets.erase(rq_iter++);
      }
      else
      {
        ++rq_iter;
      }
    }

    vector<response_get*> resps;
    ret = TAIR_RETURN_SEND_FAILED;

    vector<base_packet*> tpk;
    if ((ret = get_response(cwo, request_gets.size(), tpk)) < 1)
    {
      this_wait_object_manager->destroy_wait_object(cwo);
      TBSYS_LOG(ERROR,"all requests are failed");
      return ret;
    }

    vector<base_packet*>::iterator bp_iter = tpk.begin();
    for (; bp_iter != tpk.end(); ++bp_iter)
    {
      response_get * tpacket = dynamic_cast<response_get *> (*bp_iter);
      if (tpacket == NULL || tpacket->getPCode() != TAIR_RESP_GET_PACKET)
      {
        if (tpacket != NULL && tpacket->getPCode() == TAIR_RESP_RETURN_PACKET)
        {
          response_return *r = (response_return *)tpacket;
          ret = r->get_code();
        }
        if (tpacket != NULL && ret == TAIR_RETURN_SERVER_CAN_NOT_WORK)
        {
          new_config_version = tpacket->config_version;
          send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        continue;
      }
      resps.push_back(tpacket);
      //TBSYS_LOG(DEBUG, "response from server key_count: %d", tpacket->key_count);
    }

    vector<response_get *>::iterator rp_iter = resps.begin();
    for (; rp_iter != resps.end(); ++ rp_iter)
    {
      ret = (*rp_iter)->get_code();
      if ((ret != TAIR_RETURN_SUCCESS) && (ret != TAIR_RETURN_PARTIAL_SUCCESS))
      {
        TBSYS_LOG(WARN, "get response code: %d error", ret);
        continue;
      }

      if ((*rp_iter)->key_count == 1)
      {
        data_entry* key   = new data_entry(*((*rp_iter)->key));
        data_entry* value = new data_entry(*((*rp_iter)->data));
        data.insert(tair_keyvalue_map::value_type(key, value));
      }
      else
      {
        tair_keyvalue_map* kv_map = (*rp_iter)->key_data_map;
        assert(kv_map != NULL);
        tair_keyvalue_map::iterator it = kv_map->begin();
        while (it != kv_map->end())
        {
          data_entry * key   = new data_entry(*(it->first));
          data_entry * value = new data_entry(*(it->second));
          data.insert(tair_keyvalue_map::value_type(key, value));
          ++it;
        }
      }
      new_config_version = (*rp_iter)->config_version;
    }

    ret = TAIR_RETURN_SUCCESS;
    if (keys.size() > data.size())
    {
      ret = TAIR_RETURN_PARTIAL_SUCCESS;
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
  }

  int tair_client_impl::mget(int area, vector<data_entry*> &keys, tair_keyvalue_map &data)
  {
    int ret = mget_impl(area, keys, data, 0);

    //if (ret == TAIR_RETURN_PARTIAL_SUCCESS)
    //{
    //  vector<data_entry *> diff_keys;

    //  tair_keyvalue_map::iterator iter;
    //  vector<data_entry *>::iterator key_iter;
    //  for (key_iter = keys.begin(); key_iter != keys.end(); ++key_iter)
    //  {
    //    iter = data.find(*key_iter);//why can not find??
    //    if ( iter == data.end())
    //    {
    //      diff_keys.push_back(*key_iter);
    //    }
    //  }

    //  tair_keyvalue_map diff_data;
    //  ret = mget_impl(area, diff_keys, diff_data, 1);
    //  if (ret == TAIR_RETURN_SUCCESS || ret == TAIR_RETURN_PARTIAL_SUCCESS)
    //  {
    //    iter = diff_data.begin();
    //    while (iter != diff_data.end())
    //    {
    //      data.insert(tair_keyvalue_map::value_type(iter->first, iter->second));
    //      ++iter;
    //    }
    //    diff_data.clear();
    //  }
    //  if (keys.size() > data.size())
    //  {
    //    ret = TAIR_RETURN_PARTIAL_SUCCESS;
    //  }
    //}

    return ret;
  }


  int tair_client_impl::remove(int area, const data_entry &key)
  {

    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if( !key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }


    wait_object *cwo = this_wait_object_manager->create_wait_object();
    request_remove *packet = new request_remove();
    packet->area = area;
    packet->add_key(key.get_data(), key.get_size());

    base_packet *tpacket = 0;
    response_return *resp  = 0;

    int ret = TAIR_RETURN_SEND_FAILED;
    if( (ret = send_request(server_list[0],packet,cwo->get_id())) < 0){

      delete packet;
      goto FAIL;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0){
      goto FAIL;
    }
    if ( tpacket->getPCode() != TAIR_RESP_RETURN_PACKET ) {
      goto FAIL;
    }

    resp = (response_return*)tpacket;
    new_config_version = resp->config_version;
    if ( (ret = resp->get_code()) < 0) {
      if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK){
        send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
      }
      goto FAIL;
    }


    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
FAIL:

    this_wait_object_manager->destroy_wait_object(cwo);

    TBSYS_LOG(INFO, "remove failure: %s:%s",
        tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
        get_error_msg(ret));
    return ret;
  }

  int tair_client_impl::init_request_map(int area, vector<data_entry *>& keys, request_remove_map &request_removes)
  {
    if (area < 0 || area >= TAIR_MAX_AREA_COUNT)
    {
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    int ret = TAIR_RETURN_SUCCESS;
    request_remove_map::iterator rq_iter;

    vector<data_entry *>::iterator key_iter;
    for (key_iter = keys.begin(); key_iter != keys.end(); ++key_iter)
    {
      if (!key_entry_check(**key_iter))
      {
        ret = TAIR_RETURN_ITEMSIZE_ERROR;
        break;
      }

      vector<uint64_t> server_list;
      if (!get_server_id(**key_iter, server_list))
      {
        TBSYS_LOG(ERROR, "can not find serverId, return false");
        ret = -1;
        break;
      }

      request_remove* packet = NULL;
      rq_iter = request_removes.find(server_list[0]);
      if (rq_iter != request_removes.end())
      {
        packet = rq_iter->second;
      }
      else
      {
        packet = new request_remove();
        request_removes[server_list[0]] = packet;
      }

      packet->area = area;
      packet->add_key((*key_iter)->get_data(), (*key_iter)->get_size());
      //TBSYS_LOG(DEBUG,"remove from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());
    }

    if (ret < 0)
    {
      rq_iter = request_removes.begin();
      while (rq_iter != request_removes.end())
      {
        request_remove * req =  rq_iter->second;
        ++rq_iter;
        delete req;
      }
      request_removes.clear();
    }
    return ret;
  }

  int tair_client_impl::mdelete(int area, vector<data_entry*> &keys)
  {
    request_remove_map request_removes;
    int ret = TAIR_RETURN_SUCCESS;
    if ((ret = init_request_map(area, keys, request_removes)) < 0)
    {
      return ret;
    }
    wait_object* cwo = this_wait_object_manager->create_wait_object();
    request_remove_map::iterator rq_iter = request_removes.begin();

    while (rq_iter != request_removes.end())
    {
      if (send_request(rq_iter->first, rq_iter->second, cwo->get_id()) < 0)
      {
        delete rq_iter->second;
        request_removes.erase(rq_iter++);
      }
      else
      {
        ++rq_iter;
      }
    }

    ret = TAIR_RETURN_SEND_FAILED;
    vector<base_packet*> tpk;
    if ((ret = get_response(cwo, request_removes.size(), tpk)) < 1)
    {
      this_wait_object_manager->destroy_wait_object(cwo);
      TBSYS_LOG(ERROR, "all requests are failed, ret: %d", ret);
      return ret;
    }

    uint32_t send_success = 0;
    vector<base_packet *>::iterator bp_iter = tpk.begin();
    for (; bp_iter != tpk.end(); ++bp_iter)
    {
      response_return* tpacket = dynamic_cast<response_return*> (*bp_iter);
      if (tpacket->getPCode() != TAIR_RESP_RETURN_PACKET)
      {
        TBSYS_LOG(ERROR, "mdelete return pcode: %d", tpacket->getPCode());
        continue;
      }
      new_config_version = tpacket->config_version;
      if ((ret = tpacket->get_code()) < 0)
      {
        if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK)
        {
          send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        continue;
      }
      ++send_success;

      //TBSYS_LOG(DEBUG,"response from server key_count:%d",tpacket->key_count);
    }

    TBSYS_LOG(DEBUG,"mdelete keys size: %d, send success: %u, return packet size: %u", keys.size(), send_success, tpk.size());
    if (keys.size() != send_success)
    {
      ret = TAIR_RETURN_PARTIAL_SUCCESS;
    }
    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;
  }

  // add count

  int tair_client_impl::add_count(int area,

      const data_entry &key,
      int count, int *ret_count,
      int init_value /*=0*/,int  expire_time  /*=0*/)
  {
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT || expire_time < 0 ){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    if( !key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }


    wait_object *cwo = this_wait_object_manager->create_wait_object();

    request_inc_dec *packet = new request_inc_dec();
    packet->area = area;
    packet->key = key;
    packet->add_count = count;
    packet->init_value = init_value;
    packet->expired = expire_time;

    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = 0;
    response_inc_dec *resp = 0;


    if( (ret = send_request(server_list[0],packet,cwo->get_id())) < 0){

      delete packet;
      goto FAIL;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0){
      TBSYS_LOG(DEBUG,"get response failed.");
      goto FAIL;
    }

    if (tpacket->getPCode() != TAIR_RESP_INCDEC_PACKET) {
      goto FAIL;
    }
    resp = (response_inc_dec*)tpacket;
    *ret_count = resp->value;
    ret = 0;
    new_config_version = resp->config_version;


    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;

FAIL:
    if (tpacket && tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
      response_return *r = (response_return*)tpacket;
      ret = r->get_code();
      new_config_version = r->config_version;
      if(ret == TAIR_RETURN_SERVER_CAN_NOT_WORK){//just in case
        send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
      }
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    TBSYS_LOG(INFO, "add count failure: %s", get_error_msg(ret));

    return ret;
  }

  
  //zcard added by zbcwilliam at 6.29; imitate srem
  int tair_client_impl::zcard(const int area, const data_entry &key, long long &retnum) {
    //check
    if(!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //get server list
    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"zcard from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    //init packet
    int ret = TAIR_RETURN_SEND_FAILED;
    wait_object *cwo = 0;
    base_packet *tpacket = 0;
    request_zcard*request = new request_zcard();
	request->area = area;
	request->key = key;
	TBSYS_LOG(DEBUG,"zcard key=%s",key.get_data());  //get_data called
	

    cwo = this_wait_object_manager->create_wait_object();

    //send request
    if( send_request(server_list[0],request,cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        delete request;
        return ret;
    }

    //get response
    if( (ret = get_response(cwo,1,tpacket)) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");
        return ret;
    }

    assert(tpacket != NULL);

    if (tpacket->getPCode() == TAIR_RESP_ZCARD_PACKET) {
        response_zcard*response = (response_zcard*)tpacket;
        ret = response->get_code();
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end zcard:ret:%d",ret);

        new_config_version = response->config_version;

		//cout << "response->retnum is: " << response->retnum <<std::endl;
		retnum = response->retnum;//added 6.29
		
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }

  int tair_client_impl::zadd(const int area, const data_entry &key, const double score, const data_entry &value,
          const int expire, const int version)
  {
    //check
    if(!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if(!data_entry_check(value)) {
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //get server list
    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"zadd from server:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    //init packet
    int ret = TAIR_RETURN_SEND_FAILED;
    wait_object *cwo = 0;
    base_packet *tpacket = 0;
    request_zadd *packet = new request_zadd();
    packet->area=area;
    packet->version=version;
    packet->key=key;
    packet->score=score;
    packet->value=value;
    packet->expire=expire;

    cwo = this_wait_object_manager->create_wait_object();

    //send request
    if( send_request(server_list[0], packet,cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        delete packet;
        return ret;
    }

    //get response
    if( (ret = get_response(cwo, 1, tpacket)) < 0 ) {
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");
        return ret;
    }

    assert(tpacket != NULL);

    if (tpacket->getPCode() == TAIR_RESP_ZADD_PACKET) {
        response_zadd* response = (response_zadd*)tpacket;
        ret = response->get_code();
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end zadd:ret:%d",ret);

        new_config_version = response->config_version;
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }


  //zrem added 6.30
  int tair_client_impl::zrem(const int area, const data_entry &key, const data_entry &value,
          const int expire, const int version) {
    //check
    if(!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //get server list
    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"zrem from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    //init packet
    int ret = TAIR_RETURN_SEND_FAILED;
    wait_object *cwo = 0;
    base_packet *tpacket = 0;
    request_zrem *packet = new request_zrem();
	packet->area = area;
	packet->version = version;
	packet->expire = expire;
	packet->key = key;
	packet->value = value;

    cwo = this_wait_object_manager->create_wait_object();

    //send request
    if( send_request(server_list[0],packet,cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        delete packet;
        return ret;
    }

    //get response
    if( (ret = get_response(cwo,1,tpacket)) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");
        return ret;
    }

    assert(tpacket != NULL);

    if (tpacket->getPCode() == TAIR_RESP_ZREM_PACKET) {
        response_zrem *response = (response_zrem *)tpacket;
        ret = response->get_code();
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end zrem:ret:%d",ret);

        new_config_version = response->config_version;
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }  

  
  //zrange
  int tair_client_impl::zrange (const int area, const data_entry & key, int32_t start,int32_t end,
      vector <data_entry *> &values, vector<double> &scores,int32_t withscore)
  {
    //check
    if(!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //get server list
    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"zrange from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

	//init packet
    int ret = TAIR_RETURN_SEND_FAILED;
    wait_object *cwo = NULL;
    base_packet *tpacket = NULL;
    request_zrange *request = new request_zrange();    //changed
    request->area=area;
    request->key=key;
    request->start=start;
    request->end=end;
	request->withscore=withscore;//6.11-22.22
    TBSYS_LOG(DEBUG,"zrange key=%s",key.get_data());  //get_data called

    cwo = this_wait_object_manager->create_wait_object();

    //send request
    if( send_request(server_list[0], request, cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        delete request;
		printf("send request OK\n");
        return ret;
    }

    TBSYS_LOG(DEBUG,"zrange get_response");				//changed

    //get response
    if( (ret = get_response(cwo, 1, tpacket)) < 0 )/*get_response make tpacket not null*/ {
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");  //error occours
        return ret;
    }

    assert(tpacket != NULL);
	printf("assert tpacket not null OK\n");

    if (tpacket->getPCode() == TAIR_RESP_ZRANGE_PACKET) {
        response_zrange*response = (response_zrange*)tpacket;
        ret = response->get_code();         //get_code called;
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end zrange:ret:%d",ret);

        response->alloc_free(false);         //added 6.25
        scores = response->scores;			//added
        values = response->values;			//pay attention !!!
        /*for(size_t i = 0; i < values.size(); i++) {
		  printf("values[%d] is %s\n",i,hexStr(values[i]->get_data(),values[i]->get_size()).c_str());
        }*///added 6.25
        new_config_version = response->config_version;
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();            //get_code is used here. attention!!
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }

  //zrevrange added 6.30
  int tair_client_impl::zrevrange (const int area, const data_entry & key, int32_t start,int32_t end,
      vector <data_entry *> &values, vector<double> &scores,int32_t withscore)
  {
    //check
    if(!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //get server list
    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"zrevrange from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

	//init packet
    int ret = TAIR_RETURN_SEND_FAILED;
    wait_object *cwo = NULL;
    base_packet *tpacket = NULL;
    request_zrevrange *request = new request_zrevrange();    //changed
    request->area=area;
    request->key=key;
    request->start=start;
    request->end=end;
	request->withscore=withscore;//6.11-22.22
    TBSYS_LOG(DEBUG,"zrevrange key=%s",key.get_data());  //get_data called

    cwo = this_wait_object_manager->create_wait_object();

    //send request
    if( send_request(server_list[0], request, cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        delete request;
		printf("send request OK\n");
        return ret;
    }

    TBSYS_LOG(DEBUG,"zrevrange get_response");				//changed

    //get response
    if( (ret = get_response(cwo, 1, tpacket)) < 0 )/*get_response make tpacket not null*/ {
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");  //error occours
        return ret;
    }

    assert(tpacket != NULL);
	printf("assert tpacket not null OK\n");

    if (tpacket->getPCode() == TAIR_RESP_ZREVRANGE_PACKET) {
        response_zrevrange*response = (response_zrevrange*)tpacket;
        ret = response->get_code();         //get_code called;
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end zrevrange:ret:%d",ret);

        response->alloc_free(false);         //added 6.25
        scores = response->scores;			//added
        values = response->values;			//pay attention !!!
        /*for(size_t i = 0; i < values.size(); i++) {
		  printf("values[%d] is %s\n",i,hexStr(values[i]->get_data(),values[i]->get_size()).c_str());
        }*///added 6.25
        new_config_version = response->config_version;
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();            //get_code is used here. attention!!
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }

//zrangebyscore
  int tair_client_impl::zrangebyscore (const int area, const data_entry & key, const double start,const double end,
      vector <data_entry *> &values, vector<double> &scores,const int limit,const int withscore)
  {
    //check
    if(!key_entry_check(key)){
      return TAIR_RETURN_ITEMSIZE_ERROR;
    }
    if( area < 0 || area >= TAIR_MAX_AREA_COUNT){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //get server list
    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find serverId, return false");
      return -1;
    }
    TBSYS_LOG(DEBUG,"zrangebyscore from server:%s",tbsys::CNetUtil::addrToString(server_list[0]).c_str());

    int ret = TAIR_RETURN_SEND_FAILED;
    wait_object *cwo = NULL;
    base_packet *tpacket = NULL;
    request_zrangebyscore *request = new request_zrangebyscore();
    request->area=area;
    request->key=key;
    request->start=start;
    request->end=end;
    TBSYS_LOG(DEBUG,"zrangebyscore key=%s",key.get_data());
	//TBSYS_LOG(ERROR,"zrangebyscore key=%s\n",key.get_data());  //added 6.25

    cwo = this_wait_object_manager->create_wait_object();
	//TBSYS_LOG(ERROR,"cwo->get_id=%d\n",cwo->get_id());  //added 6.25

    //send request
    if( send_request(server_list[0], request, cwo->get_id()) < 0 ){
        this_wait_object_manager->destroy_wait_object(cwo);
        delete request;
		printf("send request OK\n");
        return ret;
    }

    TBSYS_LOG(DEBUG,"zrangebyscore get_response");

    //get response
    if( (ret = get_response(cwo, 1, tpacket)) < 0 ) {
        this_wait_object_manager->destroy_wait_object(cwo);
        TBSYS_LOG(ERROR,"all requests are failed");
        return ret;
    }

    assert(tpacket != NULL);
	printf("assert tpacket not null OK\n");

    if (tpacket->getPCode() == TAIR_RESP_ZRANGEBYSCORE_PACKET) {
        response_zrangebyscore *response = (response_zrangebyscore *)tpacket;
        ret = response->get_code();
        if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
            send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
        }
        TBSYS_LOG(DEBUG, "end zrangebyscore:ret:%d",ret);

        response->alloc_free(false);
        values = response->values;
        new_config_version = response->config_version;
        this_wait_object_manager->destroy_wait_object(cwo);
    } else {
        if (tpacket->getPCode() == TAIR_RESP_RETURN_PACKET) {
            response_return *r = (response_return *)tpacket;
            ret = r->get_code();
            if (ret == TAIR_RETURN_SERVER_CAN_NOT_WORK) {
                new_config_version = r->config_version;
                send_fail_count = UPDATE_SERVER_TABLE_INTERVAL;
            }
        }

        this_wait_object_manager->destroy_wait_object(cwo);

        TBSYS_LOG(WARN, " failure: %s:%s",
            tbsys::CNetUtil::addrToString(server_list[0]).c_str(),
            get_error_msg(ret));
    }

    return ret;
  }

  int tair_client_impl::remove_area(int area, bool lazy)
  {

    if( UNLIKELY(area < -4  || area >= TAIR_MAX_AREA_COUNT)){
      return TAIR_RETURN_INVALID_ARGUMENT;
    }

    //1.send request to all server
    map<uint64_t,base_packet *> request_list;
    map<uint64_t,base_packet *>::iterator it;
    for (uint32_t i=0; i<ds_server_list.size(); i++) {
      uint64_t server_id = ds_server_list[i];
      if (server_id == 0) {
        continue;
      }
      it = request_list.find(server_id);
      if (it == request_list.end()) {
        base_packet *packet = NULL;
        if (lazy) {
          packet = new request_lazy_remove_area();
          ((request_lazy_remove_area*)(packet))->area = area;
        } else {
          packet = new request_remove_area();
          ((request_remove_area*)(packet))->area = area;
        }
        request_list[server_id] = packet;
      }
    }
    if(request_list.empty()){
      return TAIR_RETURN_SUCCESS;
    }
    //2. send request

    wait_object *cwo = this_wait_object_manager->create_wait_object();

    int send_count = 0;
    int ret = TAIR_RETURN_SEND_FAILED;
    for (it=request_list.begin(); it!=request_list.end(); ++it) {
      uint64_t server_id = it->first;
      base_packet *packet = it->second;
      TBSYS_LOG(INFO, "request_remove_area=>%s", tbsys::CNetUtil::addrToString(server_id).c_str());

      if( (ret = send_request(server_id,packet,cwo->get_id())) < 0){

        delete packet;
      }else {
        ++send_count;
      }
    }
    base_packet *tpacket = 0;
    if(send_count > 0){
      if( (ret = get_response(cwo,send_count,tpacket)) < 0){
        //TODO log
      }
    }


    this_wait_object_manager->destroy_wait_object(cwo);

    return ret;
  }

  int tair_client_impl::dump_area(set<dump_meta_info>& info)
  {
    //1.send request to all server
    map<uint64_t,request_dump *> request_list;
    map<uint64_t,request_dump *>::iterator it;
    for (uint32_t i=0; i<ds_server_list.size(); ++i) {
      uint64_t serverId = ds_server_list[i];
      if (serverId == 0) {
        continue;
      }
      it = request_list.find(serverId);
      if (it == request_list.end()) {
        request_dump *packet = new request_dump();
        packet->info_set = info;
        request_list[serverId] = packet;
      }
    }
    if(request_list.empty()){
      return TAIR_RETURN_SUCCESS;
    }
    //2. send request

    wait_object *cwo = this_wait_object_manager->create_wait_object();

    int sendCount = 0;
    int ret = TAIR_RETURN_SEND_FAILED;
    for (it=request_list.begin(); it!=request_list.end(); ++it) {
      uint64_t serverId = it->first;
      request_dump *packet = it->second;
      TBSYS_LOG(INFO, "dump_area=>%s", tbsys::CNetUtil::addrToString(serverId).c_str());

      if( (ret = send_request(serverId,packet,cwo->get_id())) < 0){

        delete packet;
      }else {
        ++sendCount;
      }
    }
    base_packet *tpacket = 0;
    if(sendCount > 0){
      if( (ret = get_response(cwo,sendCount,tpacket)) < 0){
        //TODO log
      }
    }

    this_wait_object_manager->destroy_wait_object(cwo);
    return ret;

  }

  void tair_client_impl::set_timeout(int this_timeout)
  {
    assert(timeout >= 0);
    timeout = this_timeout;
  }

  /*-----------------------------------------------------------------------------
   *  tair_client_impl::ErrMsg
   *-----------------------------------------------------------------------------*/


  const std::map<int,string> tair_client_impl::m_errmsg = tair_client_impl::init_errmsg();

  std::map<int,string> tair_client_impl::init_errmsg()
  {
    std::map<int,string> temp;
    temp[TAIR_RETURN_SUCCESS]                 = "success";
    temp[TAIR_RETURN_FAILED]          = "general failed";
    temp[TAIR_RETURN_DATA_NOT_EXIST]  = "data not exists";
    temp[TAIR_RETURN_VERSION_ERROR]           = "version error";
    temp[TAIR_RETURN_TYPE_NOT_MATCH]  = "data type not match";
    temp[TAIR_RETURN_ITEM_EMPTY]              = "item is empty";
    temp[TAIR_RETURN_SERIALIZE_ERROR] = "serialize failed";
    temp[TAIR_RETURN_OUT_OF_RANGE]            = "item's index out of range";
    temp[TAIR_RETURN_ITEMSIZE_ERROR]  = "key or vlaue too large";
    temp[TAIR_RETURN_SEND_FAILED]             = "send packet error";
    temp[TAIR_RETURN_TIMEOUT]         = "timeout";
    temp[TAIR_RETURN_SERVER_CAN_NOT_WORK]   = "server can not work";
    temp[TAIR_RETURN_WRITE_NOT_ON_MASTER]   = "write not on master";
    temp[TAIR_RETURN_DUPLICATE_BUSY]          = "duplicate busy";
    temp[TAIR_RETURN_MIGRATE_BUSY]    = "migrate busy";
    temp[TAIR_RETURN_PARTIAL_SUCCESS] = "partial success";
    temp[TAIR_RETURN_DATA_EXPIRED]            = "expired";
    temp[TAIR_RETURN_PLUGIN_ERROR]            = "plugin error";
    temp[TAIR_RETURN_PROXYED]                 = "porxied";
    temp[TAIR_RETURN_INVALID_ARGUMENT]        = "invalid argument";
    temp[TAIR_RETURN_CANNOT_OVERRIDE] = "cann't override old value.please check and remove it first.";
    return temp;
  }

  const char *tair_client_impl::get_error_msg(int ret)
  {
    std::map<int,string>::const_iterator it = tair_client_impl::m_errmsg.find(ret);
    return it != tair_client_impl::m_errmsg.end() ? it->second.c_str() : "unknow";
  }

  void tair_client_impl::get_server_with_key(const data_entry& key,std::vector<std::string>& servers)
  {
    if( !key_entry_check(key)){
      return;
    }

    vector<uint64_t> server_list;
    if ( !get_server_id(key, server_list)) {
      TBSYS_LOG(DEBUG, "can not find server_id, return false");
      return;
    }

    for(vector<uint64_t>::iterator it= server_list.begin(); it != server_list.end(); ++it){
      servers.push_back(tbsys::CNetUtil::addrToString(*it));
    }
    return;
  }



  // @override IPacketHandler
  tbnet::IPacketHandler::HPRetCode tair_client_impl::handlePacket(tbnet::Packet *packet, void *args)
  {
    if (!packet->isRegularPacket()) {
      tbnet::ControlPacket *cp = (tbnet::ControlPacket*)packet;
      TBSYS_LOG(WARN, "ControlPacket, cmd:%d", cp->getCommand());
      ++send_fail_count;
      if (cp->getCommand() == tbnet::ControlPacket::CMD_DISCONN_PACKET) {
        return tbnet::IPacketHandler::FREE_CHANNEL;
      }
    }

    int id = static_cast<int> ((reinterpret_cast<long>(args)));
    if (id) {
      if (packet->isRegularPacket()) {

        this_wait_object_manager->wakeup_wait_object(id, (base_packet*)packet);

      } else {
        this_wait_object_manager->wakeup_wait_object(id, NULL);

      }
    } else if (packet->isRegularPacket()) {
      if (packet->getPCode() == TAIR_RESP_GET_GROUP_PACKET && args == NULL) {
        response_get_group *rggp = (response_get_group *)packet;
        new_config_version = rggp->config_version;
        if (config_version != new_config_version) {
          uint64_t *server_list = rggp->get_server_list(bucket_count, copy_count);
          if (server_list != NULL) {
            uint32_t min = (rggp->server_list_count > ds_server_list.size() ?
                 ds_server_list.size() : rggp->server_list_count);
            for (uint32_t i = 0; i < min; i++) {
              TBSYS_LOG(DEBUG, "update server table: [%d] => [%s]",
                      i, tbsys::CNetUtil::addrToString(server_list[i]).c_str());
              ds_server_list[i] = server_list[i];
            }
          }

          TBSYS_LOG(INFO, "config_version: %u => %u", config_version, rggp->config_version);
          config_version = new_config_version;
        }
      }
      delete packet;
    }

    return tbnet::IPacketHandler::KEEP_CHANNEL;
  }

  //@override Runnable
  void tair_client_impl::run(tbsys::CThread *thread, void *arg)
  {
    if (config_server_list.size() == 0U) {
      return;
    }

    int config_server_index = 0;
    uint32_t old_config_version = 0;
    // int loopCount = 0;

    while (!is_stop) {
      //++loopCount;
      TAIR_SLEEP(is_stop, 1);
      if (is_stop) break;

      if(config_version < TAIR_CONFIG_MIN_VERSION){
        //ought to update
      } else if (config_version == new_config_version && send_fail_count < UPDATE_SERVER_TABLE_INTERVAL) {
        continue;
      }

      old_config_version = new_config_version;

      config_server_index ++;
      config_server_index %= config_server_list.size();
      uint64_t serverId = config_server_list[config_server_index];

      TBSYS_LOG(WARN, "send request to configserver(%s), config_version: %u, new_config_version: %d",
          tbsys::CNetUtil::addrToString(serverId).c_str(), config_version, new_config_version);

      request_get_group *packet = new request_get_group();
      packet->set_group_name(group_name.c_str());
      if (connmgr->sendPacket(serverId, packet, NULL, NULL) == false) {
        TBSYS_LOG(ERROR, "send request_get_group to %s failure.",
            tbsys::CNetUtil::addrToString(serverId).c_str());
        delete packet;
      }
      send_fail_count = 0;
    }
  }


  void tair_client_impl::force_change_dataserver_status(uint64_t server_id, int cmd)
  {
    for (size_t i = 0; i < config_server_list.size(); ++i) {
      uint64_t conf_serverId = config_server_list[i];
      request_data_server_ctrl *packet = new request_data_server_ctrl();
      packet->server_id = server_id;
      packet->cmd = cmd;
      if (connmgr->sendPacket(conf_serverId, packet, NULL, NULL) == false) {
        TBSYS_LOG(ERROR, "send request_data_server_ctrl to %s failure.",
            tbsys::CNetUtil::addrToString(server_id).c_str());
        delete packet;
      }
    }
  }

  void tair_client_impl::get_migrate_status(uint64_t server_id,vector<pair<uint64_t,uint32_t> >& result)
  {

    if(config_server_list.size() == 0){
      TBSYS_LOG(WARN,"config server list is empty");
      return;
    }

    request_get_migrate_machine *packet = new request_get_migrate_machine();
    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = 0;
    response_get_migrate_machine *resp = 0;

    packet->server_id = server_id;


    wait_object *cwo = this_wait_object_manager->create_wait_object();


    if( (ret = send_request(config_server_list[0],packet,cwo->get_id())) < 0){

      delete packet;
      goto FAIL;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0){
      goto FAIL;
    }

    if(tpacket == 0 || tpacket->getPCode() != TAIR_RESP_GET_MIGRATE_MACHINE_PACKET){
      goto FAIL;
    }

    resp = (response_get_migrate_machine *)tpacket;
    log_debug("resp->_vec_ms = %d", resp->vec_ms.size());
    result = resp->vec_ms;

    this_wait_object_manager->destroy_wait_object(cwo);

    return;
FAIL:

    TBSYS_LOG(DEBUG,"get_migrate_status failed:%s",get_error_msg(ret));
    this_wait_object_manager->destroy_wait_object(cwo);

  }
  void tair_client_impl::query_from_configserver(uint32_t query_type, const string group_name, map<string, string>& out, uint64_t serverId)
  {

    if(config_server_list.size() == 0){
      TBSYS_LOG(WARN,"config server list is empty");
      return;
    }


    request_query_info *packet = new request_query_info();
    int ret = TAIR_RETURN_SEND_FAILED;
    base_packet *tpacket = 0;
    response_query_info *resp = 0;

    packet->query_type = query_type;
    packet->group_name = group_name;
    packet->server_id  = serverId;


    wait_object *cwo = this_wait_object_manager->create_wait_object();



    if( (ret = send_request(config_server_list[0],packet,cwo->get_id())) < 0){

      delete packet;
      goto FAIL;
    }

    if( (ret = get_response(cwo,1,tpacket)) < 0){
      goto FAIL;
    }

    if(tpacket == 0 || tpacket->getPCode() != TAIR_RESP_QUERY_INFO_PACKET){
      goto FAIL;
    }

    resp = (response_query_info*)tpacket;
    out = resp->map_k_v;

    this_wait_object_manager->destroy_wait_object(cwo);

    return;
FAIL:

    TBSYS_LOG(DEBUG,"query from config server failed:%s",get_error_msg(ret));
    this_wait_object_manager->destroy_wait_object(cwo);

  }

  bool tair_client_impl::retrieve_server_addr()
  {
    if (config_server_list.size() == 0U || group_name.empty()) {
      TBSYS_LOG(WARN, "config server list is empty, or groupname is NULL, return false");
      return false;
    }
    wait_object *cwo = 0;
    int send_success = 0;

    response_get_group *rggp = 0;
    base_packet *tpacket = NULL;

    for (uint32_t i = 0; i < config_server_list.size(); i++) {
      uint64_t server_id = config_server_list[i];
      request_get_group *packet = new request_get_group();
      packet->set_group_name(group_name.c_str());


      cwo = this_wait_object_manager->create_wait_object();
      if (connmgr->sendPacket(server_id, packet, NULL, (void*)((long)cwo->get_id())) == false) {
        TBSYS_LOG(ERROR, "Send RequestGetGroupPacket to %s failure.",
            tbsys::CNetUtil::addrToString(server_id).c_str());
        this_wait_object_manager->destroy_wait_object(cwo);

        delete packet;
        continue;
      }
      cwo->wait_done(1,timeout);
      tpacket = cwo->get_packet();
      if (tpacket == 0 || tpacket->getPCode() != TAIR_RESP_GET_GROUP_PACKET){
        TBSYS_LOG(ERROR,"get group packet failed,retry");
        tpacket = 0;

        this_wait_object_manager->destroy_wait_object(cwo);

        cwo = 0;
        continue;
      }else{
        ++send_success;
        break;
      }
    }

    if (send_success <= 0 ) {
      TBSYS_LOG(ERROR,"cann't connect");
      return false;
    }

    //deal with response_get_group
    uint64_t *server_list = NULL ;
    uint32_t server_list_count = 0;

    rggp = dynamic_cast<response_get_group*>(tpacket);
    if (rggp->config_version <= 0) {
      TBSYS_LOG(ERROR, "group doesn't exist: %s", group_name.c_str());
      this_wait_object_manager->destroy_wait_object(cwo);
      return false;
    }

    if(rggp->bucket_count <= 0 || rggp->copy_count <= 0){
      TBSYS_LOG(ERROR, "bucket or copy count doesn't correct");
      this_wait_object_manager->destroy_wait_object(cwo);
      return false;
    }

    bucket_count = rggp->bucket_count;
    copy_count = rggp->copy_count;

    server_list = rggp->get_server_list(bucket_count, copy_count);
    if(rggp->server_list_count <= 0 || server_list == NULL){
      TBSYS_LOG(WARN, "server table is empty");
      this_wait_object_manager->destroy_wait_object(cwo);
      return false;
    }

    server_list_count = (uint32_t)(rggp->server_list_count);
    assert(server_list_count == bucket_count * copy_count);

    for (uint32_t i = 0; i < server_list_count; ++i) {
      //TBSYS_LOG(DEBUG, "server table: [%d] => [%s]", i, tbsys::CNetUtil::addrToString(ds_server_list[i]).c_str());
      ds_server_list.push_back(server_list[i]);
    }

    new_config_version = config_version = rggp->config_version; //set the same value on the first time.
    this_wait_object_manager->destroy_wait_object(cwo);
    thread.start(this, NULL);
    return true;
  }

  void tair_client_impl::start_tbnet()
  {
    connmgr->setDefaultQueueTimeout(0, timeout);
    connmgr->setDefaultQueueLimit(0, 1000);
    transport->start();
  }


  void tair_client_impl::stop_tbnet()
  {
    transport->stop();
  }

  void tair_client_impl::wait_tbnet()
  {
    transport->wait();
  }

  void tair_client_impl::reset() //reset enviroment
  {
    timeout = 2000;
    config_version = 0;
    new_config_version = 0;
    send_fail_count = 0;
    bucket_count = 0;
    copy_count = 0;
    ds_server_list.clear();
    config_server_list.clear();
    group_name = "";

    packet_factory = NULL;
    streamer = NULL;
    transport = NULL;
    connmgr = NULL;
    this_wait_object_manager = NULL;

    inited = false;
  }

  bool tair_client_impl::get_server_id(uint32_t hash, vector<uint64_t>& server)
  {
    server.clear();
    hash %= bucket_count;
    for(uint32_t i = 0; i < copy_count && i < ds_server_list.size(); ++i) {
      uint64_t server_id = ds_server_list[hash + i * bucket_count];
      if (server_id) {
          server.push_back(server_id);
      }
    }

    return (server.size() > 0 ? true : false);
  }

  bool tair_client_impl::get_server_id(const data_entry &key, vector<uint64_t>& server)
  {
    uint32_t hash = util::string_util::mur_mur_hash(key.get_data(), key.get_size());
    return get_server_id(hash, server);
  }

  int tair_client_impl::send_request(uint64_t server_id, base_packet *packet, int waitId)
  {
    assert(server_id != 0 && packet != 0 && waitId >= 0);
    if (connmgr->sendPacket(server_id, packet, NULL, (void*)((long)waitId)) == false) {
      TBSYS_LOG(ERROR, "Send RequestGetPacket to %s failure.",
          tbsys::CNetUtil::addrToString(server_id).c_str());
      send_fail_count ++;
      return TAIR_RETURN_SEND_FAILED;
    }
    return 0;
  }


  int tair_client_impl::get_response(wait_object *cwo,int wait_count,base_packet*& tpacket)

  {
    assert(cwo != 0 && wait_count >= 0);
    cwo->wait_done(wait_count, timeout);
    base_packet *packet = cwo->get_packet();
    if(packet == 0){
      return TAIR_RETURN_TIMEOUT;
    }
    tpacket = packet;
    return 0;
  }

  int tair_client_impl::get_response(wait_object *cwo, int wait_count, vector<base_packet*>& tpacket)
  {
    assert(cwo != NULL && wait_count >= 0);
    cwo->wait_done(wait_count, timeout);
    int r_num = cwo->get_packet_count();
    int push_num = (r_num < wait_count) ? r_num: wait_count;

    for (int idx = 0 ; idx < push_num ; ++ idx)
    {
      base_packet *packet = cwo->get_packet(idx);
      if(packet == NULL){
        return TAIR_RETURN_TIMEOUT;
      }
      tpacket.push_back(packet);
    }
    return push_num;
  }

  bool tair_client_impl::key_entry_check(const data_entry& key)
  {
    if( key.get_size() == 0 || key.get_data() == 0 ){
      return false;
    }
    if( (key.get_size() == 1) && ( *(key.get_data()) == '\0')){
      return false;
    }
    if( key.get_size() >= TAIR_MAX_KEY_SIZE ){
      return false;
    }
    return true;
  }


  bool tair_client_impl::data_entry_check(const data_entry& data)
  {
    if( data.get_size() == 0 || data.get_data() == 0 ){
      return false;
    }
    if( data.get_size() >= TAIR_MAX_DATA_SIZE ){
      return false;
    }
    return true;
  }

  bool tair_client_impl::map_data_check(const map<data_entry*, data_entry*> &md)
  {
    map<data_entry*, data_entry*>::const_iterator iter;
    for(iter = md.begin(); iter != md.end(); iter++) {
        if(!data_entry_check(*(iter->first))) {
            return false;
        }
        if(!data_entry_check(*(iter->second))) {
            return false;
        }
    }
    return true;
  }

  bool tair_client_impl::vector_data_check(const vector<data_entry*> &vd)
  {
    size_t vd_len = vd.size();
    for(size_t i = 0; i < vd_len; i++) {
        if(!data_entry_check(*(vd[i]))) {
            return false;
        }
    }
    return true;
  }

  bool tair_client_impl::vector_key_check(const vector<data_entry*> &vk)
  {
    size_t vk_len = vk.size();
    for(size_t i = 0; i < vk_len; i++) {
        if(!key_entry_check(*(vk[i]))) {
            return false;
        }
    }
    return true;
  }
}
