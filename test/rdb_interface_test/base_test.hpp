#ifndef __BASE_TEST__
#define __BASE_TEST__

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <gtest/gtest.h>

#include "test_helper.hpp"

#include "tair_client_api_impl.hpp"

using namespace tair;

//const static short NOT_CARE_VERSION = 0;
//const static int NOT_CARE_EXPIRE = -1;
//const static int CANCEL_EXPIRE = 0;


class TairClientRDBTest:public testing::Test {
protected:
    virtual void SetUp() {
        //init
        TBSYS_LOGGER.setLogLevel("ERROR");
        bool done = tairClient.startup(server_addr, slave_server_addr, group_name);
        ASSERT_EQ(done, true);
        printf("---------------------start up----------------------\n");
    }

    virtual void TearDown() {
        //finit
        tairClient.close();
        printf("---------------------close-------------------------\n");
    }
public:
    tair_client_impl tairClient;
public:
    const static char* server_addr;
    const static char* slave_server_addr;
    const static char* group_name;

    const static int default_namespace;
    const static int max_namespace;
};

const char* TairClientRDBTest::server_addr = "localhost:5198";
const char* TairClientRDBTest::slave_server_addr = NULL;
const char* TairClientRDBTest::group_name = "group_1";

const int TairClientRDBTest::default_namespace = 4;
const int TairClientRDBTest::max_namespace = 16;


#endif
