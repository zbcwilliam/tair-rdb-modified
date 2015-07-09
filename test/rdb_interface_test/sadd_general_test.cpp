#include "base_test.hpp"

//int sadd(int area, data_entry &key, data_entry &value, int expire, int version);

TEST_F(TairClientRDBTest, test_sadd_expire_timestamp) {
    //set expire with timestamp
    int resultcode;
    MVDE* mvde = mapv_data_entry_rand(128, 128, 1, 10);

    MVDE::iterator iter;
    for(iter = mvde->begin(); iter != mvde->end(); iter++) {
        data_entry* key = iter->first;
        VDE* vde = iter->second;
        
        VDE::iterator iter2;
        for(size_t i = 0; i < vde->size(); i++) {
            data_entry* value = (*vde)[i];
            resultcode = tairClient.sadd(TairClientRDBTest::default_namespace, *key, *value,
                    time(NULL) + 5, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        }

        sleep(2);
    
        VDE* ret_values = new VDE();
        resultcode = tairClient.smembers(TairClientRDBTest::default_namespace, *key, *ret_values);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        if (set_data_entry_cmp(vde, ret_values)) {
            ASSERT_TRUE(1);
        } else {
            ASSERT_TRUE(0);
        }
        vector_data_entry_free(ret_values);

        sleep(4);

        resultcode = tairClient.remove(TairClientRDBTest::default_namespace, *key);
        ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
    }

    mapv_data_entry_free(mvde, 1);
}

TEST_F(TairClientRDBTest, test_sadd_expire_timeinterval) {
    //set expire with time interval
    int resultcode;
    MVDE* mvde = mapv_data_entry_rand(128, 128, 1, 10);

    MVDE::iterator iter;
    for(iter = mvde->begin(); iter != mvde->end(); iter++) {
        data_entry* key = iter->first;
        VDE* vde = iter->second;
        
        VDE::iterator iter2;
        for(size_t i = 0; i < vde->size(); i++) {
            data_entry* value = (*vde)[i];
            resultcode = tairClient.sadd(TairClientRDBTest::default_namespace, *key, *value,
                    time(NULL) + 5, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        }

        sleep(2);
    
        VDE* ret_values = new VDE();
        resultcode = tairClient.smembers(TairClientRDBTest::default_namespace, *key, *ret_values);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        if (set_data_entry_cmp(vde, ret_values)) {
            ASSERT_TRUE(1);
        } else {
            ASSERT_TRUE(0);
        }
        vector_data_entry_free(ret_values);

        sleep(4);

        resultcode = tairClient.remove(TairClientRDBTest::default_namespace, *key);
        ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
    }

    mapv_data_entry_free(mvde, 1);
}

TEST_F(TairClientRDBTest, test_sadd_expire_cancel) {
    int resultcode;

    data_entry* key = data_entry_rand(128);
    VDE* vde1 = vector_data_entry_rand(8, 10);
    VDE* vde2 = vector_data_entry_rand(8, 10);

    for(size_t i = 0; i < vde1->size(); i++) {
        data_entry* value = (*vde1)[i];
        resultcode = tairClient.sadd(TairClientRDBTest::default_namespace, *key,
                *value, 5, NOT_CARE_VERSION);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    }

    sleep(2);

    VDE* ret_values = new VDE();
    resultcode = tairClient.smembers(TairClientRDBTest::default_namespace, *key,
            *ret_values);
    ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    if (set_data_entry_cmp(vde1, ret_values)) {
        ASSERT_TRUE(1);
    } else {
        ASSERT_TRUE(0);
    }
    vector_data_entry_free(ret_values);

    for(size_t i = 0; i < vde2->size(); i++) {
        data_entry* value = (*vde2)[i];
        resultcode = tairClient.sadd(TairClientRDBTest::default_namespace, *key,
                *value, CANCEL_EXPIRE, NOT_CARE_VERSION);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    }
        
    sleep(4);

    resultcode = tairClient.remove(TairClientRDBTest::default_namespace, *key);
    ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);

    data_entry_free(key, 1);
    vector_data_entry_free(vde1, 1);
    vector_data_entry_free(vde2, 1);
}

TEST_F(TairClientRDBTest, test_sadd_version_care) {
    int resultcode;

    data_entry* key = data_entry_rand(128);
    data_entry* field = data_entry_rand(128);
    data_entry* value = data_entry_rand(128);
    VDE* vde0 = vector_data_entry_rand(128, 100);
    VDE* vde1 = vector_data_entry_rand(128, 100);

    size_t index = 0;
    for(; index < vde0->size(); index++) {
        data_entry* tvalue = (*vde0)[index];
        resultcode = tairClient.sadd(TairClientRDBTest::default_namespace, *key,
                *tvalue, NOT_CARE_EXPIRE, index);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    }

    resultcode = tairClient.sadd(TairClientRDBTest::default_namespace, *key,
            *value, NOT_CARE_EXPIRE, 99);
    ASSERT_EQ(resultcode, TAIR_RETURN_VERSION_ERROR);

    resultcode = tairClient.sadd(TairClientRDBTest::default_namespace, *key,
            *value, NOT_CARE_EXPIRE, 101);
    ASSERT_EQ(resultcode, TAIR_RETURN_VERSION_ERROR);

    resultcode = tairClient.sadd(TairClientRDBTest::default_namespace, *key,
            *value, NOT_CARE_EXPIRE, 0);
    ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);

    index++;

    for(size_t i = 0; i < vde1->size(); i++) {
        data_entry* tvalue = (*vde1)[i];
        resultcode = tairClient.sadd(TairClientRDBTest::default_namespace, *key,
            *tvalue, NOT_CARE_EXPIRE, index++);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    }

    resultcode = tairClient.remove(TairClientRDBTest::default_namespace, *key);
    ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);

    data_entry_free(key, 1);
    data_entry_free(field, 1);
    data_entry_free(value, 1);

    vector_data_entry_free(vde0, 1);
    vector_data_entry_free(vde1, 1);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

