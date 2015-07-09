#include "base_test.hpp"

//int srem(int area, data_entry &key, data_entry &value, int expire, int version);

TEST_F(TairClientRDBTest, test_srem_expire_timestamp) {
    int resultcode;
    MVDE* mvde = mapv_data_entry_rand(128, 128, 1, 10);

    MVDE::iterator iter;
    for(iter = mvde->begin(); iter != mvde->end(); iter++) {
        data_entry* key = iter->first;
        VDE* vde = iter->second;

        for(size_t i = 0; i < vde->size(); i++) {
            data_entry* value = (*vde)[i];
            resultcode = tairClient.sadd(TairClientRDBTest::default_namespace,
                    *key, *value, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode , TAIR_RETURN_SUCCESS);
        }

        resultcode = tairClient.srem(TairClientRDBTest::default_namespace,
                *key, *((*vde)[0]), time(NULL) + 5, NOT_CARE_VERSION);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);

        sleep(2);

        VDE* ret_value = new VDE();
        resultcode = tairClient.smembers(TairClientRDBTest::default_namespace,
                *key, *ret_value);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(ret_value->size(), vde->size() - 1);

        vector_data_entry_free(ret_value);

        sleep(4);

        resultcode = tairClient.remove(TairClientRDBTest::default_namespace, *key);
        ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
    }

    mapv_data_entry_free(mvde, 1);
}

TEST_F(TairClientRDBTest, test_srem_expire_timeintverval) {
    int resultcode;
    MVDE* mvde = mapv_data_entry_rand(128, 128, 1, 10);

    MVDE::iterator iter;
    for(iter = mvde->begin(); iter != mvde->end(); iter++) {
        data_entry* key = iter->first;
        VDE* vde = iter->second;

        for(size_t i = 0; i < vde->size(); i++) {
            data_entry* value = (*vde)[i];
            resultcode = tairClient.sadd(TairClientRDBTest::default_namespace,
                    *key, *value, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode , TAIR_RETURN_SUCCESS);
        }

        resultcode = tairClient.srem(TairClientRDBTest::default_namespace,
                *key, *((*vde)[0]), 5, NOT_CARE_VERSION);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);

        sleep(2);

        VDE* ret_value = new VDE();
        resultcode = tairClient.smembers(TairClientRDBTest::default_namespace,
                *key, *ret_value);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(ret_value->size(), vde->size() - 1);

        vector_data_entry_free(ret_value);

        sleep(4);

        resultcode = tairClient.remove(TairClientRDBTest::default_namespace, *key);
        ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
    }

    mapv_data_entry_free(mvde, 1);
}

TEST_F(TairClientRDBTest, test_srem_expire_cancel) {
    int resultcode;
    data_entry* key = data_entry_rand(128);
    VDE* vde = vector_data_entry_rand(128, 100);

    for(size_t i = 0; i < vde->size(); i++) {
        data_entry* value = (*vde)[i];
        resultcode = tairClient.sadd(TairClientRDBTest::default_namespace,
                *key, *value, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    }

    resultcode = tairClient.srem(TairClientRDBTest::default_namespace,
            *key, *((*vde)[0]), 5, NOT_CARE_VERSION);
    ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);

    sleep(2);

    VDE* ret_value = new VDE();
    resultcode = tairClient.smembers(TairClientRDBTest::default_namespace,
            *key, *ret_value);
    ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(ret_value->size(), vde->size() - 1);

    vector_data_entry_free(ret_value);

    resultcode = tairClient.srem(TairClientRDBTest::default_namespace,
            *key, *((*vde)[1]), CANCEL_EXPIRE, NOT_CARE_VERSION);
    ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    
    sleep(4);

    resultcode = tairClient.remove(TairClientRDBTest::default_namespace, *key);
    ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);

    data_entry_free(key, 1);
    vector_data_entry_free(vde, 1);
}

TEST_F(TairClientRDBTest, test_srem_version_care) {
    int resultcode;
    data_entry* key = data_entry_rand(128);
    VDE* vde0 = vector_data_entry_rand(128, 100);
    VDE* vde1 = vector_data_entry_rand(128, 100);

    for(int i = 0; i < 100; i++) {
        data_entry* value = (*vde0)[i];
        resultcode = tairClient.sadd(TairClientRDBTest::default_namespace, *key,
                *value, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    }

    VDE* ret_values = new VDE();
    resultcode = tairClient.smembers(TairClientRDBTest::default_namespace, *key,
            *ret_values);
    ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    if (set_data_entry_cmp(vde0, ret_values)) {
        ASSERT_TRUE(1);
    } else {
        ASSERT_TRUE(0);
    }

    vector_data_entry_free(ret_values);

    resultcode = tairClient.srem(TairClientRDBTest::default_namespace, *key,
            *((*vde0)[0]), NOT_CARE_EXPIRE, 99);
    ASSERT_EQ(resultcode, TAIR_RETURN_VERSION_ERROR);
    resultcode = tairClient.srem(TairClientRDBTest::default_namespace, *key,
            *((*vde0)[0]), NOT_CARE_EXPIRE, 101);
    ASSERT_EQ(resultcode, TAIR_RETURN_VERSION_ERROR);
    resultcode = tairClient.srem(TairClientRDBTest::default_namespace, *key,
            *((*vde0)[0]), NOT_CARE_EXPIRE, 0);
    ASSERT_EQ(resultcode , TAIR_RETURN_SUCCESS);
    resultcode = tairClient.srem(TairClientRDBTest::default_namespace, *key,
            *((*vde0)[1]), NOT_CARE_EXPIRE, 101);
    ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);

    VDE* ret_values1 = new VDE();
    resultcode = tairClient.smembers(TairClientRDBTest::default_namespace, *key,
            *ret_values1);
    ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(98, ret_values1->size());

    vector_data_entry_free(ret_values1);
    
    data_entry_free(key, 1);
    vector_data_entry_free(vde0, 1);
    vector_data_entry_free(vde1, 1);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
