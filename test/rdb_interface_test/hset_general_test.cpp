#include "base_test.hpp"

//int hset(int area, data_entry &key, data_entry &field, data_entry &value, int expire, int version);

TEST_F(TairClientRDBTest, test_hset_expire_timestamp) {
    //set expire with timestamp
    int resultcode;
    MMDE* mmde = mmap_data_entry_rand(128, 128, 128, 1, 10);

    MMDE::iterator iter;
    for(iter = mmde->begin(); iter != mmde->end(); iter++) {
        data_entry* key = iter->first;
        MDE* mde = iter->second;
        
        MDE::iterator iter2;
        for(iter2 = mde->begin(); iter2 != mde->end(); iter2++) {
            data_entry* field = iter2->first;
            data_entry* value = iter2->second;

            resultcode = tairClient.hset(TairClientRDBTest::default_namespace, *key, *field,
                    *value, time(NULL) + 5, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        }

        sleep(2);
    
        MDE* ret_field_values = new MDE();
        resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, *key, *ret_field_values);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        if (map_data_entry_cmp(mde, ret_field_values)) {
            ASSERT_TRUE(1);
        } else {
            ASSERT_TRUE(0);
        }

        map_data_entry_free(ret_field_values);

        sleep(4);

        resultcode = tairClient.remove(TairClientRDBTest::default_namespace, *key);
        ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
    }

    mmap_data_entry_free(mmde, 1);
}

TEST_F(TairClientRDBTest, test_hset_expire_timeinterval) {
    //set expire with timestamp
    int resultcode;
    MMDE* mmde = mmap_data_entry_rand(128, 128, 128, 1, 10);

    MMDE::iterator iter;
    for(iter = mmde->begin(); iter != mmde->end(); iter++) {
        data_entry* key = iter->first;
        MDE* mde = iter->second;
        
        MDE::iterator iter2;
        for(iter2 = mde->begin(); iter2 != mde->end(); iter2++) {
            data_entry* field = iter2->first;
            data_entry* value = iter2->second;

            resultcode = tairClient.hset(TairClientRDBTest::default_namespace, *key, *field,
                    *value, 5, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        }

        sleep(2);
    
        MDE* ret_field_values = new MDE();
        resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, *key, *ret_field_values);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        if (map_data_entry_cmp(mde, ret_field_values)) {
            ASSERT_TRUE(1);
        } else {
            ASSERT_TRUE(0);
        }

        map_data_entry_free(ret_field_values);

        sleep(4);

        resultcode = tairClient.remove(TairClientRDBTest::default_namespace, *key);
        ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
    }

    mmap_data_entry_free(mmde, 1);
}

TEST_F(TairClientRDBTest, test_hset_expire_cancel) {
    int resultcode;

    data_entry* key = data_entry_rand(128);
    MDE* mde1 = map_data_entry_rand(128, 128, 10);
    MDE* mde2 = map_data_entry_rand(128, 128, 10);

    MDE::iterator iter1;
    for(iter1 = mde1->begin(); iter1 != mde1->end(); iter1++) {
        data_entry* field = iter1->first;
        data_entry* value = iter1->second;
        resultcode = tairClient.hset(TairClientRDBTest::default_namespace, *key,
                *field, *value, 5, NOT_CARE_VERSION);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    }

    sleep(2);

    MDE* ret_field_values = new MDE();
    resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, *key,
            *ret_field_values);
    if (map_data_entry_cmp(mde1, ret_field_values)) {
        ASSERT_TRUE(1);
    } else {
        ASSERT_TRUE(0);
    }
    map_data_entry_free(ret_field_values);

    MDE::iterator iter2;
    for(iter2 = mde2->begin(); iter2 != mde2->end(); iter2++) {
        data_entry* field = iter2->first;
        data_entry* value = iter2->second;
        resultcode = tairClient.hset(TairClientRDBTest::default_namespace, *key,
                *field, *value, CANCEL_EXPIRE, NOT_CARE_VERSION);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    }
        
    sleep(4);

    resultcode = tairClient.remove(TairClientRDBTest::default_namespace, *key);
    ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);

    data_entry_free(key, 1);
    map_data_entry_free(mde1, 1);
    map_data_entry_free(mde2, 1);
}

TEST_F(TairClientRDBTest, test_hset_version_care) {
    int resultcode;

    data_entry* key = data_entry_rand(128);
    data_entry* field = data_entry_rand(128);
    data_entry* value = data_entry_rand(128);
    MDE* mde0 = map_data_entry_rand(128, 128, 100);
    MDE* mde1 = map_data_entry_rand(128, 128, 100);

    int index = 0;
    MDE::iterator iter0;
    for(iter0 = mde0->begin(); iter0 != mde0->end(); iter0++) {
        data_entry* tfield = iter0->first;
        data_entry* tvalue = iter0->second;

        resultcode = tairClient.hset(TairClientRDBTest::default_namespace, *key,
                *tfield, *tvalue, NOT_CARE_EXPIRE, index++);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    }

    resultcode = tairClient.hset(TairClientRDBTest::default_namespace, *key,
           *field, *value, NOT_CARE_EXPIRE, 99);
    ASSERT_EQ(resultcode, TAIR_RETURN_VERSION_ERROR); 

    resultcode = tairClient.hset(TairClientRDBTest::default_namespace, *key,
            *field, *value, NOT_CARE_EXPIRE, 101);
    ASSERT_EQ(resultcode, TAIR_RETURN_VERSION_ERROR);

    resultcode = tairClient.hset(TairClientRDBTest::default_namespace, *key,
            *field, *value, NOT_CARE_EXPIRE, 0);
    ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);

    index++;

    MDE::iterator iter1;
    for(iter1 = mde1->begin(); iter1 != mde1->end(); iter1++) {
        data_entry* tfield = iter1->first;
        data_entry* tvalue = iter1->second;

        resultcode = tairClient.hset(TairClientRDBTest::default_namespace, *key,
                *tfield, *tvalue, NOT_CARE_EXPIRE, index++);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    }

    resultcode = tairClient.remove(TairClientRDBTest::default_namespace, *key);
    ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);

    data_entry_free(key, 1);
    data_entry_free(field, 1);
    data_entry_free(value, 1);

    map_data_entry_free(mde0, 1);
    map_data_entry_free(mde1, 1);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
