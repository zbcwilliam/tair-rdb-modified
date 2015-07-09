#include "base_test.hpp"

//int hmset(int area, data_entry &key, map<data_entry*, data_entry*> &field_values,
//          map<data_entry*, data_entry*> &key_values_success,
//          int expire, int version);
TEST_F(TairClientRDBTest, test_hmset_expire_timestamp) {
    MMDE* mmde = mmap_data_entry_rand(128, 128, 128, 1, 10);
    {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE* mde = iter->second;
            MDE retvalue;
            int resultcode = tairClient.hmset(TairClientRDBTest::default_namespace,
                    G(key), G(mde), retvalue,
                    time(NULL) + 5, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        }
    }
    sleep(2);
    {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE *orgin_field_values = iter->second;
            MDE* field_values = new MDE();
            int resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, G(key), G(field_values));
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
            ASSERT_TRUE(map_data_entry_cmp(orgin_field_values, field_values));
            map_data_entry_free(field_values);
        }
    }
    sleep(4);
    {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            int resultcode = tairClient.remove(TairClientRDBTest::default_namespace, G(key));
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
        }

    }
    mmap_data_entry_free(mmde, 1);
}

TEST_F(TairClientRDBTest, test_hmset_expire_timeintval) {
    //expire with intval
    MMDE* mmde = mmap_data_entry_rand(128, 128, 128, 1, 10);
    {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE* field_values = iter->second;
            MDE retvalue;
            int resultcode = tairClient.hmset(TairClientRDBTest::default_namespace,
                    G(key), G(field_values), retvalue,
                    5, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        }
    }
    sleep(2);
    {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE *orgin_field_values = iter->second;
            MDE* field_values = new MDE();
            int resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, G(key), G(field_values));
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
            ASSERT_TRUE(map_data_entry_cmp(orgin_field_values, field_values));
            map_data_entry_free(field_values);
        }
    }
    sleep(4);
    {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            int resultcode = tairClient.remove(TairClientRDBTest::default_namespace, G(key));
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
        }

    }
    mmap_data_entry_free(mmde, 1);
}


TEST_F(TairClientRDBTest, test_hmset_expire_cancel) {
    MMDE* mmde = mmap_data_entry_rand(128, 128, 128, 1, 10);
    {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE* field_values = iter->second;
            MDE retvalue;
            int resultcode = tairClient.hmset(TairClientRDBTest::default_namespace,
                    G(key), G(field_values), retvalue,
                    5, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        }
    }
    sleep(2);
    {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE *orgin_field_values = iter->second;
            MDE* field_values = new MDE();
            int resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, G(key), G(field_values));
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
            ASSERT_TRUE(map_data_entry_cmp(orgin_field_values, field_values));
            map_data_entry_free(field_values);
        }
    }

    {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE *tmp_f_v = map_data_entry_rand(12, 12, 5);
            MDE retvalue;
            int resultcode = tairClient.hmset(TairClientRDBTest::default_namespace,
                    G(key), G(tmp_f_v), retvalue, CANCEL_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
            map_data_entry_free(tmp_f_v, 1);
        }
    }

    sleep(4);
    {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            int resultcode = tairClient.remove(TairClientRDBTest::default_namespace, G(key));
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        }

    }
    mmap_data_entry_free(mmde, 1);
}

TEST_F(TairClientRDBTest, test_hmset_version) {
    {
        //version
        int index;
        data_entry* key = data_entry_rand(128);
        {
            for(index = 0; index < 100; index++) {
                MDE retvalue;
                MDE* mde = map_data_entry_rand(128, 128, 2);
                int resultcode = tairClient.hmset(TairClientRDBTest::default_namespace,
                        G(key), G(mde), retvalue,
                        NOT_CARE_EXPIRE, index);
                ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
                map_data_entry_free(mde, 1);
            }
        }
        {
            MDE retvalue;
            MDE* mde = map_data_entry_rand(128,128, 2);
            int resultcode = tairClient.hmset(TairClientRDBTest::default_namespace,
                    G(key), G(mde), retvalue,
                    NOT_CARE_EXPIRE, 99);
            ASSERT_EQ(resultcode, TAIR_RETURN_VERSION_ERROR);
            map_data_entry_free(mde, 1);
        }
        {
            MDE retvalue;
            MDE* mde = map_data_entry_rand(128,128, 2);
            int resultcode = tairClient.hmset(TairClientRDBTest::default_namespace,
                    G(key), G(mde), retvalue,
                    NOT_CARE_EXPIRE, 101);
            ASSERT_EQ(resultcode, TAIR_RETURN_VERSION_ERROR);
            map_data_entry_free(mde, 1);
        }
        {
            MDE retvalue;
            MDE* mde = map_data_entry_rand(128,128, 2);
            int resultcode = tairClient.hmset(TairClientRDBTest::default_namespace,
                    G(key), G(mde), retvalue,
                    NOT_CARE_EXPIRE, 100);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
            map_data_entry_free(mde, 1);
        }
        {
            int resultcode = tairClient.remove(TairClientRDBTest::default_namespace, G(key));
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        }
        data_entry_free(key, 1);
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
