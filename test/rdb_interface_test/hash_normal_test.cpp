#include "base_test.hpp"

//this file test hash's all interface common test

TEST_F(TairClientRDBTest, test_hash_normal) {
    int resultcode;
    //hset //hget
    MMDE* mmde = mmap_data_entry_rand(16, 16, 16, 10, 10);
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE* mde = iter->second;
            MDE::iterator iter2;
            for(iter2 = mde->begin(); iter2 != mde->end(); iter2++) {
                data_entry* field = iter2->first;
                data_entry* value = iter2->second;
                resultcode = tairClient.hset(i, *key, *field, *value,
                        NOT_CARE_EXPIRE, NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);

                data_entry* ret_val = new data_entry();
                resultcode = tairClient.hget(i, *key, *field, *ret_val);
                ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
                if (data_entry_cmp(value, ret_val)) {
                    ASSERT_TRUE(1);
                } else {
                    ASSERT_TRUE(0);
                }

                data_entry_free(ret_val);
            }

            MDE* ret_field_values = new MDE();
            resultcode = tairClient.hgetall(i, *key, *ret_field_values);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
            if (map_data_entry_cmp(ret_field_values, mde)) {
                ASSERT_TRUE(1);
            } else {
                ASSERT_TRUE(0);
            }

            map_data_entry_free(ret_field_values);
        }
    }

    //hdel
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE* mde = iter->second;
            MDE::iterator iter2;
            for(iter2 = mde->begin(); iter2 != mde->end(); iter2++) {
                data_entry* field = iter2->first;
                
                resultcode = tairClient.hdel(i, *key, *field,
                        NOT_CARE_EXPIRE, NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
                
                resultcode = tairClient.hdel(i, *key, *field,
                        NOT_CARE_EXPIRE, NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
            }
        }
    }

    //delete
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            
            resultcode = tairClient.remove(i, *key);
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
        }
    }

    //hmset  //hmget
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE* fields = iter->second;
            
            MDE* part_mde = map_get_n_data_entry(fields, 3);
            VDE* part_keys = vector_key_from_map(part_mde);
            VDE* part_values = vector_value_from_map(part_mde);
            
            MDE* ret_field_values = new MDE();
            int resultcode = tairClient.hmset(i, *key, *part_mde, *ret_field_values,
                    NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
            map_data_entry_free(ret_field_values);

            VDE* ret_part_value = new VDE();
            
            resultcode = tairClient.hmget(i, *key, *part_keys, *ret_part_value);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);            

            if (vector_data_entry_cmp(ret_part_value, part_values)) {
                ASSERT_TRUE(1);
            } else {
                ASSERT_TRUE(0);
            }

            part_mde->clear();
            delete part_mde;

            part_keys->clear();
            delete part_keys;

            part_values->clear();
            delete part_values;
            
            vector_data_entry_free(ret_part_value);
        }
    }

    //delete
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            
            resultcode = tairClient.remove(i, *key);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        }
    }
    
    mmap_data_entry_free(mmde, 1);
}

TEST_F(TairClientRDBTest, test_hash_key_null) {
    int resultcode;

    MMDE* mmde = mmap_data_entry_rand(0, 128, 128, TairClientRDBTest::max_namespace, 10);
    
    MMDE::iterator iter;
    for(iter = mmde->begin(); iter != mmde->end(); iter++) {
        data_entry* key = iter->first;
        MDE* mde = iter->second;
        
        MDE::iterator iter;
        for(iter = mde->begin(); iter != mde->end(); iter++) {
            data_entry* field = iter->first;
            data_entry* value = iter->second;

            resultcode = tairClient.hset(TairClientRDBTest::default_namespace, *key,
                    *field, *value, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);

            data_entry* ret_val = new data_entry();
            resultcode = tairClient.hget(TairClientRDBTest::default_namespace, *key,
                    *field, *ret_val);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            data_entry_free(ret_val);
        }

        MDE* ret_field_values = new MDE();
        resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, *key,
                    *ret_field_values);
        ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
        map_data_entry_free(ret_field_values);
    }

    for(iter = mmde->begin(); iter != mmde->end(); iter++) {
        data_entry* key = iter->first;
        MDE* mde = iter->second;

        MDE::iterator iter;
        for(iter = mde->begin(); iter != mde->end(); iter++) {
            data_entry* field = iter->first;

            resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, *key,
                    *field, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
        }
    }


    //hmset  //hmget
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE* mde = iter->second;
            MDE* part_mde = map_get_n_data_entry(mde, 3);
            VDE* part_keys = vector_key_from_map(part_mde);
            VDE* part_values = vector_value_from_map(part_mde);

            MDE* ret_field_value = new MDE();
            int resultcode = tairClient.hmset(i, *key, *part_mde, *ret_field_value,
                    NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            map_data_entry_free(ret_field_value);

            VDE* ret_part_value = new VDE();
            resultcode = tairClient.hmget(i, *key, *part_keys, *ret_part_value);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);            

            part_mde->clear();
            delete part_mde;

            part_keys->clear();
            delete part_keys;

            part_values->clear();
            delete part_values;

            vector_data_entry_free(ret_part_value);
        }
    }

    //delete
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            
            resultcode = tairClient.remove(i, *key);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
        }
    }
    
    mmap_data_entry_free(mmde, 1);
}

TEST_F(TairClientRDBTest, test_hash_key_1024) {
    int resultcode;

    MMDE* mmde = mmap_data_entry_rand(1024, 128, 128, TairClientRDBTest::max_namespace, 10);
    
    MMDE::iterator iter;
    for(iter = mmde->begin(); iter != mmde->end(); iter++) {
        data_entry* key = iter->first;
        MDE* mde = iter->second;
        
        MDE::iterator iter;
        for(iter = mde->begin(); iter != mde->end(); iter++) {
            data_entry* field = iter->first;
            data_entry* value = iter->second;

            resultcode = tairClient.hset(TairClientRDBTest::default_namespace, *key,
                    *field, *value, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);

            data_entry* ret_val = new data_entry();
            resultcode = tairClient.hget(TairClientRDBTest::default_namespace, *key,
                    *field, *ret_val);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            data_entry_free(ret_val);
        }

        MDE* ret_field_values = new MDE();
        resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, *key,
                    *ret_field_values);
        ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
        map_data_entry_free(ret_field_values);
    }

    for(iter = mmde->begin(); iter != mmde->end(); iter++) {
        data_entry* key = iter->first;
        MDE* mde = iter->second;

        MDE::iterator iter;
        for(iter = mde->begin(); iter != mde->end(); iter++) {
            data_entry* field = iter->first;

            resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, *key,
                    *field, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
        }
    }


    //hmset  //hmget
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE* mde = iter->second;
            MDE* part_mde = map_get_n_data_entry(mde, 3);
            VDE* part_keys = vector_key_from_map(part_mde);
            VDE* part_values = vector_value_from_map(part_mde);
            MDE* ret_field_values = new MDE();
            int resultcode = tairClient.hmset(i, *key, *part_mde, *ret_field_values,
                    NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            map_data_entry_free(ret_field_values);

            VDE* ret_part_value = new VDE();
            resultcode = tairClient.hmget(i, *key, *part_keys, *ret_part_value);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);            

            part_mde->clear();
            delete part_mde;

            part_keys->clear();
            delete part_keys;

            part_values->clear();
            delete part_values;

            vector_data_entry_free(ret_part_value);
        }
    }

    //delete
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            
            resultcode = tairClient.remove(i, *key);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
        }
    }
    
    mmap_data_entry_free(mmde, 1);
}


TEST_F(TairClientRDBTest, test_hash_field_null) {
    int resultcode;

    MMDE* mmde = mmap_data_entry_rand(16, 0, 128, TairClientRDBTest::max_namespace, 10);
    
    MMDE::iterator iter;
    for(iter = mmde->begin(); iter != mmde->end(); iter++) {
        data_entry* key = iter->first;
        MDE* mde = iter->second;
        
        MDE::iterator iter;
        for(iter = mde->begin(); iter != mde->end(); iter++) {
            data_entry* field = iter->first;
            data_entry* value = iter->second;

            resultcode = tairClient.hset(TairClientRDBTest::default_namespace, *key,
                    *field, *value, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);

            data_entry* ret_val = new data_entry();
            resultcode = tairClient.hget(TairClientRDBTest::default_namespace, *key,
                    *field, *ret_val);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            data_entry_free(ret_val);
        }

        MDE* ret_field_values = new MDE();
        resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, *key,
                    *ret_field_values);
        ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
        map_data_entry_free(ret_field_values);
    }

    for(iter = mmde->begin(); iter != mmde->end(); iter++) {
        data_entry* key = iter->first;
        MDE* mde = iter->second;

        MDE::iterator iter;
        for(iter = mde->begin(); iter != mde->end(); iter++) {
            data_entry* field = iter->first;

            resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, *key,
                    *field, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
        }
    }


    //hmset  //hmget
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE* mde = iter->second;
            MDE* part_mde = map_get_n_data_entry(mde, 3);
            VDE* part_keys = vector_key_from_map(part_mde);
            VDE* part_values = vector_value_from_map(part_mde);

            MDE* ret_field_values = new MDE();
            int resultcode = tairClient.hmset(i, *key, *part_mde, *ret_field_values,
                    NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            map_data_entry_free(ret_field_values);

            VDE* ret_part_value = new VDE();
            resultcode = tairClient.hmget(i, *key, *part_keys, *ret_part_value);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);            

            part_mde->clear();
            delete part_mde;

            part_keys->clear();
            delete part_keys;

            part_values->clear();
            delete part_values;

            vector_data_entry_free(ret_part_value);
        }
    }

    //delete
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            
            resultcode = tairClient.remove(i, *key);
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
        }
    }
    
    mmap_data_entry_free(mmde, 1);
}

TEST_F(TairClientRDBTest, test_hash_field_1024x1024) {
    int resultcode;

    MMDE* mmde = mmap_data_entry_rand(16, 1024*1024, 128, TairClientRDBTest::max_namespace, 10);
    
    MMDE::iterator iter;
    for(iter = mmde->begin(); iter != mmde->end(); iter++) {
        data_entry* key = iter->first;
        MDE* mde = iter->second;
        
        MDE::iterator iter;
        for(iter = mde->begin(); iter != mde->end(); iter++) {
            data_entry* field = iter->first;
            data_entry* value = iter->second;

            resultcode = tairClient.hset(TairClientRDBTest::default_namespace, *key,
                    *field, *value, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);

            data_entry* ret_val = new data_entry();
            resultcode = tairClient.hget(TairClientRDBTest::default_namespace, *key,
                    *field, *ret_val);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            data_entry_free(ret_val);
        }

        MDE* ret_field_values = new MDE();
        resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, *key,
                    *ret_field_values);
        ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
        map_data_entry_free(ret_field_values);
    }

    for(iter = mmde->begin(); iter != mmde->end(); iter++) {
        data_entry* key = iter->first;
        MDE* mde = iter->second;

        MDE::iterator iter;
        for(iter = mde->begin(); iter != mde->end(); iter++) {
            data_entry* field = iter->first;

            resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, *key,
                    *field, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
        }
    }


    //hmset  //hmget
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE* mde = iter->second;
            MDE* part_mde = map_get_n_data_entry(mde, 3);
            VDE* part_keys = vector_key_from_map(part_mde);
            VDE* part_values = vector_value_from_map(part_mde);
            
            MDE* ret_field_values = new MDE();
            int resultcode = tairClient.hmset(i, *key, *part_mde, *ret_field_values,
                    NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            map_data_entry_free(ret_field_values);

            VDE* ret_part_value = new VDE();
            resultcode = tairClient.hmget(i, *key, *part_keys, *ret_part_value);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);            

            part_mde->clear();
            delete part_mde;

            part_keys->clear();
            delete part_keys;

            part_values->clear();
            delete part_values;

            vector_data_entry_free(ret_part_value);
        }
    }

    //delete
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            
            resultcode = tairClient.remove(i, *key);
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
        }
    }
    
    mmap_data_entry_free(mmde, 1);
}


//
TEST_F(TairClientRDBTest, test_hash_value_null) {
    int resultcode;

    MMDE* mmde = mmap_data_entry_rand(16, 128, 0, TairClientRDBTest::max_namespace, 10);
    
    MMDE::iterator iter;
    for(iter = mmde->begin(); iter != mmde->end(); iter++) {
        data_entry* key = iter->first;
        MDE* mde = iter->second;
        
        MDE::iterator iter;
        for(iter = mde->begin(); iter != mde->end(); iter++) {
            data_entry* field = iter->first;
            data_entry* value = iter->second;

            resultcode = tairClient.hset(TairClientRDBTest::default_namespace, *key,
                    *field, *value, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);

            data_entry* ret_val = new data_entry();
            resultcode = tairClient.hget(TairClientRDBTest::default_namespace, *key,
                    *field, *ret_val);
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
            data_entry_free(ret_val);
        }

        MDE* ret_field_values = new MDE();
        resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, *key,
                    *ret_field_values);
        ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
        map_data_entry_free(ret_field_values);
    }

    for(iter = mmde->begin(); iter != mmde->end(); iter++) {
        data_entry* key = iter->first;
        MDE* mde = iter->second;

        MDE::iterator iter;
        for(iter = mde->begin(); iter != mde->end(); iter++) {
            data_entry* field = iter->first;

            resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, *key,
                    *field, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
        }
    }


    //hmset  //hmget
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE* mde = iter->second;
            MDE* part_mde = map_get_n_data_entry(mde, 3);
            VDE* part_keys = vector_key_from_map(part_mde);
            VDE* part_values = vector_value_from_map(part_mde);

            MDE* ret_field_value = new MDE();
            int resultcode = tairClient.hmset(i, *key, *part_mde, *ret_field_value,
                    NOT_CARE_EXPIRE, NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            map_data_entry_free(ret_field_value);

            VDE* ret_part_value = new VDE();
            resultcode = tairClient.hmget(i, *key, *part_keys, *ret_part_value);
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);            
            
            part_mde->clear();
            delete part_mde;

            part_keys->clear();
            delete part_keys;

            part_values->clear();
            delete part_values;

            vector_data_entry_free(ret_part_value);
        }
    }

    //delete
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            
            resultcode = tairClient.remove(i, *key);
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
        }
    }
    
    mmap_data_entry_free(mmde, 1);
}

TEST_F(TairClientRDBTest, test_hash_value_1024x1024) {
    int resultcode;

    MMDE* mmde = mmap_data_entry_rand(16, 128, 1024*1024, TairClientRDBTest::max_namespace, 10);
    
    MMDE::iterator iter;
    for(iter = mmde->begin(); iter != mmde->end(); iter++) {
        data_entry* key = iter->first;
        MDE* mde = iter->second;
        
        MDE::iterator iter;
        for(iter = mde->begin(); iter != mde->end(); iter++) {
            data_entry* field = iter->first;
            data_entry* value = iter->second;

            resultcode = tairClient.hset(TairClientRDBTest::default_namespace, *key,
                    *field, *value, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);

            data_entry* ret_val = new data_entry();
            resultcode = tairClient.hget(TairClientRDBTest::default_namespace, *key,
                    *field, *ret_val);
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
            data_entry_free(ret_val);
        }

        MDE* ret_field_values = new MDE();
        resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, *key,
                    *ret_field_values);
        ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
        map_data_entry_free(ret_field_values);
    }

    for(iter = mmde->begin(); iter != mmde->end(); iter++) {
        data_entry* key = iter->first;
        MDE* mde = iter->second;

        MDE::iterator iter;
        for(iter = mde->begin(); iter != mde->end(); iter++) {
            data_entry* field = iter->first;

            resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, *key,
                    *field, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
        }
    }


    //hmset  //hmget
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            MDE* mde = iter->second;
            MDE* part_mde = map_get_n_data_entry(mde, 3);
            VDE* part_keys = vector_key_from_map(part_mde);
            VDE* part_values = vector_value_from_map(part_mde);

            MDE* ret_field_values = new MDE();
            int resultcode = tairClient.hmset(i, *key, *part_mde, *ret_field_values,
                    NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            map_data_entry_free(ret_field_values);

            VDE* ret_part_value = new VDE();
            resultcode = tairClient.hmget(i, *key, *part_keys, *ret_part_value);
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);            

            part_mde->clear();
            delete part_mde;

            part_keys->clear();
            delete part_keys;

            part_values->clear();
            delete part_values;

            vector_data_entry_free(ret_part_value);
        }
    }

    //delete
    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MMDE::iterator iter;
        for(iter = mmde->begin(); iter != mmde->end(); iter++) {
            data_entry* key = iter->first;
            
            resultcode = tairClient.remove(i, *key);
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
        }
    }
    
    mmap_data_entry_free(mmde, 1);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

