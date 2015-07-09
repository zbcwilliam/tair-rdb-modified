#include "base_test.hpp"

TEST_F(TairClientRDBTest, test_set_normal) {
    int resultcode;

    MVDE* mvde = mapv_data_entry_rand(64, 64, 10, 10);

    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MVDE::iterator iter;
        for(iter = mvde->begin(); iter != mvde->end(); iter++) {
            data_entry* key = iter->first;
            VDE* vde = iter->second;

            for(size_t j = 0; j < vde->size(); j++) {
                resultcode = tairClient.sadd(i, *key, *((*vde)[j]), NOT_CARE_EXPIRE,
                    NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);            
            }

            VDE* ret_values = new VDE();
            resultcode = tairClient.smembers(i, *key, *ret_values);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
            if (set_data_entry_cmp(vde, ret_values)) {
                ASSERT_TRUE(1);
            } else {
                ASSERT_TRUE(0);
            }

            vector_data_entry_free(ret_values);
        }
    }

    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MVDE::iterator iter;
        for(iter = mvde->begin(); iter != mvde->end(); iter++) {
            data_entry* key = iter->first;
            VDE* vde = iter->second;

            for(size_t j = 0; j < vde->size(); j++) {
                resultcode = tairClient.srem(i, *key, *((*vde)[j]), NOT_CARE_EXPIRE,
                        NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);

                resultcode = tairClient.srem(i, *key, *((*vde)[j]), NOT_CARE_EXPIRE,
                        NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
            }
        }
    }

    mapv_data_entry_free(mvde, 1);
}

TEST_F(TairClientRDBTest, test_set_key_null) {
    int resultcode;

    MVDE* mvde = mapv_data_entry_rand(0, 128, 10, 10);

    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MVDE::iterator iter;
        for(iter = mvde->begin(); iter != mvde->end(); iter++) {
            data_entry* key = iter->first;
            VDE* vde = iter->second;

            for(size_t j = 0; j < vde->size(); j++) {
                resultcode = tairClient.sadd(i, *key, *((*vde)[j]), NOT_CARE_EXPIRE,
                    NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);            
            }

            VDE* ret_values = new VDE();
            resultcode = tairClient.smembers(i, *key, *ret_values);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            vector_data_entry_free(ret_values);
        }
    }

    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MVDE::iterator iter;
        for(iter = mvde->begin(); iter != mvde->end(); iter++) {
            data_entry* key = iter->first;
            VDE* vde = iter->second;

            for(size_t j = 0; j < vde->size(); j++) {
                resultcode = tairClient.srem(i, *key, *((*vde)[j]), NOT_CARE_EXPIRE,
                        NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);

                resultcode = tairClient.srem(i, *key, *((*vde)[j]), NOT_CARE_EXPIRE,
                        NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            }
        }
    }

    mapv_data_entry_free(mvde, 1);

}

TEST_F(TairClientRDBTest, test_set_key_1024) {
    int resultcode;

    MVDE* mvde = mapv_data_entry_rand(1024, 128, 10, 10);

    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MVDE::iterator iter;
        for(iter = mvde->begin(); iter != mvde->end(); iter++) {
            data_entry* key = iter->first;
            VDE* vde = iter->second;

            for(size_t j = 0; j < vde->size(); j++) {
                resultcode = tairClient.sadd(i, *key, *((*vde)[j]), NOT_CARE_EXPIRE,
                    NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);            
            }

            VDE* ret_values = new VDE();
            resultcode = tairClient.smembers(i, *key, *ret_values);
            ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            vector_data_entry_free(ret_values);
        }
    }

    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MVDE::iterator iter;
        for(iter = mvde->begin(); iter != mvde->end(); iter++) {
            data_entry* key = iter->first;
            VDE* vde = iter->second;

            for(size_t j = 0; j < vde->size(); j++) {
                resultcode = tairClient.srem(i, *key, *((*vde)[j]), NOT_CARE_EXPIRE,
                        NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);

                resultcode = tairClient.srem(i, *key, *((*vde)[j]), NOT_CARE_EXPIRE,
                        NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            }
        }
    }

    mapv_data_entry_free(mvde, 1);

}


TEST_F(TairClientRDBTest, test_set_value_null) {
    int resultcode;

    MVDE* mvde = mapv_data_entry_rand(128, 0, 10, 10);

    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MVDE::iterator iter;
        for(iter = mvde->begin(); iter != mvde->end(); iter++) {
            data_entry* key = iter->first;
            VDE* vde = iter->second;

            for(size_t j = 0; j < vde->size(); j++) {
                resultcode = tairClient.sadd(i, *key, *((*vde)[j]), NOT_CARE_EXPIRE,
                    NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);            
            }

            VDE* ret_values = new VDE();
            resultcode = tairClient.smembers(i, *key, *ret_values);
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
            vector_data_entry_free(ret_values);
        }
    }

    for(int i = 0; i < TairClientRDBTest::max_namespace; i++) {
        MVDE::iterator iter;
        for(iter = mvde->begin(); iter != mvde->end(); iter++) {
            data_entry* key = iter->first;
            VDE* vde = iter->second;

            for(size_t j = 0; j < vde->size(); j++) {
                resultcode = tairClient.srem(i, *key, *((*vde)[j]), NOT_CARE_EXPIRE,
                        NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);

                resultcode = tairClient.srem(i, *key, *((*vde)[j]), NOT_CARE_EXPIRE,
                        NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            }
        }
    }

    mapv_data_entry_free(mvde, 1);

}

TEST_F(TairClientRDBTest, test_set_value_1024x1024) {
    int resultcode;

    MVDE* mvde = mapv_data_entry_rand(128, 1024*1024, 1, 10);

    for(int i = 0; i < 1; i++) {
        MVDE::iterator iter;
        for(iter = mvde->begin(); iter != mvde->end(); iter++) {
            data_entry* key = iter->first;
            VDE* vde = iter->second;

            for(size_t j = 0; j < vde->size(); j++) {
                resultcode = tairClient.sadd(i, *key, *((*vde)[j]), NOT_CARE_EXPIRE,
                    NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);            
            }

            VDE* ret_values = new VDE();
            resultcode = tairClient.smembers(i, *key, *ret_values);
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
            vector_data_entry_free(ret_values);
        }
    }

    for(int i = 0; i < 1; i++) {
        MVDE::iterator iter;
        for(iter = mvde->begin(); iter != mvde->end(); iter++) {
            data_entry* key = iter->first;
            VDE* vde = iter->second;

            for(size_t j = 0; j < vde->size(); j++) {
                resultcode = tairClient.srem(i, *key, *((*vde)[j]), NOT_CARE_EXPIRE,
                        NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);

                resultcode = tairClient.srem(i, *key, *((*vde)[j]), NOT_CARE_EXPIRE,
                        NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_ITEMSIZE_ERROR);
            }
        }
    }

    mapv_data_entry_free(mvde, 1);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

