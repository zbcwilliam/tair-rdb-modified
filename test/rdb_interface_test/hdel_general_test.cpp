#include "base_test.hpp"

//int hdel(int area, data_entry &key, data_entry &field, int expire, int version);

TEST_F(TairClientRDBTest, test_hdel_expire_timestamp) {
    //set expire with timestamp
    char* skey = NULL;
    int keysize = 16;
    randData(&skey, keysize);
    data_entry key(skey, keysize, false);

    char* sfield = NULL;
    int fieldsize = 128;

    char* svalue = NULL;
    int valuesize = 128;

    map<data_entry*, data_entry*> field_values;
    for(int i = 0; i < 100; i++) {
        randData(&svalue, valuesize);
        data_entry* value = new data_entry(svalue, valuesize, false);
        randData(&sfield, fieldsize);
        data_entry* field = new data_entry(sfield, fieldsize, false);

        field_values.insert(make_pair(field, value));
        int resultcode = tairClient.hset(TairClientRDBTest::default_namespace, key,
                *field, *value, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    }

    {
        data_entry* field_del = NULL;
        map<data_entry*, data_entry*>::iterator iter;
        for(iter = field_values.begin(); iter != field_values.end(); iter++) {
            field_del = iter->first;
            break;
        }
        int resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, key, *field_del,
                time(NULL) + 5, NOT_CARE_VERSION);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    }

    sleep(2);

    {
        map<data_entry *, data_entry *> field_values_get;
        int resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, key, field_values_get);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(99, field_values_get.size());

        map<data_entry *, data_entry *>::iterator iter;
        for(iter = field_values_get.begin(); iter != field_values_get.end(); iter++) {
            data_entry* tmp_f = iter->first;
            data_entry* tmp_v = iter->second;
            if (tmp_f) {
                delete tmp_f;
            }
            if (tmp_v) {
                delete tmp_v;
            }
        }
        field_values_get.clear();
    }

    sleep(4);

    {
        int resultcode = tairClient.remove(TairClientRDBTest::default_namespace, key);
        ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);

        if (skey) {
            free(skey);
        }

        map<data_entry *, data_entry *>::iterator iter;
        for(iter = field_values.begin(); iter != field_values.end(); iter++) {
            data_entry* tmp_f = iter->first;
            data_entry* tmp_v = iter->second;
            if (tmp_f) {
                char* buff = tmp_f->get_data();
                if (buff) {
                    free(buff);
                }
                delete tmp_f;
            }
            if (tmp_v) {
                char* buff = tmp_v->get_data();
                if (buff) {
                    free(buff);
                }
                delete tmp_v;
            }
        }
        field_values.clear();
    }
}

TEST_F(TairClientRDBTest, test_hdel_expire_interval) {
    //set expire with interval
    char* skey = NULL;
    int keysize = 16;
    randData(&skey, keysize);
    data_entry key(skey, keysize, false);

    char* sfield = NULL;
    int fieldsize = 128;

    char* svalue = NULL;
    int valuesize = 128;

    map<data_entry*, data_entry*> field_values;
    for(int i = 0; i < 100; i++) {
        randData(&svalue, valuesize);
        data_entry* value = new data_entry(svalue, valuesize, false);
        randData(&sfield, fieldsize);
        data_entry* field = new data_entry(sfield, fieldsize, false);

        field_values.insert(make_pair(field, value));
        int resultcode = tairClient.hset(TairClientRDBTest::default_namespace, key,
                *field, *value, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    }

    {
        data_entry* field_del = NULL;
        map<data_entry*, data_entry*>::iterator iter;
        for(iter = field_values.begin(); iter != field_values.end(); iter++) {
            field_del = iter->first;
            break;
        }
        int resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, key, *field_del,
                5, NOT_CARE_VERSION);
        ASSERT_EQ(resultcode,TAIR_RETURN_SUCCESS);
    }

    sleep(2);

    {
        map<data_entry *, data_entry *> field_values_get;
        int resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, key, field_values_get);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(99, field_values_get.size());

        map<data_entry *, data_entry *>::iterator iter;
        for(iter = field_values_get.begin(); iter != field_values_get.end(); iter++) {
            data_entry* tmp_f = iter->first;
            data_entry* tmp_v = iter->second;
            if (tmp_f) {
                delete tmp_f;
            }
            if (tmp_v) {
                delete tmp_v;
            }
        }
        field_values_get.clear();
    }

    sleep(4);

    {
        int resultcode = tairClient.remove(TairClientRDBTest::default_namespace, key);
        ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);

        if (skey) {
            free(skey);
        }

        map<data_entry *, data_entry *>::iterator iter;
        for(iter = field_values.begin(); iter != field_values.end(); iter++) {
            data_entry* tmp_f = iter->first;
            data_entry* tmp_v = iter->second;
            if (tmp_f) {
                char* buff = tmp_f->get_data();
                if (buff) {
                    free(buff);
                }
                delete tmp_f;
            }
            if (tmp_v) {
                char* buff = tmp_v->get_data();
                if (buff) {
                    free(buff);
                }
                delete tmp_v;
            }
        }
        field_values.clear();
    }
}

TEST_F(TairClientRDBTest, test_hdel_expire_cancel) {
    //cancel expire
    char* skey = NULL;
    int keysize = 16;
    randData(&skey, keysize);
    data_entry key(skey, keysize, false);

    char* sfield = NULL;
    int fieldsize = 128;

    char* svalue = NULL;
    int valuesize = 128;

    map<data_entry*, data_entry*> field_values;
    for(int i = 0; i < 100; i++) {
        randData(&svalue, valuesize);
        data_entry* value = new data_entry(svalue, valuesize, false);
        randData(&sfield, fieldsize);
        data_entry* field = new data_entry(sfield, fieldsize, false);

        field_values.insert(make_pair(field, value));
        int resultcode = tairClient.hset(TairClientRDBTest::default_namespace, key,
                *field, *value, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
    }

    {
        data_entry* field_del = NULL;
        map<data_entry*, data_entry*>::iterator iter;
        for(iter = field_values.begin(); iter != field_values.end(); iter++) {
            field_del = iter->first;
            break;
        }

        int resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, key, *field_del,
                5, NOT_CARE_VERSION);
        ASSERT_EQ(resultcode,TAIR_RETURN_SUCCESS);
    }

    sleep(2);

    {
        {
            map<data_entry *, data_entry *> field_values_get;
            int resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, key, field_values_get);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
            ASSERT_EQ(99, field_values_get.size());

            {
                map<data_entry *, data_entry *>::iterator iter;
                for(iter = field_values_get.begin(); iter != field_values_get.end(); iter++) {
                    data_entry *tmp_f = iter->first;
                    data_entry *tmp_v = iter->second;
                    if (tmp_f) {
                        delete tmp_f;
                    }
                    if (tmp_v) {
                        delete tmp_v;
                    }
                }
                field_values_get.clear();
            }
        }

        {

            int index = 0;
            data_entry* field_del = NULL;
            map<data_entry*, data_entry*>::iterator iter;
            for(iter = field_values.begin(); iter != field_values.end() && index < 2; iter++, index++) {
                field_del = iter->first;
            }

            int resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, key, *field_del,
                    CANCEL_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        }

        {
            map<data_entry *, data_entry *> field_values_get;
            int resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, key, field_values_get);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
            ASSERT_EQ(98, field_values_get.size());

            {
                map<data_entry *, data_entry *>::iterator iter;
                for(iter = field_values_get.begin(); iter != field_values_get.end(); iter++) {
                    data_entry *tmp_f = iter->first;
                    data_entry *tmp_v = iter->second;
                    if (tmp_f) {
                        delete tmp_f;
                    }
                    if (tmp_v) {
                        delete tmp_v;
                    }
                }
                field_values_get.clear();
            }
        }
    }

    sleep(4);

    {
        map<data_entry *, data_entry *> field_values_get;
        int resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, key, field_values_get);
        ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(98, field_values_get.size());

        {
            map<data_entry *, data_entry *>::iterator iter;
            for(iter = field_values_get.begin(); iter != field_values_get.end(); iter++) {
                data_entry *tmp_f = iter->first;
                data_entry *tmp_v = iter->second;
                if (tmp_f) {
                    delete tmp_f;
                }
                if (tmp_v) {
                    delete tmp_v;
                }
            }
            field_values_get.clear();
        }
    }

    int resultcode = tairClient.remove(TairClientRDBTest::default_namespace, key);
    ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);

    if (skey) {
        free(skey);
    }

    map<data_entry *, data_entry *>::iterator iter;
    for(iter = field_values.begin(); iter != field_values.end(); iter++) {
        data_entry *tmp_f = iter->first;
        data_entry *tmp_v = iter->second;
        if (tmp_f) {
            char* buff = tmp_f->get_data();
            if (buff) {
                free(buff);
            }
            delete tmp_f;
        }
        if (tmp_v) {
            char* buff = tmp_v->get_data();
            if (buff) {
                free(buff);
            }
            delete tmp_v;
        }
    }
    field_values.clear();
}

TEST_F(TairClientRDBTest, test_hdel_version) {
    {
        //version
        char* skey = NULL;
        int keysize = 16;
        randData(&skey, keysize);
        data_entry key(skey, keysize, false);

        int code = tairClient.remove(TairClientRDBTest::default_namespace, key);
        ASSERT_EQ(code, TAIR_RETURN_DATA_NOT_EXIST);

        map<data_entry *, data_entry *> field_values;
        for(int i = 0; i < 100; i++) {
            char* svalue = NULL;
            randData(&svalue, 128);
            data_entry* value = new data_entry(svalue, 128, false);
            char* sfield = NULL;
            randData(&sfield, 128);
            data_entry* field = new data_entry(sfield, 128, false);

            field_values.insert(make_pair(field, value));

            int resultcode = tairClient.hset(TairClientRDBTest::default_namespace, key,
                *field, *value, NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
        }

        {
            map<data_entry *, data_entry *> field_values_get;
            int resultcode = tairClient.hgetall(TairClientRDBTest::default_namespace, key, field_values_get);
            ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
            ASSERT_EQ(100, field_values_get.size());

            map<data_entry*, data_entry*>::iterator iter;
            for(iter = field_values_get.begin(); iter != field_values_get.end(); iter++) {
                data_entry* tmp_field = iter->first;
                data_entry* tmp_value = iter->second;

                if (tmp_field) {
                    delete tmp_field;
                }
                if (tmp_value) {
                    delete tmp_value;
                }
            }
            field_values_get.clear();
        }

        {

            {
                data_entry temp("zhangsan", 8, true);
                int resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, key, temp,
                        NOT_CARE_EXPIRE, NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);
            }

            map<data_entry*, data_entry*>::iterator iter;
            data_entry* field_del[2];
            {
                int index = 0;
                for(iter = field_values.begin(); iter != field_values.end() && index < 2; iter++, index ++) {
                   field_del[index] = iter->first; 
                }
                
                int resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, key, *field_del[0],
                        NOT_CARE_EXPIRE, 99);
                ASSERT_EQ(resultcode, TAIR_RETURN_VERSION_ERROR);
        
                resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, key, *field_del[0],
                        NOT_CARE_EXPIRE, 101);
                ASSERT_EQ(resultcode, TAIR_RETURN_VERSION_ERROR);

                resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, key, *field_del[0],
                        NOT_CARE_EXPIRE, 100);
                ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);

                resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, key, *field_del[1],
                        NOT_CARE_EXPIRE, NOT_CARE_VERSION);
                ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
            }

            int i = 2;
            for(; iter != field_values.end(); iter++, i++) {
                int resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, key, *(iter->first),
                        NOT_CARE_EXPIRE, 100 + i);
                ASSERT_EQ(resultcode, TAIR_RETURN_SUCCESS);
            }

            int resultcode = tairClient.hdel(TairClientRDBTest::default_namespace, key, *(field_del[0]),
                   NOT_CARE_EXPIRE, NOT_CARE_VERSION);
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);

            resultcode = tairClient.remove(TairClientRDBTest::default_namespace, key);
            ASSERT_EQ(resultcode, TAIR_RETURN_DATA_NOT_EXIST);

            if (skey) {
                free(skey);
            }

            map<data_entry*, data_entry*>::iterator iter_x;
            for(iter_x = field_values.begin(); iter_x != field_values.end(); iter_x++) {
                data_entry* tmp_field = iter_x->first;
                data_entry* tmp_value = iter_x->second;

                if (tmp_field) {
                    char* buff = tmp_field->get_data();
                    if (buff) {
                        free(buff);
                    }
                    delete tmp_field;
                }
                if (tmp_value) {
                    char* buff = tmp_value->get_data();
                    if (buff) {
                        free(buff);
                    }
                    delete tmp_value;
                }
            }
            field_values.clear();
        }
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
