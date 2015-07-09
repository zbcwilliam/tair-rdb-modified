#include "test_helper.hpp"

#define range(start, end)   for(int i = (start); i < (end); i++)

int staticData(char** buffer, int size) {
    if(size == 0) {
        *buffer = NULL;
        return 1;
    }

    *buffer = (char*)malloc(sizeof(char) * size);
    if ((*buffer) == NULL) {
        return 0;
    }

    for(int i = 0; i < size; i++) {
        (*buffer)[i] = (i % 26) + 'a';
    }

    return 1;
}

int randData(char** buffer, int size) {
    if(size == 0) {
        *buffer = NULL;
        return 1;
    }

    struct timeval tv;
    gettimeofday(&tv, NULL);

    srand(tv.tv_usec);
    *buffer = (char*)malloc(sizeof(char) * size);
    if ((*buffer) == NULL) {
        return 0;
    }
    for(int i = 0; i < size; i++) {
        (*buffer)[i] = rand() % 256; 
    }
    return 1;
}

data_entry* data_entry_rand(int size, int isrand) {
    char* buffer = NULL;
    if (isrand) {
        if (!randData(&buffer, size)) {
            return 0;
        }
    } else {
        if (!staticData(&buffer, size)) {
            return 0;
        }
    }

    data_entry* entry = new data_entry(buffer, size, false);
    return entry;
}

VDE* vector_data_entry_rand(int len, int size, int isrand) {
    VDE* vde = new VDE();
    if (vde == NULL) {
        return NULL;
    }
    for(int i = 0; i < size; i++) {
        data_entry* entry = data_entry_rand(len, isrand);
        if (entry == NULL) {
            vector_data_entry_free(vde, 1);
            return NULL;
        }
        vde->push_back(entry);
    }

    return vde;
}

MDE* map_data_entry_rand(int k_len, int v_len, int size, int isrand) {
    MDE* mde = new map<data_entry*, data_entry*>();
    if (mde == NULL) {
        return NULL;
    }
    for(int i = 0; i < size; i++) {
        data_entry* key = data_entry_rand(k_len, isrand);
        if (key == NULL) {
            map_data_entry_free(mde, 1);
            return NULL;
        }
        data_entry* value = data_entry_rand(v_len, isrand);
        if (value == NULL) {
            map_data_entry_free(mde, 1);
            data_entry_free(key, 1);
            return NULL;
        }
        mde->insert(make_pair(key, value));
    }

    return mde;
}

MMDE* mmap_data_entry_rand(int k_len, int f_len,
        int v_len, int size, int ssize, int isrand) {
    MMDE* mmde = new MMDE();
    if (mmde == NULL) {
        return NULL;
    }

    for(int i = 0; i < size; i++) {
        data_entry* key = data_entry_rand(k_len, isrand);
        if (!key) {
            mmap_data_entry_free(mmde, 1);
            return NULL;
        }
        MDE* mde = map_data_entry_rand(f_len, v_len, ssize, isrand);
        if (!mde) {
            data_entry_free(key, 1);
            mmap_data_entry_free(mmde, 1);
            return NULL;
        }

        mmde->insert(make_pair(key, mde));
    }

    return mmde;
}

MVDE* mapv_data_entry_rand(int k_len, int f_len, int size, int ssize, int isrand) {
    MVDE* mvde = new MVDE();
    if (mvde == NULL) {
        return NULL;
    }

    for(int i = 0; i < size; i++) {
        data_entry* key = data_entry_rand(k_len, isrand);
        if (!key) {
            mapv_data_entry_free(mvde, 1);
            return NULL;
        }
        VDE* vde = vector_data_entry_rand(f_len, ssize, isrand);
        if (!vde) {
            data_entry_free(key, 1);
            mapv_data_entry_free(mvde, 1);
            return NULL;
        }

        mvde->insert(make_pair(key, vde));
    }

    return mvde;
}

void data_entry_free(data_entry* de, int isfree) {
    if (de) {
        if (isfree) {
            char* buff = de->get_data();
            free(buff);
        }
        delete de;
    }
}

void vector_data_entry_free(VDE* vde, int isfree) {
    if (!vde) {
        return;
    }

    for(size_t i = 0; i < vde->size(); i++) {
        data_entry_free((*vde)[i], isfree);
    }
    vde->clear();
    delete vde;
}

void map_data_entry_free(MDE* mde, int isfree) {
    if (!mde) {
        return;
    }

    MDE::iterator iter;
    for(iter = mde->begin(); iter != mde->end(); iter++) {
        data_entry_free(iter->first, isfree);
        data_entry_free(iter->second, isfree);
    }

    mde->clear();
    delete mde;
}

void mmap_data_entry_free(MMDE* mmde, int isfree) {
    if (!mmde) {
        return;
    }

    MMDE::iterator iter;
    for(iter = mmde->begin(); iter != mmde->end(); iter++) {
        data_entry* key = iter->first;
        MDE* mde = iter->second;
        data_entry_free(key, isfree);
        map_data_entry_free(mde, isfree);
    }

    mmde->clear();
    delete mmde;
}

void mapv_data_entry_free(MVDE* mvde, int isfree) {
    if (!mvde) {
        return;
    }

    int index = 0;
    MVDE::iterator iter;
    for(iter = mvde->begin(); iter != mvde->end(); iter++) {
        data_entry* key = iter->first;
        VDE* vde = iter->second;
        data_entry_free(key, isfree);
        vector_data_entry_free(vde, isfree);
        printf("%d\n", index++);
    }

    mvde->clear();
    delete mvde;
}

int data_entry_cmp(data_entry* de1, data_entry* de2) {
    if (de1 == de2) {
        return 1;
    }
    if (de1 == NULL || de2 == NULL) {
        return 0;
    }
    
    if (de1->get_size() != de2->get_size()) {
        return 0;
    }

    if (memcmp(de1->get_data(), de2->get_data(), de1->get_size())) {
        return 0;
    }

    return 1;
}

int vector_data_entry_cmp(VDE* vde1, VDE* vde2) {
    if (vde1 == vde2) {
        return 1;
    }

    if (vde1->size() != vde2->size()) {
        return 0;
    }

    for(size_t i = 0; i < vde1->size(); i++) {
        if (!data_entry_cmp((*vde1)[i], (*vde2)[i])) {
            return 0;
        }
    }

    return 1;
}

int set_data_entry_cmp(VDE* vde1, VDE* vde2) {
    if (vde1 == vde2) {
        return 1;
    }

    if (vde1->size() != vde2->size()) {
        return 0;
    }

    char* flag = (char*)calloc(vde1->size(), sizeof(char));
    if (flag == NULL) {
        fprintf(stderr, "file %s, line %d: error alloc\n", __FILE__, __LINE__);
        return 0;
    }

    for(size_t i = 0; i < vde1->size(); i++) {
        size_t j;
        for(j = 0; j < vde2->size(); j++) {
            if (flag[j] == 0 && data_entry_cmp((*vde1)[i], (*vde2)[j])) {
                flag[j] = 1;
                break;
            }
        }
        if (j == vde2->size()) {
            free(flag);
            return 0;
        }
    }

    free(flag);
    return 1;
}

int map_data_entry_cmp(MDE* vde1, MDE* vde2) {
    if (vde1 == vde2) {
        return 1;
    }

    if (vde1->size() != vde2->size()) {
        return 0;
    }

    MDE::iterator iter1;
    MDE::iterator iter2;
    for(iter1 = vde1->begin(); iter1 != vde1->end(); iter1++) {
        for(iter2 = vde2->begin(); iter2 != vde2->end(); iter2++) {
            if (data_entry_cmp(iter1->first, iter2->first)) {
                if (data_entry_cmp(iter1->second, iter2->second)) {
                    break;
                }
                return 0;
            }
        }
        if (iter2 == vde2->end()) {
            return 0;
        }
    }

    return 1;
}

int mmap_data_entry_cmp(MMDE* mmde1, MMDE* mmde2) {
    if (mmde1 == mmde2) {
        return 1;
    }

    if (mmde1->size() != mmde2->size()) {
        return 0;
    }

    MMDE::iterator iter1;
    MMDE::iterator iter2;
    for(iter1 = mmde1->begin(); iter1 != mmde1->end(); iter1++) {
        for(iter2 = mmde2->begin(); iter2 != mmde2->end(); iter2++) {
            if (data_entry_cmp(iter1->first, iter2->first)) {
                if (map_data_entry_cmp(iter1->second, iter2->second)) {
                    break;
                }
                return 0;
            }
        }
        if (iter2 == mmde2->end()) {
            return 0;
        }
    }

    return 1;
}

int mapv_data_entry_cmp(MVDE* mvde1, MVDE* mvde2) {
    if (mvde1 == mvde2) {
        return 1;
    }

    if (mvde1->size() != mvde2->size()) {
        return 0;
    }

    MVDE::iterator iter1;
    MVDE::iterator iter2;
    for(iter1 = mvde1->begin(); iter1 != mvde1->end(); iter1++) {
        for(iter2 = mvde2->begin(); iter2 != mvde2->end(); iter2++) {
            if (data_entry_cmp(iter1->first, iter2->first)) {
                if (vector_data_entry_cmp(iter1->second, iter2->second)) {
                    break;
                }
                return 0;
            }
        }
        if (iter2 == mvde2->end()) {
            return 0;
        }
    }

    return 1;
}


MDE* map_get_n_data_entry(MDE* mde, int n) {
    if (mde == NULL || n > (int)(mde->size())) {
        return NULL;
    }
    
    MDE* ret = new map<data_entry*, data_entry*>();

    int index = 0;
    MDE::iterator iter;
    for(iter = mde->begin(); iter != mde->end() && index < n; iter++, index++) {
        ret->insert(make_pair(iter->first, iter->second));
    }

    return ret;
}

VDE* vector_get_n_data_entry(VDE* vde, int n) {
    if (vde == NULL || n > (int)(vde->size())) {
        return NULL;
    }

    VDE* ret = new vector<data_entry* >();
    for(int index = 0; index < n; index++) {
        ret->push_back((*vde)[index]);
    }

    return vde;
}

VDE* vector_key_from_map(MDE* mde) {
    if (mde == NULL) {
        return NULL;
    }

    VDE* vde = new VDE();
    MDE::iterator iter;
    for(iter = mde->begin(); iter != mde->end(); iter++) {
        data_entry* key = iter->first;
        vde->push_back(key);
    }

    return vde;
}

VDE* vector_value_from_map(MDE* mde) {
    if (mde == NULL) {
        return NULL;
    }

    VDE* vde = new VDE();
    MDE::iterator iter;
    for(iter = mde->begin(); iter != mde->end(); iter++) {
        data_entry* value = iter->second;
        vde->push_back(value);
    }

    return vde;
}

void print_hex_string(char* buffer, int size) {
    for(int i = 0; i < size; i++) {
        printf("%2x ", buffer[i]);
    }
    printf("\n");
}
