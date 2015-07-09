#include "mockdb_manager.h"

using namespace tair::storage::mockdb;

int mockdb_manager::put(int bucket_number, data_entry & key, data_entry & value, 
        bool version_care, int expire) 
{
    return TAIR_RETURN_SUCCESS;
}


int mockdb_manager::remove(int bucket_number, data_entry & key, bool version_care) 
{
    return TAIR_RETURN_SUCCESS;
}

int mockdb_manager::get(int bucket_number, data_entry & key, data_entry & value)
{
    value.set_data(buf, buf_len);    
    return TAIR_RETURN_SUCCESS;
}



