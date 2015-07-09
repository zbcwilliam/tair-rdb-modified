#ifndef MOCKDB_MANAGER_H
#define MOCKDB_MANAGER_H

#include "storage/storage_manager.hpp"
#include "common/data_entry.hpp"
#include "common/stat_info.hpp"

namespace tair 
{
namespace storage 
{
namespace mockdb
{

class mockdb_manager : public tair::storage::storage_manager
{
public:
    mockdb_manager(): buf_len(800)
    {
    }
    virtual ~mockdb_manager()
    {
    }

    int put(int bucket_number, data_entry & key, data_entry & value,
              bool version_care, int expire_time);
    int get(int bucket_number, data_entry & key, data_entry & value);
    int remove(int bucket_number, data_entry & key, bool version_care);
    int clear(int area)
    {
        return 0;
    }

    bool init_buckets(const std::vector <int>&buckets)
    {
        return true;
    }
    void close_buckets(const std::vector <int>&buckets)
    {
        return ;
    }

    void begin_scan(md_info & info)
    {
    }
    void end_scan(md_info & info)
    {
    }
    bool get_next_items(md_info & info, std::vector <item_data_info *>&list)
    {
        return false;
    }

    void set_area_quota(int area, uint64_t quota) 
    {
    }
    void set_area_quota(std::map<int, uint64_t> &quota_map)
    {
    }

    void get_stats(tair_stat * stat)
    {
    }
private:
    char buf[1024];
    int  buf_len;
};


}; // end namespace rdb
}; // end namespace storage
}; // end namespace tari




#endif
