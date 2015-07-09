#include "binlog.hpp"
#include "data_entry.hpp"

#include <gtest/gtest.h>

using namespace tair::common;

TEST(test_simple_1, simple_binlog_test) {
   data_entry *org = new data_entry("fuck");
   BinLogWriter *writer = new BinLogWriter("fuck", true);
   writer->append(10,11,123,org);
   delete org;
   delete writer;

   BinLogReader *reader = new BinLogReader("fuck", true);
   int s_ret, b_ret, pcode;
   data_entry *key = NULL;
   reader->read(&s_ret, &b_ret, &pcode, &key);
   delete reader;

   if (key == NULL) {
       printf("key == NULL");
       return;
   }

   printf("s_ret: %d\n", s_ret);
   printf("b_ret: %d\n", b_ret);
   printf("pcode: %d\n", pcode);
   printf("key: %s\n", key->get_data());

   delete key;
}
