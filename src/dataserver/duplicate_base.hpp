/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: duplicate_base.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   Daoan <daoan@taobao.com>
 *
 */
#ifndef TAIR_DUPLICATE_BASE_H
#define TAIR_DUPLICATE_BASE_H
namespace tair{
   class base_duplicator {
   public:
      base_duplicator(){};
      virtual ~base_duplicator(){};
      virtual bool has_bucket_duplicate_done(int bucket_number) = 0;
   };
}
#endif
