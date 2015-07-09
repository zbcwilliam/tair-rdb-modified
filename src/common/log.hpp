/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * wrap macros for tblog
 *
 * Version: $Id: log.hpp 28 2010-09-19 05:18:09Z ruohai@taobao.com $
 *
 * Authors:
 *   ruohai <ruohai@taobao.com>
 *     - initial release
 *
 */
#ifndef TAIR_LOG_H
#define TAIR_LOG_H
#include <tbsys.h>

namespace tair {
#define log_error(...) TBSYS_LOG(ERROR, __VA_ARGS__)
#define log_warn(...) TBSYS_LOG(WARN, __VA_ARGS__)
#define log_info(...) TBSYS_LOG(INFO, __VA_ARGS__)
#define log_debug(...) TBSYS_LOG(DEBUG, __VA_ARGS__)
}

#endif
