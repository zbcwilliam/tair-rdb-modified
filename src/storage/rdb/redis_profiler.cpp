#include "tbsys.h"

extern "C"
{
  void profiler_start(char* start_desc);
  void profiler_stop();
  void profiler_begin(char* begin_desc);
  void profiler_end();
  void profiler_dump();
  void profiler_set_threshold(int threshold);
  void profiler_set_status(int status);
}

void profiler_start(char* start_desc)
{
  PROFILER_START(start_desc);
}
void profiler_stop()
{
  PROFILER_STOP();
}
void profiler_begin(char* begin_desc)
{
  PROFILER_BEGIN(begin_desc);
}
void profiler_end()
{
  PROFILER_END();
}
void profiler_dump()
{
  PROFILER_DUMP();
}
void profiler_set_threshold(int threshold)
{
  PROFILER_SET_THRESHOLD(threshold);
}
void profiler_set_status(int status)
{
  PROFILER_SET_STATUS(status);
}
