# Makefile.in generated by automake 1.13.4 from Makefile.am.
# test/rdb_interface_test/Makefile.  Generated from Makefile.in by configure.

# Copyright (C) 1994-2013 Free Software Foundation, Inc.

# This Makefile.in is free software; the Free Software Foundation
# gives unlimited permission to copy and/or distribute it,
# with or without modifications, as long as this notice is preserved.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY, to the extent permitted by law; without
# even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE.




am__is_gnu_make = test -n '$(MAKEFILE_LIST)' && test -n '$(MAKELEVEL)'
am__make_running_with_option = \
  case $${target_option-} in \
      ?) ;; \
      *) echo "am__make_running_with_option: internal error: invalid" \
              "target option '$${target_option-}' specified" >&2; \
         exit 1;; \
  esac; \
  has_opt=no; \
  sane_makeflags=$$MAKEFLAGS; \
  if $(am__is_gnu_make); then \
    sane_makeflags=$$MFLAGS; \
  else \
    case $$MAKEFLAGS in \
      *\\[\ \	]*) \
        bs=\\; \
        sane_makeflags=`printf '%s\n' "$$MAKEFLAGS" \
          | sed "s/$$bs$$bs[$$bs $$bs	]*//g"`;; \
    esac; \
  fi; \
  skip_next=no; \
  strip_trailopt () \
  { \
    flg=`printf '%s\n' "$$flg" | sed "s/$$1.*$$//"`; \
  }; \
  for flg in $$sane_makeflags; do \
    test $$skip_next = yes && { skip_next=no; continue; }; \
    case $$flg in \
      *=*|--*) continue;; \
        -*I) strip_trailopt 'I'; skip_next=yes;; \
      -*I?*) strip_trailopt 'I';; \
        -*O) strip_trailopt 'O'; skip_next=yes;; \
      -*O?*) strip_trailopt 'O';; \
        -*l) strip_trailopt 'l'; skip_next=yes;; \
      -*l?*) strip_trailopt 'l';; \
      -[dEDm]) skip_next=yes;; \
      -[JT]) skip_next=yes;; \
    esac; \
    case $$flg in \
      *$$target_option*) has_opt=yes; break;; \
    esac; \
  done; \
  test $$has_opt = yes
am__make_dryrun = (target_option=n; $(am__make_running_with_option))
am__make_keepgoing = (target_option=k; $(am__make_running_with_option))
pkgdatadir = $(datadir)/tair_rdb
pkgincludedir = $(includedir)/tair_rdb
pkglibdir = $(libdir)/tair_rdb
pkglibexecdir = $(libexecdir)/tair_rdb
am__cd = CDPATH="$${ZSH_VERSION+.}$(PATH_SEPARATOR)" && cd
install_sh_DATA = $(install_sh) -c -m 644
install_sh_PROGRAM = $(install_sh) -c
install_sh_SCRIPT = $(install_sh) -c
INSTALL_HEADER = $(INSTALL_DATA)
transform = $(program_transform_name)
NORMAL_INSTALL = :
PRE_INSTALL = :
POST_INSTALL = :
NORMAL_UNINSTALL = :
PRE_UNINSTALL = :
POST_UNINSTALL = :
build_triplet = x86_64-unknown-linux-gnu
host_triplet = x86_64-unknown-linux-gnu
sbin_PROGRAMS = sadd_general_test$(EXEEXT) \
	smembers_general_test$(EXEEXT) srem_general_test$(EXEEXT) \
	hset_general_test$(EXEEXT) hget_general_test$(EXEEXT) \
	hmget_general_test$(EXEEXT) hmset_general_test$(EXEEXT) \
	hgetall_general_test$(EXEEXT) hdel_general_test$(EXEEXT) \
	hash_normal_test$(EXEEXT) set_normal_test$(EXEEXT) \
	test_test_helper$(EXEEXT)
subdir = test/rdb_interface_test
DIST_COMMON = $(srcdir)/Makefile.in $(srcdir)/Makefile.am \
	$(top_srcdir)/depcomp
ACLOCAL_M4 = $(top_srcdir)/aclocal.m4
am__aclocal_m4_deps = $(top_srcdir)/ax_boost.m4 \
	$(top_srcdir)/configure.ac
am__configure_deps = $(am__aclocal_m4_deps) $(CONFIGURE_DEPENDENCIES) \
	$(ACLOCAL_M4)
mkinstalldirs = $(install_sh) -d
CONFIG_CLEAN_FILES =
CONFIG_CLEAN_VPATH_FILES =
am__installdirs = "$(DESTDIR)$(sbindir)"
PROGRAMS = $(sbin_PROGRAMS)
am__objects_1 = test_helper.$(OBJEXT)
am_hash_normal_test_OBJECTS = hash_normal_test.$(OBJEXT) \
	$(am__objects_1)
hash_normal_test_OBJECTS = $(am_hash_normal_test_OBJECTS)
hash_normal_test_LDADD = $(LDADD)
hash_normal_test_DEPENDENCIES =  \
	$(top_builddir)/src/client/.libs/libtairclientapi.a \
	$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a
AM_V_lt = $(am__v_lt_$(V))
am__v_lt_ = $(am__v_lt_$(AM_DEFAULT_VERBOSITY))
am__v_lt_0 = --silent
am__v_lt_1 = 
am_hdel_general_test_OBJECTS = hdel_general_test.$(OBJEXT) \
	$(am__objects_1)
hdel_general_test_OBJECTS = $(am_hdel_general_test_OBJECTS)
hdel_general_test_LDADD = $(LDADD)
hdel_general_test_DEPENDENCIES =  \
	$(top_builddir)/src/client/.libs/libtairclientapi.a \
	$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a
am_hget_general_test_OBJECTS = hget_general_test.$(OBJEXT) \
	$(am__objects_1)
hget_general_test_OBJECTS = $(am_hget_general_test_OBJECTS)
hget_general_test_LDADD = $(LDADD)
hget_general_test_DEPENDENCIES =  \
	$(top_builddir)/src/client/.libs/libtairclientapi.a \
	$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a
am_hgetall_general_test_OBJECTS = hgetall_general_test.$(OBJEXT) \
	$(am__objects_1)
hgetall_general_test_OBJECTS = $(am_hgetall_general_test_OBJECTS)
hgetall_general_test_LDADD = $(LDADD)
hgetall_general_test_DEPENDENCIES =  \
	$(top_builddir)/src/client/.libs/libtairclientapi.a \
	$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a
am_hmget_general_test_OBJECTS = hmget_general_test.$(OBJEXT) \
	$(am__objects_1)
hmget_general_test_OBJECTS = $(am_hmget_general_test_OBJECTS)
hmget_general_test_LDADD = $(LDADD)
hmget_general_test_DEPENDENCIES =  \
	$(top_builddir)/src/client/.libs/libtairclientapi.a \
	$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a
am_hmset_general_test_OBJECTS = hmset_general_test.$(OBJEXT) \
	$(am__objects_1)
hmset_general_test_OBJECTS = $(am_hmset_general_test_OBJECTS)
hmset_general_test_LDADD = $(LDADD)
hmset_general_test_DEPENDENCIES =  \
	$(top_builddir)/src/client/.libs/libtairclientapi.a \
	$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a
am_hset_general_test_OBJECTS = hset_general_test.$(OBJEXT) \
	$(am__objects_1)
hset_general_test_OBJECTS = $(am_hset_general_test_OBJECTS)
hset_general_test_LDADD = $(LDADD)
hset_general_test_DEPENDENCIES =  \
	$(top_builddir)/src/client/.libs/libtairclientapi.a \
	$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a
am_sadd_general_test_OBJECTS = sadd_general_test.$(OBJEXT) \
	$(am__objects_1)
sadd_general_test_OBJECTS = $(am_sadd_general_test_OBJECTS)
sadd_general_test_LDADD = $(LDADD)
sadd_general_test_DEPENDENCIES =  \
	$(top_builddir)/src/client/.libs/libtairclientapi.a \
	$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a
am_set_normal_test_OBJECTS = set_normal_test.$(OBJEXT) \
	$(am__objects_1)
set_normal_test_OBJECTS = $(am_set_normal_test_OBJECTS)
set_normal_test_LDADD = $(LDADD)
set_normal_test_DEPENDENCIES =  \
	$(top_builddir)/src/client/.libs/libtairclientapi.a \
	$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a
am_smembers_general_test_OBJECTS = smembers_general_test.$(OBJEXT) \
	$(am__objects_1)
smembers_general_test_OBJECTS = $(am_smembers_general_test_OBJECTS)
smembers_general_test_LDADD = $(LDADD)
smembers_general_test_DEPENDENCIES =  \
	$(top_builddir)/src/client/.libs/libtairclientapi.a \
	$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a
am_srem_general_test_OBJECTS = srem_general_test.$(OBJEXT) \
	$(am__objects_1)
srem_general_test_OBJECTS = $(am_srem_general_test_OBJECTS)
srem_general_test_LDADD = $(LDADD)
srem_general_test_DEPENDENCIES =  \
	$(top_builddir)/src/client/.libs/libtairclientapi.a \
	$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a
am_test_test_helper_OBJECTS = test_test_helper.$(OBJEXT) \
	$(am__objects_1)
test_test_helper_OBJECTS = $(am_test_test_helper_OBJECTS)
test_test_helper_LDADD = $(LDADD)
test_test_helper_DEPENDENCIES =  \
	$(top_builddir)/src/client/.libs/libtairclientapi.a \
	$(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a
AM_V_P = $(am__v_P_$(V))
am__v_P_ = $(am__v_P_$(AM_DEFAULT_VERBOSITY))
am__v_P_0 = false
am__v_P_1 = :
AM_V_GEN = $(am__v_GEN_$(V))
am__v_GEN_ = $(am__v_GEN_$(AM_DEFAULT_VERBOSITY))
am__v_GEN_0 = @echo "  GEN     " $@;
am__v_GEN_1 = 
AM_V_at = $(am__v_at_$(V))
am__v_at_ = $(am__v_at_$(AM_DEFAULT_VERBOSITY))
am__v_at_0 = @
am__v_at_1 = 
DEFAULT_INCLUDES = -I.
depcomp = $(SHELL) $(top_srcdir)/depcomp
am__depfiles_maybe = depfiles
am__mv = mv -f
CXXCOMPILE = $(CXX) $(DEFS) $(DEFAULT_INCLUDES) $(INCLUDES) \
	$(AM_CPPFLAGS) $(CPPFLAGS) $(AM_CXXFLAGS) $(CXXFLAGS)
LTCXXCOMPILE = $(LIBTOOL) $(AM_V_lt) --tag=CXX $(AM_LIBTOOLFLAGS) \
	$(LIBTOOLFLAGS) --mode=compile $(CXX) $(DEFS) \
	$(DEFAULT_INCLUDES) $(INCLUDES) $(AM_CPPFLAGS) $(CPPFLAGS) \
	$(AM_CXXFLAGS) $(CXXFLAGS)
AM_V_CXX = $(am__v_CXX_$(V))
am__v_CXX_ = $(am__v_CXX_$(AM_DEFAULT_VERBOSITY))
am__v_CXX_0 = @echo "  CXX     " $@;
am__v_CXX_1 = 
CXXLD = $(CXX)
CXXLINK = $(LIBTOOL) $(AM_V_lt) --tag=CXX $(AM_LIBTOOLFLAGS) \
	$(LIBTOOLFLAGS) --mode=link $(CXXLD) $(AM_CXXFLAGS) \
	$(CXXFLAGS) $(AM_LDFLAGS) $(LDFLAGS) -o $@
AM_V_CXXLD = $(am__v_CXXLD_$(V))
am__v_CXXLD_ = $(am__v_CXXLD_$(AM_DEFAULT_VERBOSITY))
am__v_CXXLD_0 = @echo "  CXXLD   " $@;
am__v_CXXLD_1 = 
COMPILE = $(CC) $(DEFS) $(DEFAULT_INCLUDES) $(INCLUDES) $(AM_CPPFLAGS) \
	$(CPPFLAGS) $(AM_CFLAGS) $(CFLAGS)
LTCOMPILE = $(LIBTOOL) $(AM_V_lt) --tag=CC $(AM_LIBTOOLFLAGS) \
	$(LIBTOOLFLAGS) --mode=compile $(CC) $(DEFS) \
	$(DEFAULT_INCLUDES) $(INCLUDES) $(AM_CPPFLAGS) $(CPPFLAGS) \
	$(AM_CFLAGS) $(CFLAGS)
AM_V_CC = $(am__v_CC_$(V))
am__v_CC_ = $(am__v_CC_$(AM_DEFAULT_VERBOSITY))
am__v_CC_0 = @echo "  CC      " $@;
am__v_CC_1 = 
CCLD = $(CC)
LINK = $(LIBTOOL) $(AM_V_lt) --tag=CC $(AM_LIBTOOLFLAGS) \
	$(LIBTOOLFLAGS) --mode=link $(CCLD) $(AM_CFLAGS) $(CFLAGS) \
	$(AM_LDFLAGS) $(LDFLAGS) -o $@
AM_V_CCLD = $(am__v_CCLD_$(V))
am__v_CCLD_ = $(am__v_CCLD_$(AM_DEFAULT_VERBOSITY))
am__v_CCLD_0 = @echo "  CCLD    " $@;
am__v_CCLD_1 = 
SOURCES = $(hash_normal_test_SOURCES) $(hdel_general_test_SOURCES) \
	$(hget_general_test_SOURCES) $(hgetall_general_test_SOURCES) \
	$(hmget_general_test_SOURCES) $(hmset_general_test_SOURCES) \
	$(hset_general_test_SOURCES) $(sadd_general_test_SOURCES) \
	$(set_normal_test_SOURCES) $(smembers_general_test_SOURCES) \
	$(srem_general_test_SOURCES) $(test_test_helper_SOURCES)
DIST_SOURCES = $(hash_normal_test_SOURCES) \
	$(hdel_general_test_SOURCES) $(hget_general_test_SOURCES) \
	$(hgetall_general_test_SOURCES) $(hmget_general_test_SOURCES) \
	$(hmset_general_test_SOURCES) $(hset_general_test_SOURCES) \
	$(sadd_general_test_SOURCES) $(set_normal_test_SOURCES) \
	$(smembers_general_test_SOURCES) $(srem_general_test_SOURCES) \
	$(test_test_helper_SOURCES)
am__can_run_installinfo = \
  case $$AM_UPDATE_INFO_DIR in \
    n|no|NO) false;; \
    *) (install-info --version) >/dev/null 2>&1;; \
  esac
am__tagged_files = $(HEADERS) $(SOURCES) $(TAGS_FILES) $(LISP)
# Read a list of newline-separated strings from the standard input,
# and print each of them once, without duplicates.  Input order is
# *not* preserved.
am__uniquify_input = $(AWK) '\
  BEGIN { nonempty = 0; } \
  { items[$$0] = 1; nonempty = 1; } \
  END { if (nonempty) { for (i in items) print i; }; } \
'
# Make sure the list of sources is unique.  This is necessary because,
# e.g., the same source file might be shared among _SOURCES variables
# for different programs/libraries.
am__define_uniq_tagged_files = \
  list='$(am__tagged_files)'; \
  unique=`for i in $$list; do \
    if test -f "$$i"; then echo $$i; else echo $(srcdir)/$$i; fi; \
  done | $(am__uniquify_input)`
ETAGS = etags
CTAGS = ctags
DISTFILES = $(DIST_COMMON) $(DIST_SOURCES) $(TEXINFOS) $(EXTRA_DIST)
ACLOCAL = ${SHELL} /root/tair-rdb/missing aclocal-1.13
AMTAR = $${TAR-tar}
AM_DEFAULT_VERBOSITY = 1
AR = ar
AUTOCONF = ${SHELL} /root/tair-rdb/missing autoconf
AUTOHEADER = ${SHELL} /root/tair-rdb/missing autoheader
AUTOMAKE = ${SHELL} /root/tair-rdb/missing automake-1.13
AWK = gawk
BOOST_CPPFLAGS = -pthread -I/usr/include
BOOST_DATE_TIME_LIB = -lboost_date_time
BOOST_FILESYSTEM_LIB = -lboost_filesystem
BOOST_IOSTREAMS_LIB = -lboost_iostreams
BOOST_LDFLAGS = -L/usr/lib
BOOST_PROGRAM_OPTIONS_LIB = -lboost_program_options
BOOST_REGEX_LIB = -lboost_regex
BOOST_SERIALIZATION_LIB = -lboost_serialization
BOOST_SIGNALS_LIB = -lboost_signals
BOOST_THREAD_LIB = -lboost_thread
BOOST_UNIT_TEST_FRAMEWORK_LIB = -lboost_unit_test_framework
BOOST_WSERIALIZATION_LIB = -lboost_wserialization
CC = gcc
CCDEPMODE = depmode=gcc3
CFLAGS = -g -O2
CPP = gcc -E
CPPFLAGS = 
CXX = g++
CXXCPP = g++ -E
CXXDEPMODE = depmode=gcc3
CXXFLAGS = -g -Wall -DBOOST_SPIRIT_THREADSAFE -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -DTAIR_DEBUG
CYGPATH_W = echo
DEFS = -DPACKAGE_NAME=\"tair_rdb\" -DPACKAGE_TARNAME=\"tair_rdb\" -DPACKAGE_VERSION=\"2.3.5.5\" -DPACKAGE_STRING=\"tair_rdb\ 2.3.5.5\" -DPACKAGE_BUGREPORT=\"ruohai@taobao.com\" -DPACKAGE_URL=\"\" -DPACKAGE=\"tair_rdb\" -DVERSION=\"2.3.5.5\" -DSTDC_HEADERS=1 -DHAVE_SYS_TYPES_H=1 -DHAVE_SYS_STAT_H=1 -DHAVE_STDLIB_H=1 -DHAVE_STRING_H=1 -DHAVE_MEMORY_H=1 -DHAVE_STRINGS_H=1 -DHAVE_INTTYPES_H=1 -DHAVE_STDINT_H=1 -DHAVE_UNISTD_H=1 -DHAVE_DLFCN_H=1 -DLT_OBJDIR=\".libs/\" -DHAVE_BOOST=/\*\*/ -DHAVE_BOOST_FILESYSTEM=/\*\*/ -DHAVE_BOOST_PROGRAM_OPTIONS=/\*\*/ -DHAVE_BOOST_THREAD=/\*\*/ -DHAVE_BOOST_IOSTREAMS=/\*\*/ -DHAVE_BOOST_SERIALIZATION=/\*\*/ -DHAVE_BOOST_SIGNALS=/\*\*/ -DHAVE_BOOST_DATE_TIME=/\*\*/ -DHAVE_BOOST_REGEX=/\*\*/ -DHAVE_BOOST_UNIT_TEST_FRAMEWORK=/\*\*/
DEPDIR = .deps
DLLTOOL = false
DSYMUTIL = 
DUMPBIN = 
ECHO_C = 
ECHO_N = -n
ECHO_T = 
EGREP = /usr/bin/grep -E
EXEEXT = 
FGREP = /usr/bin/grep -F
GCOV_LIB = 
GREP = /usr/bin/grep
INSTALL = /usr/bin/install -c
INSTALL_DATA = ${INSTALL} -m 644
INSTALL_PROGRAM = ${INSTALL}
INSTALL_SCRIPT = ${INSTALL}
INSTALL_STRIP_PROGRAM = $(install_sh) -c -s
LD = /usr/bin/ld -m elf_x86_64
LDFLAGS = 
LIBOBJS = 
LIBS = 
LIBTOOL = $(SHELL) $(top_builddir)/libtool
LIPO = 
LN_S = ln -s
LTLIBOBJS = 
MAKEINFO = ${SHELL} /root/tair-rdb/missing makeinfo
MANIFEST_TOOL = :
MKDIR_P = /usr/bin/mkdir -p
NM = /usr/bin/nm -B
NMEDIT = 
OBJDUMP = objdump
OBJEXT = o
OTOOL = 
OTOOL64 = 
PACKAGE = tair_rdb
PACKAGE_BUGREPORT = ruohai@taobao.com
PACKAGE_NAME = tair_rdb
PACKAGE_STRING = tair_rdb 2.3.5.5
PACKAGE_TARNAME = tair_rdb
PACKAGE_URL = 
PACKAGE_VERSION = 2.3.5.5
PATH_SEPARATOR = :
RANLIB = ranlib
SED = /usr/bin/sed
SET_MAKE = 
SHELL = /bin/sh
STRIP = strip
VERSION = 2.3.5.5
abs_builddir = /root/tair-rdb/test/rdb_interface_test
abs_srcdir = /root/tair-rdb/test/rdb_interface_test
abs_top_builddir = /root/tair-rdb
abs_top_srcdir = /root/tair-rdb
ac_ct_AR = ar
ac_ct_CC = gcc
ac_ct_CXX = g++
ac_ct_DUMPBIN = 
am__include = include
am__leading_dot = .
am__quote = 
am__tar = $${TAR-tar} chof - "$$tardir"
am__untar = $${TAR-tar} xf -
bindir = ${exec_prefix}/bin
build = x86_64-unknown-linux-gnu
build_alias = 
build_cpu = x86_64
build_os = linux-gnu
build_vendor = unknown
builddir = .
datadir = ${datarootdir}
datarootdir = ${prefix}/share
docdir = ${datarootdir}/doc/${PACKAGE_TARNAME}
dvidir = ${docdir}
exec_prefix = ${prefix}
host = x86_64-unknown-linux-gnu
host_alias = 
host_cpu = x86_64
host_os = linux-gnu
host_vendor = unknown
htmldir = ${docdir}
includedir = ${prefix}/include
infodir = ${datarootdir}/info
install_sh = ${SHELL} /root/tair-rdb/install-sh
libdir = ${exec_prefix}/lib
libexecdir = ${exec_prefix}/libexec
localedir = ${datarootdir}/locale
localstatedir = ${prefix}/var
mandir = ${datarootdir}/man
mkdir_p = $(MKDIR_P)
oldincludedir = /usr/include
pdfdir = ${docdir}
prefix = /root/tair_rdb_bin
program_transform_name = s,x,x,
psdir = ${docdir}
sbindir = ${exec_prefix}/sbin
sharedstatedir = ${prefix}/com
srcdir = .
sysconfdir = ${prefix}/etc
target_alias = 
top_build_prefix = ../../
top_builddir = ../..
top_srcdir = ../..
AM_CPPFLAGS = -pthread -I/usr/include -I$(TBLIB_ROOT)/include/tbsys \
			 -I$(TBLIB_ROOT)/include/tbnet \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/packets \
			 -I${top_srcdir}/src/storage/fdb \
			 -I${top_srcdir}/src/storage/mdb \
			 -I${top_srcdir}/src/storage/kdb \
			 -I${top_srcdir}/src/storage \
			 -I${top_srcdir}/src/client \
			 -I${top_srcdir}/src/dataserver \
			 -I${top_srcdir}/src/plugin \
       -I${top_srcdir}/src  \
       -I/opt/gtest-1.7.0/include \
       -I${top_srcdir}/src/storage/kdb/kyotocabinet

LDADD = \
	  $(top_builddir)/src/client/.libs/libtairclientapi.a \
	  $(TBLIB_ROOT)/lib/libtbnet.a \
	  $(TBLIB_ROOT)/lib/libtbsys.a 

source_list = base_test.hpp \
            test_helper.hpp \
            test_helper.cpp

AM_LDFLAGS = -static -lpthread -L/opt/gtest-1.7.0/lib -lgtest_main -lgtest  -lz -lrt ${BOOST_LDFLAGS} ${GCOV_LIB}  -lboost_thread
sadd_general_test_SOURCES = sadd_general_test.cpp ${source_list}
smembers_general_test_SOURCES = smembers_general_test.cpp ${source_list}
srem_general_test_SOURCES = srem_general_test.cpp ${source_list}
hset_general_test_SOURCES = hset_general_test.cpp ${source_list}
hget_general_test_SOURCES = hget_general_test.cpp ${source_list}
hmset_general_test_SOURCES = hmset_general_test.cpp ${source_list}
hmget_general_test_SOURCES = hmget_general_test.cpp ${source_list}
hgetall_general_test_SOURCES = hgetall_general_test.cpp ${source_list}
hdel_general_test_SOURCES = hdel_general_test.cpp ${source_list}
hash_normal_test_SOURCES = hash_normal_test.cpp ${source_list}
set_normal_test_SOURCES = set_normal_test.cpp ${source_list}
test_test_helper_SOURCES = test_test_helper.cpp ${source_list}
all: all-am

.SUFFIXES:
.SUFFIXES: .cpp .lo .o .obj
$(srcdir)/Makefile.in:  $(srcdir)/Makefile.am  $(am__configure_deps)
	@for dep in $?; do \
	  case '$(am__configure_deps)' in \
	    *$$dep*) \
	      ( cd $(top_builddir) && $(MAKE) $(AM_MAKEFLAGS) am--refresh ) \
	        && { if test -f $@; then exit 0; else break; fi; }; \
	      exit 1;; \
	  esac; \
	done; \
	echo ' cd $(top_srcdir) && $(AUTOMAKE) --gnu test/rdb_interface_test/Makefile'; \
	$(am__cd) $(top_srcdir) && \
	  $(AUTOMAKE) --gnu test/rdb_interface_test/Makefile
.PRECIOUS: Makefile
Makefile: $(srcdir)/Makefile.in $(top_builddir)/config.status
	@case '$?' in \
	  *config.status*) \
	    cd $(top_builddir) && $(MAKE) $(AM_MAKEFLAGS) am--refresh;; \
	  *) \
	    echo ' cd $(top_builddir) && $(SHELL) ./config.status $(subdir)/$@ $(am__depfiles_maybe)'; \
	    cd $(top_builddir) && $(SHELL) ./config.status $(subdir)/$@ $(am__depfiles_maybe);; \
	esac;

$(top_builddir)/config.status: $(top_srcdir)/configure $(CONFIG_STATUS_DEPENDENCIES)
	cd $(top_builddir) && $(MAKE) $(AM_MAKEFLAGS) am--refresh

$(top_srcdir)/configure:  $(am__configure_deps)
	cd $(top_builddir) && $(MAKE) $(AM_MAKEFLAGS) am--refresh
$(ACLOCAL_M4):  $(am__aclocal_m4_deps)
	cd $(top_builddir) && $(MAKE) $(AM_MAKEFLAGS) am--refresh
$(am__aclocal_m4_deps):
install-sbinPROGRAMS: $(sbin_PROGRAMS)
	@$(NORMAL_INSTALL)
	@list='$(sbin_PROGRAMS)'; test -n "$(sbindir)" || list=; \
	if test -n "$$list"; then \
	  echo " $(MKDIR_P) '$(DESTDIR)$(sbindir)'"; \
	  $(MKDIR_P) "$(DESTDIR)$(sbindir)" || exit 1; \
	fi; \
	for p in $$list; do echo "$$p $$p"; done | \
	sed 's/$(EXEEXT)$$//' | \
	while read p p1; do if test -f $$p \
	 || test -f $$p1 \
	  ; then echo "$$p"; echo "$$p"; else :; fi; \
	done | \
	sed -e 'p;s,.*/,,;n;h' \
	    -e 's|.*|.|' \
	    -e 'p;x;s,.*/,,;s/$(EXEEXT)$$//;$(transform);s/$$/$(EXEEXT)/' | \
	sed 'N;N;N;s,\n, ,g' | \
	$(AWK) 'BEGIN { files["."] = ""; dirs["."] = 1 } \
	  { d=$$3; if (dirs[d] != 1) { print "d", d; dirs[d] = 1 } \
	    if ($$2 == $$4) files[d] = files[d] " " $$1; \
	    else { print "f", $$3 "/" $$4, $$1; } } \
	  END { for (d in files) print "f", d, files[d] }' | \
	while read type dir files; do \
	    if test "$$dir" = .; then dir=; else dir=/$$dir; fi; \
	    test -z "$$files" || { \
	    echo " $(INSTALL_PROGRAM_ENV) $(LIBTOOL) $(AM_LIBTOOLFLAGS) $(LIBTOOLFLAGS) --mode=install $(INSTALL_PROGRAM) $$files '$(DESTDIR)$(sbindir)$$dir'"; \
	    $(INSTALL_PROGRAM_ENV) $(LIBTOOL) $(AM_LIBTOOLFLAGS) $(LIBTOOLFLAGS) --mode=install $(INSTALL_PROGRAM) $$files "$(DESTDIR)$(sbindir)$$dir" || exit $$?; \
	    } \
	; done

uninstall-sbinPROGRAMS:
	@$(NORMAL_UNINSTALL)
	@list='$(sbin_PROGRAMS)'; test -n "$(sbindir)" || list=; \
	files=`for p in $$list; do echo "$$p"; done | \
	  sed -e 'h;s,^.*/,,;s/$(EXEEXT)$$//;$(transform)' \
	      -e 's/$$/$(EXEEXT)/' \
	`; \
	test -n "$$list" || exit 0; \
	echo " ( cd '$(DESTDIR)$(sbindir)' && rm -f" $$files ")"; \
	cd "$(DESTDIR)$(sbindir)" && rm -f $$files

clean-sbinPROGRAMS:
	@list='$(sbin_PROGRAMS)'; test -n "$$list" || exit 0; \
	echo " rm -f" $$list; \
	rm -f $$list || exit $$?; \
	test -n "$(EXEEXT)" || exit 0; \
	list=`for p in $$list; do echo "$$p"; done | sed 's/$(EXEEXT)$$//'`; \
	echo " rm -f" $$list; \
	rm -f $$list

hash_normal_test$(EXEEXT): $(hash_normal_test_OBJECTS) $(hash_normal_test_DEPENDENCIES) $(EXTRA_hash_normal_test_DEPENDENCIES) 
	@rm -f hash_normal_test$(EXEEXT)
	$(AM_V_CXXLD)$(CXXLINK) $(hash_normal_test_OBJECTS) $(hash_normal_test_LDADD) $(LIBS)

hdel_general_test$(EXEEXT): $(hdel_general_test_OBJECTS) $(hdel_general_test_DEPENDENCIES) $(EXTRA_hdel_general_test_DEPENDENCIES) 
	@rm -f hdel_general_test$(EXEEXT)
	$(AM_V_CXXLD)$(CXXLINK) $(hdel_general_test_OBJECTS) $(hdel_general_test_LDADD) $(LIBS)

hget_general_test$(EXEEXT): $(hget_general_test_OBJECTS) $(hget_general_test_DEPENDENCIES) $(EXTRA_hget_general_test_DEPENDENCIES) 
	@rm -f hget_general_test$(EXEEXT)
	$(AM_V_CXXLD)$(CXXLINK) $(hget_general_test_OBJECTS) $(hget_general_test_LDADD) $(LIBS)

hgetall_general_test$(EXEEXT): $(hgetall_general_test_OBJECTS) $(hgetall_general_test_DEPENDENCIES) $(EXTRA_hgetall_general_test_DEPENDENCIES) 
	@rm -f hgetall_general_test$(EXEEXT)
	$(AM_V_CXXLD)$(CXXLINK) $(hgetall_general_test_OBJECTS) $(hgetall_general_test_LDADD) $(LIBS)

hmget_general_test$(EXEEXT): $(hmget_general_test_OBJECTS) $(hmget_general_test_DEPENDENCIES) $(EXTRA_hmget_general_test_DEPENDENCIES) 
	@rm -f hmget_general_test$(EXEEXT)
	$(AM_V_CXXLD)$(CXXLINK) $(hmget_general_test_OBJECTS) $(hmget_general_test_LDADD) $(LIBS)

hmset_general_test$(EXEEXT): $(hmset_general_test_OBJECTS) $(hmset_general_test_DEPENDENCIES) $(EXTRA_hmset_general_test_DEPENDENCIES) 
	@rm -f hmset_general_test$(EXEEXT)
	$(AM_V_CXXLD)$(CXXLINK) $(hmset_general_test_OBJECTS) $(hmset_general_test_LDADD) $(LIBS)

hset_general_test$(EXEEXT): $(hset_general_test_OBJECTS) $(hset_general_test_DEPENDENCIES) $(EXTRA_hset_general_test_DEPENDENCIES) 
	@rm -f hset_general_test$(EXEEXT)
	$(AM_V_CXXLD)$(CXXLINK) $(hset_general_test_OBJECTS) $(hset_general_test_LDADD) $(LIBS)

sadd_general_test$(EXEEXT): $(sadd_general_test_OBJECTS) $(sadd_general_test_DEPENDENCIES) $(EXTRA_sadd_general_test_DEPENDENCIES) 
	@rm -f sadd_general_test$(EXEEXT)
	$(AM_V_CXXLD)$(CXXLINK) $(sadd_general_test_OBJECTS) $(sadd_general_test_LDADD) $(LIBS)

set_normal_test$(EXEEXT): $(set_normal_test_OBJECTS) $(set_normal_test_DEPENDENCIES) $(EXTRA_set_normal_test_DEPENDENCIES) 
	@rm -f set_normal_test$(EXEEXT)
	$(AM_V_CXXLD)$(CXXLINK) $(set_normal_test_OBJECTS) $(set_normal_test_LDADD) $(LIBS)

smembers_general_test$(EXEEXT): $(smembers_general_test_OBJECTS) $(smembers_general_test_DEPENDENCIES) $(EXTRA_smembers_general_test_DEPENDENCIES) 
	@rm -f smembers_general_test$(EXEEXT)
	$(AM_V_CXXLD)$(CXXLINK) $(smembers_general_test_OBJECTS) $(smembers_general_test_LDADD) $(LIBS)

srem_general_test$(EXEEXT): $(srem_general_test_OBJECTS) $(srem_general_test_DEPENDENCIES) $(EXTRA_srem_general_test_DEPENDENCIES) 
	@rm -f srem_general_test$(EXEEXT)
	$(AM_V_CXXLD)$(CXXLINK) $(srem_general_test_OBJECTS) $(srem_general_test_LDADD) $(LIBS)

test_test_helper$(EXEEXT): $(test_test_helper_OBJECTS) $(test_test_helper_DEPENDENCIES) $(EXTRA_test_test_helper_DEPENDENCIES) 
	@rm -f test_test_helper$(EXEEXT)
	$(AM_V_CXXLD)$(CXXLINK) $(test_test_helper_OBJECTS) $(test_test_helper_LDADD) $(LIBS)

mostlyclean-compile:
	-rm -f *.$(OBJEXT)

distclean-compile:
	-rm -f *.tab.c

include ./$(DEPDIR)/hash_normal_test.Po
include ./$(DEPDIR)/hdel_general_test.Po
include ./$(DEPDIR)/hget_general_test.Po
include ./$(DEPDIR)/hgetall_general_test.Po
include ./$(DEPDIR)/hmget_general_test.Po
include ./$(DEPDIR)/hmset_general_test.Po
include ./$(DEPDIR)/hset_general_test.Po
include ./$(DEPDIR)/sadd_general_test.Po
include ./$(DEPDIR)/set_normal_test.Po
include ./$(DEPDIR)/smembers_general_test.Po
include ./$(DEPDIR)/srem_general_test.Po
include ./$(DEPDIR)/test_helper.Po
include ./$(DEPDIR)/test_test_helper.Po

.cpp.o:
	$(AM_V_CXX)$(CXXCOMPILE) -MT $@ -MD -MP -MF $(DEPDIR)/$*.Tpo -c -o $@ $<
	$(AM_V_at)$(am__mv) $(DEPDIR)/$*.Tpo $(DEPDIR)/$*.Po
#	$(AM_V_CXX)source='$<' object='$@' libtool=no \
#	DEPDIR=$(DEPDIR) $(CXXDEPMODE) $(depcomp) \
#	$(AM_V_CXX_no)$(CXXCOMPILE) -c -o $@ $<

.cpp.obj:
	$(AM_V_CXX)$(CXXCOMPILE) -MT $@ -MD -MP -MF $(DEPDIR)/$*.Tpo -c -o $@ `$(CYGPATH_W) '$<'`
	$(AM_V_at)$(am__mv) $(DEPDIR)/$*.Tpo $(DEPDIR)/$*.Po
#	$(AM_V_CXX)source='$<' object='$@' libtool=no \
#	DEPDIR=$(DEPDIR) $(CXXDEPMODE) $(depcomp) \
#	$(AM_V_CXX_no)$(CXXCOMPILE) -c -o $@ `$(CYGPATH_W) '$<'`

.cpp.lo:
	$(AM_V_CXX)$(LTCXXCOMPILE) -MT $@ -MD -MP -MF $(DEPDIR)/$*.Tpo -c -o $@ $<
	$(AM_V_at)$(am__mv) $(DEPDIR)/$*.Tpo $(DEPDIR)/$*.Plo
#	$(AM_V_CXX)source='$<' object='$@' libtool=yes \
#	DEPDIR=$(DEPDIR) $(CXXDEPMODE) $(depcomp) \
#	$(AM_V_CXX_no)$(LTCXXCOMPILE) -c -o $@ $<

mostlyclean-libtool:
	-rm -f *.lo

clean-libtool:
	-rm -rf .libs _libs

ID: $(am__tagged_files)
	$(am__define_uniq_tagged_files); mkid -fID $$unique
tags: tags-am
TAGS: tags

tags-am: $(TAGS_DEPENDENCIES) $(am__tagged_files)
	set x; \
	here=`pwd`; \
	$(am__define_uniq_tagged_files); \
	shift; \
	if test -z "$(ETAGS_ARGS)$$*$$unique"; then :; else \
	  test -n "$$unique" || unique=$$empty_fix; \
	  if test $$# -gt 0; then \
	    $(ETAGS) $(ETAGSFLAGS) $(AM_ETAGSFLAGS) $(ETAGS_ARGS) \
	      "$$@" $$unique; \
	  else \
	    $(ETAGS) $(ETAGSFLAGS) $(AM_ETAGSFLAGS) $(ETAGS_ARGS) \
	      $$unique; \
	  fi; \
	fi
ctags: ctags-am

CTAGS: ctags
ctags-am: $(TAGS_DEPENDENCIES) $(am__tagged_files)
	$(am__define_uniq_tagged_files); \
	test -z "$(CTAGS_ARGS)$$unique" \
	  || $(CTAGS) $(CTAGSFLAGS) $(AM_CTAGSFLAGS) $(CTAGS_ARGS) \
	     $$unique

GTAGS:
	here=`$(am__cd) $(top_builddir) && pwd` \
	  && $(am__cd) $(top_srcdir) \
	  && gtags -i $(GTAGS_ARGS) "$$here"
cscopelist: cscopelist-am

cscopelist-am: $(am__tagged_files)
	list='$(am__tagged_files)'; \
	case "$(srcdir)" in \
	  [\\/]* | ?:[\\/]*) sdir="$(srcdir)" ;; \
	  *) sdir=$(subdir)/$(srcdir) ;; \
	esac; \
	for i in $$list; do \
	  if test -f "$$i"; then \
	    echo "$(subdir)/$$i"; \
	  else \
	    echo "$$sdir/$$i"; \
	  fi; \
	done >> $(top_builddir)/cscope.files

distclean-tags:
	-rm -f TAGS ID GTAGS GRTAGS GSYMS GPATH tags

distdir: $(DISTFILES)
	@srcdirstrip=`echo "$(srcdir)" | sed 's/[].[^$$\\*]/\\\\&/g'`; \
	topsrcdirstrip=`echo "$(top_srcdir)" | sed 's/[].[^$$\\*]/\\\\&/g'`; \
	list='$(DISTFILES)'; \
	  dist_files=`for file in $$list; do echo $$file; done | \
	  sed -e "s|^$$srcdirstrip/||;t" \
	      -e "s|^$$topsrcdirstrip/|$(top_builddir)/|;t"`; \
	case $$dist_files in \
	  */*) $(MKDIR_P) `echo "$$dist_files" | \
			   sed '/\//!d;s|^|$(distdir)/|;s,/[^/]*$$,,' | \
			   sort -u` ;; \
	esac; \
	for file in $$dist_files; do \
	  if test -f $$file || test -d $$file; then d=.; else d=$(srcdir); fi; \
	  if test -d $$d/$$file; then \
	    dir=`echo "/$$file" | sed -e 's,/[^/]*$$,,'`; \
	    if test -d "$(distdir)/$$file"; then \
	      find "$(distdir)/$$file" -type d ! -perm -700 -exec chmod u+rwx {} \;; \
	    fi; \
	    if test -d $(srcdir)/$$file && test $$d != $(srcdir); then \
	      cp -fpR $(srcdir)/$$file "$(distdir)$$dir" || exit 1; \
	      find "$(distdir)/$$file" -type d ! -perm -700 -exec chmod u+rwx {} \;; \
	    fi; \
	    cp -fpR $$d/$$file "$(distdir)$$dir" || exit 1; \
	  else \
	    test -f "$(distdir)/$$file" \
	    || cp -p $$d/$$file "$(distdir)/$$file" \
	    || exit 1; \
	  fi; \
	done
check-am: all-am
check: check-am
all-am: Makefile $(PROGRAMS)
installdirs:
	for dir in "$(DESTDIR)$(sbindir)"; do \
	  test -z "$$dir" || $(MKDIR_P) "$$dir"; \
	done
install: install-am
install-exec: install-exec-am
install-data: install-data-am
uninstall: uninstall-am

install-am: all-am
	@$(MAKE) $(AM_MAKEFLAGS) install-exec-am install-data-am

installcheck: installcheck-am
install-strip:
	if test -z '$(STRIP)'; then \
	  $(MAKE) $(AM_MAKEFLAGS) INSTALL_PROGRAM="$(INSTALL_STRIP_PROGRAM)" \
	    install_sh_PROGRAM="$(INSTALL_STRIP_PROGRAM)" INSTALL_STRIP_FLAG=-s \
	      install; \
	else \
	  $(MAKE) $(AM_MAKEFLAGS) INSTALL_PROGRAM="$(INSTALL_STRIP_PROGRAM)" \
	    install_sh_PROGRAM="$(INSTALL_STRIP_PROGRAM)" INSTALL_STRIP_FLAG=-s \
	    "INSTALL_PROGRAM_ENV=STRIPPROG='$(STRIP)'" install; \
	fi
mostlyclean-generic:

clean-generic:

distclean-generic:
	-test -z "$(CONFIG_CLEAN_FILES)" || rm -f $(CONFIG_CLEAN_FILES)
	-test . = "$(srcdir)" || test -z "$(CONFIG_CLEAN_VPATH_FILES)" || rm -f $(CONFIG_CLEAN_VPATH_FILES)

maintainer-clean-generic:
	@echo "This command is intended for maintainers to use"
	@echo "it deletes files that may require special tools to rebuild."
clean: clean-am

clean-am: clean-generic clean-libtool clean-sbinPROGRAMS \
	mostlyclean-am

distclean: distclean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile
distclean-am: clean-am distclean-compile distclean-generic \
	distclean-tags

dvi: dvi-am

dvi-am:

html: html-am

html-am:

info: info-am

info-am:

install-data-am:

install-dvi: install-dvi-am

install-dvi-am:

install-exec-am: install-sbinPROGRAMS

install-html: install-html-am

install-html-am:

install-info: install-info-am

install-info-am:

install-man:

install-pdf: install-pdf-am

install-pdf-am:

install-ps: install-ps-am

install-ps-am:

installcheck-am:

maintainer-clean: maintainer-clean-am
	-rm -rf ./$(DEPDIR)
	-rm -f Makefile
maintainer-clean-am: distclean-am maintainer-clean-generic

mostlyclean: mostlyclean-am

mostlyclean-am: mostlyclean-compile mostlyclean-generic \
	mostlyclean-libtool

pdf: pdf-am

pdf-am:

ps: ps-am

ps-am:

uninstall-am: uninstall-sbinPROGRAMS

.MAKE: install-am install-strip

.PHONY: CTAGS GTAGS TAGS all all-am check check-am clean clean-generic \
	clean-libtool clean-sbinPROGRAMS cscopelist-am ctags ctags-am \
	distclean distclean-compile distclean-generic \
	distclean-libtool distclean-tags distdir dvi dvi-am html \
	html-am info info-am install install-am install-data \
	install-data-am install-dvi install-dvi-am install-exec \
	install-exec-am install-html install-html-am install-info \
	install-info-am install-man install-pdf install-pdf-am \
	install-ps install-ps-am install-sbinPROGRAMS install-strip \
	installcheck installcheck-am installdirs maintainer-clean \
	maintainer-clean-generic mostlyclean mostlyclean-compile \
	mostlyclean-generic mostlyclean-libtool pdf pdf-am ps ps-am \
	tags tags-am uninstall uninstall-am uninstall-sbinPROGRAMS


# Tell versions [3.59,3.63) of GNU make to not export all variables.
# Otherwise a system limit (for SysV at least) may be exceeded.
.NOEXPORT:
