Name: %NAME
Version: 2.3.5.5
Release: 58.el5
Summary: Taobao key/value storage system
Group: Application
URL: http:://yum.corp.alimama.com
Packager:mobing.fql
License: GPL
Vendor: TaoBao
Prefix:%{_prefix}
Source:%{NAME}-%{VERSION}.tar.gz
BuildRoot: %{_tmppath}/%{name}-root
#BuildRequires: t-csrd-tbnet-devel >= 1.0.4
#Requires: openssl-devel >= 0.9

%description
CodeUrl:http://code.taobao.org/svn/tair/tags/rdb_release_20121009
CodeRev: 1468
Tair is a high performance, distribution key/value storage system.

%package devel
Summary: tair_rdb c++ client library
Group: Development/Libraries

%description devel
CodeUrl:http://code.taobao.org/svn/tair/tags/rdb_release_20121009
CodeRev: 1468
The %name-devel package contains  libraries and header
files for developing applications that use the %name package.

%prep
%setup

%build
#export TBLIB_ROOT=/opt/csr/common
chmod u+x bootstrap.sh
./bootstrap.sh
./configure --prefix=%{_prefix} --with-release=yes --with-kdb=yes --with-boost=%BOOST_DIR
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make DESTDIR=$RPM_BUILD_ROOT install

%clean
rm -rf $RPM_BUILD_ROOT

%post
echo %{_prefix}/lib > /etc/ld.so.conf.d/tair_rdb-%{VERSION}.conf
echo /opt/csr/common/lib >> /etc/ld.so.conf.d/tair_rdb-%{VERSION}.conf
/sbin/ldconfig

%post devel
echo %{_prefix}/lib > /etc/ld.so.conf.d/tair_rdb-%{VERSION}.conf
echo /opt/csr/common/lib >> /etc/ld.so.conf.d/tair_rdb-%{VERSION}.conf
/sbin/ldconfig

%postun
rm  -f /etc/ld.so.conf.d/tair_rdb-%{VERSION}.conf

%files
%defattr(0755, admin, admin)
%{_prefix}/bin
%{_prefix}/sbin
%{_prefix}/lib
%config(noreplace) %{_prefix}/etc/*
%attr(0755, admin, admin) %{_prefix}/set_shm.sh
%attr(0755, admin, admin) %{_prefix}/tair.sh

%files devel
%{_prefix}/include
%{_prefix}/lib/libtairclientapi.*
%{_prefix}/lib/libtairclientapi_c.*

%changelog

*Thu Mar 2 2011 MaoQi <maoqi@taobao.com>
-add post and config(noreplace)
