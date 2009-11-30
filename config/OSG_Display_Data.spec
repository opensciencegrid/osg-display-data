%define name OSG_Display_Data
%define version 0.1
%define release 1

Summary: Scripts and tools to generate the OSG Display's data.
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{version}.tar.gz
License: UNKNOWN
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Brian Bockelman <bbockelm@cse.unl.edu>
Requires: MySQL-python matplotlib >= 0.99

%description
UNKNOWN

%prep
%setup

%build
python setup.py build

%install
python setup.py install --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root)
%attr(-, daemon, daemon) /var/log/osg_display
%config /etc/osg_display/graphs.conf
%config /etc/osg_display/osg_display.condor.cron
