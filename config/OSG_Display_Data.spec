%define name OSG_Display_Data
%define version 0.9.7
%define release 2

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
Requires: MySQL-python python-matplotlib >= 0.99 numpy >= 1.1.1 msttcorefonts

%description
UNKNOWN

%prep
%setup

%build
python setup.py build

%install
python setup.py install --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES
#mkdir -p $RPM_BUILD_ROOT/etc/osg_display
#cp config/osg_display.conf $RPM_BUILD_ROOT/etc/osg_display
#cp src/scripts/osg_display.condor.cron $RPM_BUILD_ROOT/etc/osg_display
mkdir -p $RPM_BUILD_ROOT/var/log/osg_display
mkdir -p $RPM_BUILD_ROOT/var/www/html/osg_display

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root)
%attr(-, apache, apache) /var/log/osg_display
%attr(-, apache, apache) /var/www/html/osg_display
%config %attr(600, apache, apache) /etc/osg_display/osg_display.conf
%config /etc/osg_display/osg_display.condor.cron

