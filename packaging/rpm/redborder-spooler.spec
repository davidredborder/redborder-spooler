%define debug_package %{nil}

Version: %{__version}
Release: %{__release}%{?dist}
Name: redborder-spooler
License: GNU AGPLv3
URL: https://github.com/redBorder/redborder-spooler
Source0: %{name}-%{version}.tar.gz

BuildRequires: golang
Requires(pre): shadow-utils

Summary: Multi-stream telemetry, syslog and HTTP capture, spooling and accelerated catch-up replay daemon for redBorder
Group: Development/Libraries/C and C++

%description
redborder-spooler is a high-performance daemon and CLI utility
designed for maintenance windows and release updates. It passively captures
incoming UDP flow telemetry (NetFlow v5/v9/IPFIX, sFlow v5), Syslog (UDP/TCP),
and HTTP payloads across network interfaces into time-partitioned UTC hourly
buckets, enforces retention policies, and provides accelerated catch-up replay
into local collectors.

%prep
%setup -qn %{name}-%{version}

%build
go build -mod=vendor -ldflags "-X main.version=%{version} -X main.revision=%{release} -s -w" -o rb-spooler ./cmd/rb-spooler

%install
mkdir -p %{buildroot}/usr/bin
install -p -m 755 rb-spooler %{buildroot}/usr/bin/rb-spooler

mkdir -p %{buildroot}/etc/redborder-spooler
mkdir -p %{buildroot}/var/spool/redborder-spooler

install -D -m 644 resources/systemd/redborder-spooler.service %{buildroot}/usr/lib/systemd/system/redborder-spooler.service

install -D -m 644 configs/config.yaml %{buildroot}/etc/redborder-spooler/config.yaml

%clean
rm -rf %{buildroot}

%pre
getent group redborder-spooler >/dev/null || groupadd -r redborder-spooler
getent passwd redborder-spooler >/dev/null || \
    useradd -r -g redborder-spooler -d /var/spool/redborder-spooler -s /sbin/nologin \
    -c "redborder-spooler service account" redborder-spooler
exit 0

%post
%systemd_post redborder-spooler.service
/usr/sbin/setcap cap_net_raw,cap_net_bind_service=+ep /usr/bin/rb-spooler 2>/dev/null || :
if [ -d /var/spool/redborder-spooler ]; then
    chown -R redborder-spooler:redborder-spooler /var/spool/redborder-spooler 2>/dev/null || :
fi

%preun
%systemd_preun redborder-spooler.service

%postun
%systemd_postun_with_restart redborder-spooler.service

%files
%license LICENSE
%defattr(755,root,root,755)
/usr/bin/rb-spooler
%attr(0750,root,redborder-spooler) %dir /etc/redborder-spooler
%config(noreplace) %attr(0640,root,redborder-spooler) /etc/redborder-spooler/config.yaml
%attr(0750,redborder-spooler,redborder-spooler) %dir /var/spool/redborder-spooler
%attr(0644,root,root) /usr/lib/systemd/system/redborder-spooler.service

%changelog
* Thu Nov 26 2026 David Vanhoucke <dvanhoucke@redborder.com> - 1.0.0-1
- Initial release of redborder-spooler daemon and CLI
