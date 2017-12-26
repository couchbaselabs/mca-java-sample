#! /usr/bin/env bash

yum install -y pkgconfig

ulimit -n 70000
service couchbase-server stop
rpm -e couchbase-server
rm -rf /opt/couchbase

wget https://packages.couchbase.com/releases/5.0.0/couchbase-server-enterprise-5.0.0-centos7.x86_64.rpm
rpm --install couchbase-server-enterprise-5.0.0-centos7.x86_64.rpm

systemctl daemon-reexec
service couchbase-server start

systemctl stop firewalld

cat /sys/kernel/mm/transparent_hugepage/enabled
cat /sys/kernel/mm/transparent_hugepage/defrag
echo 'never' > /sys/kernel/mm/transparent_hugepage/enabled
echo 'never' > /sys/kernel/mm/transparent_hugepage/defrag