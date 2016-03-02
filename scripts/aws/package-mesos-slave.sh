#!/bin/bash

mkdir ~/package
cd ~/package

mkdir -p ./usr/local/bin/
cp /usr/local/bin/*mesos* ./usr/local/bin/

mkdir -p ./usr/local/etc/mesos
cp -r /usr/local/etc/mesos/* ./usr/local/etc/mesos/

mkdir -p ./usr/local/include/mesos
cp -r /usr/local/include/mesos/* ./usr/local/include/mesos/
cp /usr/local/include/picojson.h ./usr/local/include

mkdir -p ./usr/share/java
cp /usr/share/java/protobuf.jar ./usr/share/java

mkdir -p ./usr/local/lib
cp -r /usr/local/lib/* ./usr/local/lib/

mkdir -p ./usr/local/libexec/mesos
cp -r /usr/local/libexec/mesos/* ./usr/local/libexec/mesos/

mkdir -p ./usr/local/sbin/
cp -r /usr/local/sbin/*mesos* ./usr/local/sbin/

mkdir -p ./usr/local/share/mesos
cp -r /usr/local/share/mesos/* ./usr/local/share/mesos/

mkdir -p ./calico
cp /calico/calico_mesos ./calico
cp /calico/modules.json ./calico

mkdir -p ./usr/lib64/pkgconfig
cp /usr/lib64/pkgconfig/libglog.pc ./usr/lib64/pkgconfig

mkdir -p ./usr/lib64/
cp -r /usr/lib64/*glog* ./usr/lib64/

mkdir -p ./usr/include/glog/
cp /usr/include/glog/* ./usr/include/glog/
