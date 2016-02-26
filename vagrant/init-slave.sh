#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

install_ssh_keys() {
  # own key
  cp .vagrant/`hostname`_key.pub /home/vagrant/.ssh/id_rsa.pub
  cp .vagrant/`hostname`_key /home/vagrant/.ssh/id_rsa
  chown vagrant:vagrant /home/vagrant/.ssh/id_rsa*

  # other hosts keys
  cat .vagrant/*_key.pub >> /home/vagrant/.ssh/authorized_keys
}

cd /vagrant/vagrant

# name resolution
cp .vagrant/hosts /etc/hosts

install_ssh_keys

# disable ipv6
echo -e "\nnet.ipv6.conf.all.disable_ipv6 = 1\n" >> /etc/sysctl.conf
sysctl -p

# use apt-proxy if present
if [ -f ".vagrant/apt-proxy" ]; then
    apt_proxy=$(cat ".vagrant/apt-proxy")
    echo "Using apt-proxy: $apt_proxy";
    echo "Acquire::http::Proxy \"$apt_proxy\";" > /etc/apt/apt.conf.d/90-apt-proxy.conf
fi


# Fix locale issue
echo "LC_ALL=en_US.UTF-8" >> /etc/environment
echo "LANG=en_US.UTF-8" >> /etc/environment
locale
sysctl -p

apt-get update -q --fix-missing
apt-get -qy install software-properties-common
add-apt-repository ppa:george-edison55/cmake-3.x
apt-get update -q
# run this to find the correct version of cmake
# apt-cache policy cmake

# Install everything required to build mesos
apt-get -qy install \
  vim                                     \
  build-essential                         \
  autoconf                                \
  automake                                \
  cmake=3.2.2-2~ubuntu14.04.1~ppa1        \
  ca-certificates                         \
  gdb                                     \
  wget                                    \
  git-core                                \
  libcurl4-nss-dev                        \
  libsasl2-dev                            \
  libtool                                 \
  libsvn-dev                              \
  libapr1-dev                             \
  libgoogle-glog-dev                      \
  libboost-dev                            \
  protobuf-compiler                       \
  libprotobuf-dev                         \
  make                                    \
  python                                  \
  python2.7                               \
  libpython-dev                           \
  python-dev                              \
  python-protobuf                         \
  python-setuptools                       \
  heimdal-clients                         \
  libsasl2-modules-gssapi-heimdal         \
  unzip                                   \
  libsasl2-modules                        \
  maven                                   \
  --no-install-recommends

# Install oracle java
add-apt-repository -y ppa:webupd8team/java
apt-get -y update
/bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
apt-get -y install oracle-java7-installer oracle-java7-set-default

# picojson and protobuf are required for mesos build
wget https://raw.githubusercontent.com/kazuho/picojson/v1.3.0/picojson.h -O /usr/local/include/picojson.h
mkdir -p /usr/share/java/
wget http://search.maven.org/remotecontent?filepath=com/google/protobuf/protobuf-java/2.5.0/protobuf-java-2.5.0.jar -O protobuf.jar
mv protobuf.jar /usr/share/java/

mkdir -p /tmp

# Checkout 0.27 mesos
mkdir -p /mesos
cd /mesos
sudo git clone git://git.apache.org/mesos.git /mesos
sudo git checkout tags/0.27.0
sudo git log -n 1

# Build mesos - with unbundled dependencies
./bootstrap
mkdir build && cd build && ../configure --disable-optimize --without-included-zookeeper --with-glog=/usr/local --with-protobuf=/usr --with-boost=/usr/local
make -j 1 install

echo "export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so" >> ~/.bashrc
echo "export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so" >> /home/vagrant/.bashrc

# Checkout and build mesos net-module
cd /home/vagrant
git clone https://github.com/mesosphere/net-modules.git
cd net-modules/isolator
./bootstrap
mkdir build && cd build && ../configure --with-mesos=/usr/local --with-protobuf=/usr
make && make install

# Install docker - calico-node can run as docker image only (for now)
curl -sSL https://get.docker.com/ | sh
# Add vagrant user so docker is available
usermod -aG docker vagrant
sysctl -p

# Download calico-mesos plugin
cd /home/vagrant
wget https://github.com/projectcalico/calico-mesos/releases/download/v0.1.3/calico_mesos
chmod +x calico_mesos
mkdir /calico
mv calico_mesos /calico/calico_mesos

# Get modules.json for mesos networking plugin
wget https://raw.githubusercontent.com/projectcalico/calico-mesos-deployments/master/packages/sources/modules.json
mv modules.json /calico/modules.json

# Run calico-node
wget https://github.com/projectcalico/calico-containers/releases/download/v0.9.0/calicoctl
chmod +x calicoctl
ETCD_AUTHORITY=master:4001 ./calicoctl node

export ETCD_AUTHORITY=master:4001
# Start mesos-slave
nohup /mesos/build/bin/mesos-slave.sh              \
  --master=zk://master:2181/mesos                  \
  --executor_registration_timeout=10mins           \
  --containerizers=mesos                           \
  --log_dir=/var/log                               \
  --resources=ports:[5000-31100]                   \
  --modules=file:///calico/modules.json            \
  --isolation=com_mesosphere_mesos_NetworkIsolator \
  --hooks=com_mesosphere_mesos_NetworkHook > /var/log/mesos.out 2>&1 &