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

install_mesos() {

    apt-get -qy install mesos=0.27.1*

    echo "zk://master:2181/mesos" > /etc/mesos/zk
    echo '10mins' > /etc/mesos-slave/executor_registration_timeout
    echo 'cpus:1;mem:2500;ports:[5000-32000]' > /etc/mesos-slave/resources

    ip=$(cat /etc/hosts | grep `hostname` | grep -E -o "([0-9]{1,3}[\.]){3}[0-9]{1,3}")
    echo $ip > "/etc/mesos-master/ip"

    ln -s /lib/init/upstart-job /etc/init.d/mesos-master
    service mesos-master start
}

install_marathon() {
    apt-get install -qy marathon=0.10.0*
    service marathon start
}

install_cassandra() {
    echo "deb http://debian.datastax.com/community stable main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
    curl -L http://debian.datastax.com/debian/repo_key | sudo apt-key add -
    apt-get update
    apt-get install dsc21=2.1.11-1 cassandra=2.1.11 -y --force-yes
    service cassandra stop
    rm -rf /var/lib/cassandra/data/system/*

    CASSANDRA_CFG=/etc/cassandra/cassandra.yaml
    cp $CASSANDRA_CFG ${CASSANDRA_CFG}.bak
    sed "s/- seeds:.*/- seeds: \"master\"/" $CASSANDRA_CFG > /tmp/cass.1.yaml
    sed "s/listen_address:.*/listen_address: /" /tmp/cass.1.yaml > /tmp/cass.2.yaml
    sed "s/rpc_address:.*/rpc_address: /" /tmp/cass.2.yaml > $CASSANDRA_CFG

    service cassandra start
    sleep 30
    cqlsh master -f /vagrant/vagrant/cassandra_schema.cql
}

install_docker() {
    # Install docker - calico-node can run as docker image only (for now)
    curl -sSL https://get.docker.com/ | sh
    # Add vagrant user so docker is available
    usermod -aG docker vagrant
    sysctl -p
}

install_opscenter() {
    apt-get install -qy sysstat opscenter=5.2.2
    service opscenterd start
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

# Install oracle java
add-apt-repository -y ppa:webupd8team/java
apt-get -y update
/bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
apt-get -y install oracle-java7-installer oracle-java7-set-default

# add mesosphere repo
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E56151BF
DISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
CODENAME=$(lsb_release -cs)
echo "deb http://repos.mesosphere.io/${DISTRO} ${CODENAME} main" | tee /etc/apt/sources.list.d/mesosphere.list

# add datastax repo
echo "deb http://debian.datastax.com/community stable main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
curl -L http://debian.datastax.com/debian/repo_key | sudo apt-key add -

# Install docker
curl -sSL https://get.docker.com/ | sh
# Add vagrant user so docker is available
usermod -aG docker vagrant
sysctl -p

# install etcd
docker pull quay.io/coreos/etcd:v2.2.0
export FQDN=`hostname -f`
mkdir -p /var/etcd
FQDN=`hostname -f` docker run --detach --name etcd --net host -v /var/etcd:/data quay.io/coreos/etcd:v2.2.0 \
     --advertise-client-urls "http://${FQDN}:2379,http://${FQDN}:4001" \
     --listen-client-urls "http://0.0.0.0:2379,http://0.0.0.0:4001" \
     --data-dir /data

install_mesos

# Run calico-node
cd /home/vagrant
wget https://github.com/projectcalico/calico-containers/releases/download/v0.9.0/calicoctl
chmod +x calicoctl
ETCD_AUTHORITY=master:4001 ./calicoctl node

# Manually add rule to allow in/out-bound trafic on 192.168.* as calico-mesos plugin creates default profile on eth0 (vagrant nat) interface
ETCD_AUTHORITY=master:4001 ./calicoctl profile default_slave0 rule add inbound allow from cidr 192.168.0.0/16
ETCD_AUTHORITY=master:4001 ./calicoctl profile default_slave0 rule add outbound allow to cidr 192.168.0.0/16

install_marathon
#install_cassandra
#install_opscenter
