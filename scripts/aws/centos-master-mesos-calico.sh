#!/bin/bash

if (( $# != 1 ))
then
  echo "IP to bind services needed"
  exit 1
fi

# ip of this node - reachable for other nodes within VPC
PRIV_IP=$1

yum update -y

yum -y install wget

rpm -Uvh http://repos.mesosphere.com/el/7/noarch/RPMS/mesosphere-el-repo-7-1.noarch.rpm

echo "Installing Zookeeper"
yum -y install mesosphere-zookeeper
echo "1" >> /etc/zookeeper/conf/myid
echo "server.1=$PRIV_IP:2888:3888" >> /etc/zookeeper/conf/zoo.cfg
systemctl start zookeeper

echo "Installing mesos, starting mesos-master"
echo "Mesos available versions"
yum --showduplicates list mesos | expand
echo "Mesos version mesos-0.27.1-2.0.226.centos701406 will be installed"
yum -y install mesos-0.27.1-2.0.226.centos701406
service mesos-master restart

echo "Installing docker"
curl -fsSL https://get.docker.com/ | sh
service docker start
echo "Adding \"centos\" user to docker. Logout will be required"
usermod -aG docker centos

echo "Installing etcd-docker"
docker pull quay.io/coreos/etcd:v2.2.0
export FQDN=`hostname -f`
mkdir -p /var/etcd
FQDN=`hostname -f` docker run --detach --name etcd --net host -v /var/etcd:/data quay.io/coreos/etcd:v2.2.0 \
     --advertise-client-urls "http://${FQDN}:2379,http://${FQDN}:4001" \
     --listen-client-urls "http://0.0.0.0:2379,http://0.0.0.0:4001" \
     --data-dir /data

echo "Installing calico-docker"
cd /home/centos
wget https://github.com/projectcalico/calico-containers/releases/download/v0.9.0/calicoctl
chmod +x calicoctl
sudo ETCD_AUTHORITY=$FQDN:4001 ./calicoctl node