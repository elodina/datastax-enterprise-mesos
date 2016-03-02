#!/bin/bash

if (( $# != 1 ))
then
  echo "Master IP needed"
  exit 1
fi

# ip to mesos master
PRIV_MASTER_IP=$1
ETCD_IP=$PRIV_MASTER_IP

yum update -y

yum groupinstall -y 'Development Tools'
yum update -y
yum install -y wget git autoconf automake libcurl-devel \
apr-devel subversion-devel java java-devel protobuf-devel protobuf-python \
boost-devel zlib-devel maven libapr-devel cyrus-sasl-md5 python-devel

echo "Installing docker"
curl -fsSL https://get.docker.com/ | sh
service docker start
echo "Adding \"centos\" user to docker. Logout will be required"
usermod -aG docker centos

echo "Downloading mesos-master with networking module from s3"
cd /usr/centos/
wget https://s3-us-west-2.amazonaws.com/mesos-calico/mesos-slave-package.tar.gz
tar -xf mesos-slave-package.tar.gz
cd mesos-slave-package/
sudo cp -r --parents ./* /

echo "Installing calico-docker"
cd /home/centos
wget https://github.com/projectcalico/calico-containers/releases/download/v0.9.0/calicoctl
chmod +x calicoctl
sudo ETCD_AUTHORITY=$ETCD_IP:4001 ./calicoctl node

# Remove "require tty" to let mesos containers run sudo
sed -i '/Defaults    requiretty/d' /etc/sudoers

echo "Starting Mesos-slave"
ETCD_AUTHORITY=$ETCD_IP:4001 nohup /usr/local/sbin/mesos-slave \
--master=zk://$PRIV_MASTER_IP:2181/mesos \
--containerizers=mesos \
--modules=file:///calico/modules.json \
--isolation=com_mesosphere_mesos_NetworkIsolator \
--hooks=com_mesosphere_mesos_NetworkHook 1>/var/log/mesos.out 2>&1 &