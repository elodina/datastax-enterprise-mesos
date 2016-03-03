AWS Scripts
===========

This folder contains scripts for bootstrapping and configuring AWS cluster to run mesos with calico support.
 
Files:

* `centos.cloudformation` - sample cloudformation file to create cluster in AWS with master, slave nodes and a separate 
testing node (use for demo purposes only)
* `centos-master-mesos-calico.sh` - mesos-master provisioning script. Downloads and starts the following services:
    -   zookeeper
    -   etcd in docker container
    -   calico-node in docker container
    -   mesos-master (0.27.1)
* `centos-slave-mesos-calico.sh` - mesos-slave provisioning script. Downloads and starts the following services:
    -   calico-node in docker container
    -   mesos-slave with networking module (taken from prepared package - see `package-mesos.slave.sh` description)
* `package-mesos-slave.sh` - convenience script to copy all related to mesos+networking files into `/home/centos/package`.
This can be helpful when mesos version has changed or require rebuild (for more info why mesos+networking has to be built manually - 
see [this](https://github.com/projectcalico/calico-mesos-deployments/blob/master/docs/ManualInstallCalicoMesos.md#5-install-mesos--netmodules-dependencies))
Note that package tar is already available at https://s3-us-west-2.amazonaws.com/mesos-calico/mesos-slave-package.tar.gz .

**NOTE**: if you plan to run calico on AWS you need to *disable source/dest. check*. Follow these instructions from the calico-team:

    Open the Amazon EC2 console at https://console.aws.amazon.com/ec2/.
    
    In the navigation pane, choose Instances.
    
    Select the NAT instance, choose Actions, select Networking, and then select Change Source/Dest. Check.
    
    For the NAT instance, verify that this attribute is disabled. Otherwise, choose Yes, Disable.


