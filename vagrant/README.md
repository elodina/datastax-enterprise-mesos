# Vagrant VMs for Mesos cluster
Vagrantfile creates mesos cluster with following nodes:
- master;
- slave0..slave(N-1) (N is specified in vagrantfile);

Master provides web ui listening on http://master:5050
Both master and slave nodes runs mesos slave daemons.

Every node has pre-installed docker. Master node has pre-installed
marathon scheduler and Cassandra (meant to be used as framework storage).

Host's public key is copied to `authorized_hosts`,
so direct access like `ssh vagrant@master|slaveX` should work.

For general mesos overview please refer to
http://mesos.apache.org/documentation/latest/mesos-architecture/

## Node Names
During first run vagrantfile creates `hosts` file which
contains host names for cluster nodes. It is recommended
to append its content to `/etc/hosts` (or other OS-specific
location) of the running (hosting) OS to be able to refer
master and slaves by logical names.

## Startup
Mesos master and slaves daemons are started automatically.

Each slave node runs 'mesos-slave' daemon while master runs both
'mesos-master' and 'mesos-slave' daemons.

Daemons could be controlled by using:
`/etc/init.d/mesos-{master|slave} {start|stop|status|restart}`

## Configuration
Configuration is read from the following locations:
- `/etc/mesos`, `/etc/mesos-{master|slave}`
  for general or master|slave specific CLI options;
- `/etc/default/mesos`, `/etc/default/mesos-{master|slave}`
  for general or master|slave specific environment vars;

Please refer to CLI of 'mesos-master|slave' daemons and `/usr/bin/mesos-init-wrapper`
for details.

## Logs
Logs are written to `/var/log/mesos/mesos-{master|slave}.*`

## Starting the Framework with Cassandra as a Storage
After starting vagrant `master` machine should have C* service started, respective keyspace(`dse_mesos`) and table(`dse_mesos_framework`)
should be created. You can check it with cqlsh utility (available on PATH):

```
# cqlsh master -e 'SELECT * FROM dse_mesos.dse_mesos_framework;'
```

With that you can start the framework using local C* node as a storage:
```
# export DM_API=http://master:7001
# ./dse-mesos.sh scheduler --master zk://master:2181/mesos --debug true --storage cassandra:9042:master \
 --cassandra-keyspace dse_mesos --cassandra-table dse_mesos_framework  
```


