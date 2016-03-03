[![Build Status](https://travis-ci.org/elodina/datastax-enterprise-mesos.svg?branch=master)](https://travis-ci.org/elodina/datastax-enterprise-mesos)

DataStax Enterprise Mesos Framework
==================================

[Description](#description)    
[Installation](#installation)
* [Prerequisites](#prerequisites)
* [Scheduler configuration](#scheduler-configuration)
* [Run the scheduler](#run-the-scheduler)
* [Quick start](#quick-start)

[Typical operations](#typical-operations)
* [Shutting down framework](#shutting-down-framework)
* [Rolling restart](#rolling-restart)
* [Memory configuration](#memory-configuration)
* [Failed node recovery](#failed-node-recovery)

[Navigating the CLI](#navigating-the-cli)
* [Requesting help](#requesting-help)
* [Restarting nodes](#restarting-nodes)

Description
-----------

This framework aims to simplify running Datastax Enterprise on Mesos. Being actively developed right now.

Currently this framework supports running Cassandra nodes only.

Open issues here https://github.com/elodina/datastax-enterprise-mesos/issues

Installation
============
Prerequisites
-------------

Minimum supported Mesos version is 0.23.0

Clone and build the project

    # git clone https://github.com/elodina/datastax-enterprise-mesos
    # cd datastax-enterprise-mesos
    # ./gradlew jar
    
Get Java 7 JRE

    # wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/7u79-b15/jre-7u79-linux-x64.tar.gz
    
Get Datastax Enterprise distribution

    # wget --user user --password pass http://downloads.datastax.com/enterprise/dse.tar.gz
    
Scheduler configuration
-----------------------

```
# ./dse-mesos.sh help scheduler
Start scheduler
Usage: scheduler [options]

Option (* = required)  Description
---------------------  -----------
--debug <Boolean>      Run in debug mode. (default: false)
--framework-name       Framework name. Defaults to dse.
--framework-role       Framework role. Defaults to *.
--framework-timeout    Framework failover timeout. Defaults
                         to 30 days.
--jre                  Path to JRE archive.
* --master             Mesos Master addresses.
--principal            Principal (username) used to register
                         framework.
--secret               Secret (password) used to register
                         framework.
* --storage            Storage for cluster state. Examples:
                         file:dse-mesos.json; zk:master:
                         2181/dse-mesos.
--user                 Mesos user. Defaults to current system
                         user.

Generic Options
Option  Description
------  -----------
--api   Binding host:port for http/artifact
          server. Optional if DM_API env is
          set.
```
    
Quick start
-----------

In order not to pass the API url to each CLI call lets export the URL as follows:
```
# export DM_API=http://master:7000
```
First lets start 1 Cassandra node with the default settings. Further you will see how to change node settings.

```
# ./dse-mesos.sh node add 0
node added:
  id: 0
  state: idle
  topology: cluster:default, dc:default, rack:default
  resources: cpu:0.5, mem:512
  seed: false
  stickiness: period:30m
```
    
You now have a cluster with 1 Cassandra node that is not started.    

```
# ./dse-mesos.sh node list
node:
  id: 0
  state: idle
  topology: cluster:default, dc:default, rack:default
  resources: cpu:0.5, mem:512
  seed: false
  stickiness: period:30m
```

You can configure node with resource requirement like cpu or memory, also you can specify overrides to cassandra.yaml file.

```
# ./dse-mesos.sh node update 0 --cpu 0.8 --cassandra-yaml-configs max_hints_delivery_threads=2,hinted_handoff_enabled=false
node added:
  id: 0
  state: idle
  topology: cluster:default, dc:default, rack:default
  resources: cpu:0.8, mem:512
  seed: false
  stickiness: period:30m
  cassandra.yaml overrides: max_hints_delivery_threads=2,hinted_handoff_enabled=false
```

Key-value pairs specified after `--cassandra-yaml-configs` option will override default cassandra.yaml configuration file that comes with 
dse distribution. Note that this way you can override only *key-value* configs from casandra.yaml while there are also arrays,
objects. Also, supplied key-value pairs are "blindly" passed to the Cassandra process, i.e. configs are not validated. 

Now lets start the task. This call to CLI will block until the task is actually started but will wait no more than a configured timeout. Timeout can be passed via --timeout flag and defaults to 2 minutes. If a timeout of 0s is passed CLI won't wait for tasks to start at all and will reply with "Scheduled tasks ..." message.

```
# ./dse-mesos.sh node start 0
node started:
  id: 0
  state: running
  topology: cluster:default, dc:default, rack:default
  resources: cpu:0.5, mem:512
  seed: true
  stickiness: period:30m, hostname:slave0
  runtime:
    task id: node-0-1449579588537
    executor id: node-0-1449579588537
    slave id: faa6dc48-30d4-4385-8cd4-d0be512e0521-S1
    hostname: slave0
    seeds: slave0
```

By now you should have a single Cassandra node instance running. You should be able to connect to it via `cqlsh <hostname>` (in our case `cqlsh slave0` should work).

Here's how you stop it:

```
# ./dse-mesos.sh node stop 0
node stopped:
  id: 0
  state: idle
  topology: cluster:default, dc:default, rack:default
  resources: cpu:0.5, mem:512
  seed: true
  stickiness: period:30m, hostname:slave0, expires:2015-12-22 16:23:29+02
```

And remove:

```
# ./dse-mesos.sh node remove 0
node removed
```

OpsCenter support
-----------------
Ensure your OpsCenter version matches DataStax Enterprise version.

In order to integrate OpsCenter with existing Cassandra cluster you need to have a correct `address.yaml` configuration file.
This file is automatically created for you by the framework when you start the node.

DSE Agent (which is started with the Cassandra instance) will monitor the local node and by default
put all OpsCenter data to the local Cassandra cluster.

The only property which needs to be specified is `stomp_interface` - reachable IP address of the opscenterd machine.
You can set this property as part of `address-yaml-configs` cli option:
```
# ./dse-mesos.sh node update 0 --address-yaml-configs "stomp_interface=10.1.131.9"
```
 
Using external Cassandra in HA mode 
-----------------------------------

To run in HA mode (that is tolerate scheduler failures) this framework supports two options for persisting its state:
 
 - zookeeper (`scheduler --storage zk:...`)
 - external cassandra (`scheduler --storage casandra:...`)
 
 (Note: choosing storage type `file` doesn't guarantee you fault tolerance because after failover new scheduler instance may
 be started on a different machine where the initial state file will be not available thus new scheduler instance won't be able to
 recover its state)
 
To use `cassandra` storage type you need to do some preparations:
 
 1. have a running C* instance reachable from machines where you plan to run schedulers
 2. create a keyspace e.g. `dse_mesos` with a replication factor per your needs
 3. create a table e.g. `dse_mesos_framework` in a newly created keyspace, the schema is defined in `vagrant/cassandra_schema.cql`
  
  Last two items can be achieve by executing (cqlsh needs to be in PATH):
  
  ```
  # cqlsh host cql_port -f /vagrant/vagrant/cassandra_schema.cql
  ```
  
After that you can run scheduler with the following command:
  
  ```
  # ./dse-mesos.sh scheduler --master zk://master:2181/mesos --debug true --storage cassandra:9042:192.168.3.5,192.168.3.6 \
   --cassandra-keyspace dse_mesos --cassandra-table dse_mesos_framework
  ```
    
Typical operations
=================
Shutting down framework
-----------------------

While the scheduler has a shutdown hook it doesn't actually finish the framework.
To shutdown the framework completely (e.g. unregister it in Mesos) you may shoot a
`POST` to `/teardown` specifying the framework id to shutdown:

```
# curl -d frameworkId=20150807-094500-84125888-5050-14187-0005 -X POST http://master:5050/teardown
```

Rolling restart
---------------

You already have running nodes.

```
# ./dse-mesos.sh node update 0..1 --data-file-dirs /sstable/xvdv,/sstable/xvdw,/sstable/xvdx

nodes updated:
  id: 0
  state: running
  modified: has pending update
  topology: cluster:default, dc:default, rack:default
  resources: cpu:0.7, mem:1024
  seed: true
  data file dirs: /sstable/xvdv,/sstable/xvdw,/sstable/xvdx
  commit log dir: /var/lib/cassandra/commitlog
  ...

  id: 1
  state: running
  modified: has pending update
  topology: cluster:default, dc:default, rack:default
  resources: cpu:0.7, mem:1024
  seed: false
  data file dirs: /sstable/xvdv,/sstable/xvdw,/sstable/xvdx
  commit log dir: /var/lib/cassandra/commitlog
  ...

```

Whenever node has none `idle` state (`starting`, `running`, `stopping`, `reconciling`) and you update it,
`modified` flag will communicate that node `has pending update`, to apply update node has to be stopped.

Nodes will be restarted one by one, scheduler stops node then starts it, same applied to rest of the nodes.

```
# ./dse-mesos.sh node restart 0..1 --timeout 8min
stopping node 0 ... done
starting node 0 ... done
stopping node 1 ... done
starting node 1 ... done
nodes restarted:
  id: 0
  state: running
  data file dirs: /sstable/xvdv,/sstable/xvdw,/sstable/xvdx
  commit log dir: /var/lib/cassandra/commitlog
  ...

  id: 1
  state: running
  data file dirs: /sstable/xvdv,/sstable/xvdw,/sstable/xvdx
  commit log dir: /var/lib/cassandra/commitlog
  ...
```

Some times node could timeout on start or stop, in such case restart halts with notice:

```
./dse-mesos.sh node restart 0..1 --timeout 8min
stopping node 0 ... done
starting node 0 ... done
stopping node 1 ... done
starting node 1 ... Error: node 1 timeout on start
```

Memory configuration
--------------------

Given by `--mem` option RAM will be consumed by executor, DSE process, DSE agent.
DSE process capped by Xmx, however also consumes `offheap` memory. By default
- Xmx = max(min(1/2 mem, 1024MB), min(1/4 mem, 8GB))
- Xmn = min(100 * cpu, 1/4 * Xmx)

To override default Xmx and/or Xmn calculation specify it via `--cassandra-jvm-options`, for instance:
```
./dse-mesos.sh node update 0 --cassandra-jvm-options "-Xmx1024M"
```
in this case `Xmn` will be calculated by default (according to given `Xmx` in `--cassandra-jvm-options`), or override only `Xmn`
```
./dse-mesos.sh node update 0 --cassandra-jvm-options "-Xmn100M"
```
or both
```
./dse-mesos.sh node update 0 --cassandra-jvm-options "-Xmx2048M -Xmn100M"
```
`Xmx`, `Xmn` correspond to env variables `MAX_HEAP_SIZE`, `HEAP_NEWSIZE` and will be set in `cassandra-env.sh`

Failed node recovery
--------------------
When a node fails, DSE mesos scheduler assumes that the failure is recoverable. The scheduler will try
to restart the node after waiting failover-delay (i.e. 30s, 2m). The initial waiting delay is equal to failover-delay setting.
After each consecutive failure this delay is doubled until it reaches failover-max-delay value.

If failover-max-tries is defined and the consecutive failure count exceeds it, the node will be deactivated.

The following failover settings exists:
```
--failover-delay     - initial failover delay to wait after failure (option value is required)
--failover-max-delay - max failover delay (option value is required)
--failover-max-tries - max failover tries to deactivate broker (to reset to unbound pass --failover-max-tries "")
```

Navigating the CLI
==================
Requesting help
--------------

```
# ./dse-mesos.sh help
Usage: <cmd> ...

Commands:
  help [cmd [cmd]] - print general or command-specific help
  scheduler        - start scheduler
  node             - node management commands
  cluster          - cluster management commands

Run `help <cmd>` to see details of specific command
```

You may also run `./dse-mesos.sh help <cmd> [<cmd>]` to view help of specific command/sub-command.

Restarting nodes
----------------

```
# ./dse-mesos.sh help node restart
Restart node
Usage: node restart <id> [options]

Option     Description
------     -----------
--timeout  Time to wait until node restart.
             Should be a parsable Scala Duration
             value. Defaults to 4m.

Generic Options
Option  Description
------  -----------
--api   Binding host:port for http/artifact
          server. Optional if DM_API env is
          set.

```
