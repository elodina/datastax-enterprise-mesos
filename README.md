Datastax Enterprise Mesos Framework
==================================

[Description](#description)    
[Installation](#installation)
* [Prerequisites](#prerequisites)
* [Scheduler configuration](#scheduler-configuration)
* [Run the scheduler](#run-the-scheduler)
* [Quick start](#quick-start)
* [Richer usage example](https://github.com/elodina/datastax-enterprise-mesos/tree/master/richer_usage_example.md)

[Typical operations](#typical-operations)
* [Shutting down framework](#shutting-down-framework)

[Navigating the CLI](#navigating-the-cli)
* [Requesting help](#requesting-help)  

Description
-----------

This framework aims to simplify running Datastax Enterprise on Mesos. Being actively developed right now.

Right now this framework supports running only Cassandra nodes.

Open issues here https://github.com/elodina/datastax-enterprise-mesos/issues

Installation
============
Prerequisites
-------------

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
Command: scheduler [options]
Starts the Datastax Enterprise Mesos Scheduler.
  --api <value>
        Binding host:port for http/artifact server. Optional if DM_API env is set.
  --master <value>
        Mesos Master addresses.
  --user <value>
        Mesos user. Defaults to current system user.
  --framework-name <value>
        Framework name. Defaults to datastax-enterprise
  --framework-role <value>
        Framework role. Defaults to *
  --framework-timeout <value>
        Framework failover timeout. Defaults to 30 days.
  --storage <value>
        Storage for cluster state. Examples: file:dse-mesos.json; zk:master:2181/dse-mesos.
  --debug <value>
        Run in debug mode.
```
    
Quick start
-----------

In order not to pass the API url to each CLI call lets export the URL as follows:

    # export DM_API=http://master:6666
    
First lets start 1 Cassandra node with the default settings. Further in the readme you can see how to change these from the defaults.    

```
# ./dse-mesos.sh add cassandra-node 0
Added tasks 0

cluster:
  task:
    id: 0
    type: cassandra-node
    state: Inactive
    cpu: 0.5
    mem: 512
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: Test Cluster
    seed: false
```
    
You now have a cluster with 1 Cassandra node that is not started.    

```
# ./dse-mesos.sh status
Retrieved current cluster status

cluster:
  task:
    id: 0
    type: cassandra-node
    state: Inactive
    cpu: 0.5
    mem: 512
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: Test Cluster
    seed: false
```

Now lets start the task. This call to CLI will block until the task is actually started but will wait no more than a configured timeout. Timeout can be passed via --timeout flag and defaults to 2 minutes. If a timeout of 0s is passed CLI won't wait for tasks to start at all and will reply with "Scheduled tasks ..." message.

```
# ./dse-mesos.sh start 0
Started tasks 0

cluster:
  task:
    id: 0
    type: cassandra-node
    state: Running
    cpu: 0.5
    mem: 512
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: Test Cluster
    seed: true
    seeds: slave0
    runtime:
      task id: cassandra-node-0-1446636187056
      slave id: fcbf0a90-1c26-472b-b854-c5f1d7c26d9f-S1
      executor id: cassandra-node-0-1446636187142
      hostname: slave0
      attributes:
```

By now you should have a single Cassandra node instance running. You should be able to connect to it via `cqlsh <hostname>` (in our case `cqlsh slave0` should work). Here's how you stop it:

```
# ./dse-mesos.sh stop 0
Stopped tasks 0

cluster:
  task:
    id: 0
    type: cassandra-node
    state: Inactive
    cpu: 0.5
    mem: 512
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: Test Cluster
    seed: true
    seeds: slave0
    runtime:
      task id: cassandra-node-0-1446636187056
      slave id: fcbf0a90-1c26-472b-b854-c5f1d7c26d9f-S1
      executor id: cassandra-node-0-1446636187142
      hostname: slave0
      attributes:
```

And remove:

```
# ./dse-mesos.sh remove 0
Removed tasks 0

cluster:
  task:
    id: 0
    type: cassandra-node
    state: Inactive
    cpu: 0.5
    mem: 512
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: Test Cluster
    seed: true
    seeds: slave0
```
    
Typical operations
=================
Shutting down framework
-----------------------

While the scheduler has a shutdown hook it doesn't actually finish the framework. To shutdown the framework completely (e.g. unregister it in Mesos) you may shoot a `POST` to `/teardown` specifying the framework id to shutdown:

```
# curl -d frameworkId=20150807-094500-84125888-5050-14187-0005 -X POST http://master:5050/teardown
```

Navigating the CLI
==================
Requesting help
--------------

```
# ./dse-mesos.sh --help
Usage: dse-mesos.sh [scheduler|add|update|start|stop|remove|status] [options] <args>...

  --help
        Prints this usage text.
Command: scheduler [options]
Starts the Datastax Enterprise Mesos Scheduler.
  --api <value>
        Binding host:port for http/artifact server. Optional if DM_API env is set.
  --master <value>
        Mesos Master addresses.
  --user <value>
        Mesos user. Defaults to current system user.
  --framework-name <value>
        Framework name. Defaults to datastax-enterprise
  --framework-role <value>
        Framework role. Defaults to *
  --framework-timeout <value>
        Framework failover timeout. Defaults to 30 days.
  --storage <value>
        Storage for cluster state. Examples: file:dse-mesos.json; zk:master:2181/dse-mesos.
  --debug <value>
        Run in debug mode.
Command: add <task-type>
Adds a task to the cluster.
  <task-type>
        Task type to add
  <id>
        ID expression to add
  --api <value>
        Binding host:port for http/artifact server. Optional if DM_API env is set.
  --cpu <value>
        CPU amount (0.5, 1, 2).
  --mem <value>
        Mem amount in Mb.
  --broadcast <value>
        Network interface to broadcast for nodes.
  --constraints <value>
        Constraints (hostname=like:^master$,rack=like:^1.*$).
  --seed-constraints <value>
        Seed node constraints. Will be evaluated only across seed nodes.
  --node-out <value>
        File name to redirect Datastax Node output to.
  --agent-out <value>
        File name to redirect Datastax Agent output to.
  --cluster-name <value>
        The name of the cluster.
  --seed <value>
        Flags whether this Datastax Node is a seed node.
  --data-file-dirs <value>
        Cassandra data file directories separated by comma. Defaults to sandbox if not set
  --commit-log-dir <value>
        Cassandra commit log dir. Defaults to sandbox if not set
  --saved-caches-dir <value>
        Cassandra saved caches dir. Defaults to sandbox if not set
  --state-backoff <value>
        Backoff between checks for consistent node state.
Command: update [options] <id>
Update task configuration.
  <id>
        ID expression to update
  --api <value>
        Binding host:port for http/artifact server. Optional if DM_API env is set.
  --cpu <value>
        CPU amount (0.5, 1, 2).
  --mem <value>
        Mem amount in Mb.
  --broadcast <value>
        Network interface to broadcast for nodes.
  --constraints <value>
        Constraints (hostname=like:^master$,rack=like:^1.*$).
  --seed-constraints <value>
        Seed node constraints. Will be evaluated only across seed nodes.
  --node-out <value>
        File name to redirect Datastax Node output to.
  --agent-out <value>
        File name to redirect Datastax Agent output to.
  --cluster-name <value>
        The name of the cluster.
  --seed <value>
        Flags whether this Datastax Node is a seed node.
  --data-file-dirs <value>
        Cassandra data file directories separated by comma. Defaults to sandbox if not set
  --commit-log-dir <value>
        Cassandra commit log dir. Defaults to sandbox if not set
  --saved-caches-dir <value>
        Cassandra saved caches dir. Defaults to sandbox if not set
  --state-backoff <value>
        Backoff between checks for consistent node state.
Command: start [options] <id>
Starts tasks in the cluster.
  <id>
        ID expression to add
  --api <value>
        Binding host:port for http/artifact server. Optional if DM_API env is set.
  --timeout <value>
        Time to wait until task starts. Should be a parsable Scala Duration value. Defaults to 2m. Optional
Command: stop [options] <id>
Stops tasks in the cluster.
  <id>
        ID expression to stop
  --api <value>
        Binding host:port for http/artifact server. Optional if DM_API env is set.
Command: remove [options] <id>
Removes tasks in the cluster.
  <id>
        ID expression to remove
  --api <value>
        Binding host:port for http/artifact server. Optional if DM_API env is set.
Command: status [options]
Retrieves current cluster status.
  --api <value>
        Binding host:port for http/artifact server. Optional if DM_API env is set.
```