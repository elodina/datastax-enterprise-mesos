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
--storage              Storage for cluster state. Examples:
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
  topology: ring: default, dc:default, rack:default
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
  topology: ring: default, dc:default, rack:default
  resources: cpu:0.5, mem:512
  seed: false
  stickiness: period:30m
```

Now lets start the task. This call to CLI will block until the task is actually started but will wait no more than a configured timeout. Timeout can be passed via --timeout flag and defaults to 2 minutes. If a timeout of 0s is passed CLI won't wait for tasks to start at all and will reply with "Scheduled tasks ..." message.

```
# ./dse-mesos.sh node start 0
node started:
  id: 0
  state: running
  topology: ring: default, dc:default, rack:default
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
  topology: ring: default, dc:default, rack:default
  resources: cpu:0.5, mem:512
  seed: true
  stickiness: period:30m, hostname:slave0, expires:2015-12-22 16:23:29+02
```

And remove:

```
# ./dse-mesos.sh node remove 0
node removed
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
  ring             - ring management commands

Run `help <cmd>` to see details of specific command
```

You may also run `./dse-mesos.sh help <cmd> [<cmd>]` to view help of specific command/sub-command.