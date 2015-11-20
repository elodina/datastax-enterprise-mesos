Richer usage example
--------------------

This step-by-step example shows how to spin a 9 node Cassandra cluster with 3 seed nodes spread across 3 different availability zones.

Open 2 terminal windows and cd to DSE framework directory.   
In terminal 1:

```
    # export DM_API=http://master:6666
    # ./dse-mesos.sh scheduler --master master:5050 --storage zk:master:2181/dse-mesos
```

In terminal 2:

```
    # export DM_API=http://master:6666
    # ./dse-mesos.sh add cassandra-node 0..8 --constraints "hostname=unique,az=atMost:3" --cluster-name example
Added tasks 0..8

cluster:
  task:
    id: 0
    type: cassandra-node
    state: Inactive
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: false
    constraints: hostname=unique,az=atMost:3

  task:
    id: 1
    type: cassandra-node
    state: Inactive
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: false
    constraints: hostname=unique,az=atMost:3

  task:
    id: 2
    type: cassandra-node
    state: Inactive
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: false
    constraints: hostname=unique,az=atMost:3

  task:
    id: 3
    type: cassandra-node
    state: Inactive
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: false
    constraints: hostname=unique,az=atMost:3

  task:
    id: 4
    type: cassandra-node
    state: Inactive
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: false
    constraints: hostname=unique,az=atMost:3

  task:
    id: 5
    type: cassandra-node
    state: Inactive
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: false
    constraints: hostname=unique,az=atMost:3

  task:
    id: 6
    type: cassandra-node
    state: Inactive
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: false
    constraints: hostname=unique,az=atMost:3

  task:
    id: 7
    type: cassandra-node
    state: Inactive
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: false
    constraints: hostname=unique,az=atMost:3

  task:
    id: 8
    type: cassandra-node
    state: Inactive
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: false
    constraints: hostname=unique,az=atMost:3
    
    # ./dse-mesos.sh update 0..2 --seed true --seed-constraints "az=unique"
Updated tasks 0..2

cluster:
  task:
    id: 0
    type: cassandra-node
    state: Inactive
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: true
    constraints: hostname=unique,az=atMost:3
    seed constraints: az=unique

  task:
    id: 1
    type: cassandra-node
    state: Inactive
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: true
    constraints: hostname=unique,az=atMost:3
    seed constraints: az=unique

  task:
    id: 2
    type: cassandra-node
    state: Inactive
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: true
    constraints: hostname=unique,az=atMost:3
    seed constraints: az=unique
    
    # ./dse-mesos.sh start 0..8 --timeout 20min
Started tasks 0..8

cluster:
  task:
    id: 0
    type: cassandra-node
    state: Running
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: true
    seeds: ip-172-20-17-103.node.thecluster
    constraints: hostname=unique,az=atMost:3
    seed constraints: az=unique
    runtime:
      task id: cassandra-node-0-1446711722749
      slave id: 20151103-122609-1059591340-5050-11958-S3
      executor id: cassandra-node-0-1446711722776
      hostname: ip-172-20-17-103.node.thecluster
      attributes: az=us-east-1b

  task:
    id: 1
    type: cassandra-node
    state: Running
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: true
    seeds: ip-172-20-17-103.node.thecluster
    constraints: hostname=unique,az=atMost:3
    seed constraints: az=unique
    runtime:
      task id: cassandra-node-1-1446711782814
      slave id: 20151103-122609-1059591340-5050-11958-S1
      executor id: cassandra-node-1-1446711782814
      hostname: ip-172-20-3-55.node.thecluster
      attributes: az=us-east-1a

  task:
    id: 2
    type: cassandra-node
    state: Running
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: true
    seeds: ip-172-20-17-103.node.thecluster,ip-172-20-3-55.node.thecluster
    constraints: hostname=unique,az=atMost:3
    seed constraints: az=unique
    runtime:
      task id: cassandra-node-2-1446711878931
      slave id: 20151103-122609-1059591340-5050-11958-S0
      executor id: cassandra-node-2-1446711878931
      hostname: ip-172-20-38-224.node.thecluster
      attributes: az=us-east-1c

  task:
    id: 3
    type: cassandra-node
    state: Running
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: false
    seeds: ip-172-20-17-103.node.thecluster,ip-172-20-3-55.node.thecluster,ip-172-20-38-224.node.thecluster
    constraints: hostname=unique,az=atMost:3
    runtime:
      task id: cassandra-node-3-1446711981057
      slave id: 20151103-122609-1059591340-5050-11958-S9
      executor id: cassandra-node-3-1446711981058
      hostname: ip-172-20-45-222.node.thecluster
      attributes: az=us-east-1c

  task:
    id: 4
    type: cassandra-node
    state: Running
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: false
    seeds: ip-172-20-17-103.node.thecluster,ip-172-20-3-55.node.thecluster,ip-172-20-38-224.node.thecluster
    constraints: hostname=unique,az=atMost:3
    runtime:
      task id: cassandra-node-4-1446712089186
      slave id: 20151103-122609-1059591340-5050-11958-S7
      executor id: cassandra-node-4-1446712089186
      hostname: ip-172-20-27-57.node.thecluster
      attributes: az=us-east-1b

  task:
    id: 5
    type: cassandra-node
    state: Running
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: false
    seeds: ip-172-20-17-103.node.thecluster,ip-172-20-3-55.node.thecluster,ip-172-20-38-224.node.thecluster
    constraints: hostname=unique,az=atMost:3
    runtime:
      task id: cassandra-node-5-1446712179293
      slave id: 20151103-122609-1059591340-5050-11958-S4
      executor id: cassandra-node-5-1446712179294
      hostname: ip-172-20-9-212.node.thecluster
      attributes: az=us-east-1a

  task:
    id: 6
    type: cassandra-node
    state: Running
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: false
    seeds: ip-172-20-17-103.node.thecluster,ip-172-20-3-55.node.thecluster,ip-172-20-38-224.node.thecluster
    constraints: hostname=unique,az=atMost:3
    runtime:
      task id: cassandra-node-6-1446712293430
      slave id: 20151103-122609-1059591340-5050-11958-S5
      executor id: cassandra-node-6-1446712293430
      hostname: ip-172-20-41-171.node.thecluster
      attributes: az=us-east-1c

  task:
    id: 7
    type: cassandra-node
    state: Running
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: false
    seeds: ip-172-20-17-103.node.thecluster,ip-172-20-3-55.node.thecluster,ip-172-20-38-224.node.thecluster
    constraints: hostname=unique,az=atMost:3
    runtime:
      task id: cassandra-node-7-1446712371530
      slave id: 20151103-122609-1059591340-5050-11958-S6
      executor id: cassandra-node-7-1446712371530
      hostname: ip-172-20-11-123.node.thecluster
      attributes: az=us-east-1a

  task:
    id: 8
    type: cassandra-node
    state: Running
    cpu: 2.0
    mem: 8192
    node out: cassandra-node.log
    agent out: datastax-agent.log
    cluster name: example
    seed: false
    seeds: ip-172-20-17-103.node.thecluster,ip-172-20-3-55.node.thecluster,ip-172-20-38-224.node.thecluster
    constraints: hostname=unique,az=atMost:3
    runtime:
      task id: cassandra-node-8-1446712467638
      slave id: 20151103-122609-1059591340-5050-11958-S8
      executor id: cassandra-node-8-1446712467638
      hostname: ip-172-20-2-101.node.thecluster
      attributes: az=us-east-1a
```

Cassandra nodes take time to start (approx. ~1.5 minutes each) so be patient, startup takes a while.