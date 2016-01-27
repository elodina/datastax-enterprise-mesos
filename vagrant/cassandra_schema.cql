CREATE KEYSPACE IF NOT EXISTS dse_mesos
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE dse_mesos;

CREATE TABLE IF NOT EXISTS dse_mesos_framework(
        namespace text,
        framework_id text,
        cluster_id text,
        cluster_bind_address text,
        cluster_ports map<text, text>,
        node_id text,
        node_state text,
        node_stickiness_period text,
        node_stickiness_stopTime timestamp,
        node_stickiness_hostname text,
        node_runtime_task_id text,
        node_runtime_executor_id text,
        node_runtime_slave_id text,
        node_runtime_hostname text,
        node_runtime_address text,
        node_runtime_seeds list<text>,
        node_runtime_reservation_cpus double,
        node_runtime_reservation_mem int,
        node_runtime_reservation_ports map<text, int>,
        node_runtime_reservation_ignored_ports list<text>,
        node_runtime_attributes map<text, text>,
        node_cpu double,
        node_mem int,
        node_seed boolean,
        node_replace_address text,
        node_jvm_options text,
        node_rack text,
        node_dc text,
        node_constraints text,
        node_seed_constraints text,
        node_data_file_dirs text,
        node_commit_log_dir text,
        node_saved_caches_dir text,
        node_cassandra_dot_yaml map<text, text>,
        PRIMARY KEY(namespace, framework_id, cluster_id)
);