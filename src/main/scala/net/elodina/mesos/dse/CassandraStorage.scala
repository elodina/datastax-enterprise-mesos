/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.elodina.mesos.dse

import com.datastax.driver.core._
import net.elodina.mesos.dse.Node.{Failover, Reservation, Runtime, Stickiness}
import net.elodina.mesos.dse.Util.{Period, BindAddress}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Try

class CassandraStorage(port: Int, contactPoints: String, keyspace: String, stateTable: String) extends Storage {

  import CassandraStorage._

  private val tg = new AtomicMonotonicTimestampGenerator

  private val session =
    Cluster.builder()
      .withPort(port).addContactPoints(contactPoints).build().connect(keyspace)

  private val SelectPs = session.prepare(CassandraStorage.selectQuery(stateTable))
  private val InsertionPs = session.prepare(CassandraStorage.insertionQuery(stateTable))
  private val DeletionPs = session.prepare(CassandraStorage.deleteQuery(stateTable))

  private def stringOrNull[T](value: T): String = {
    if (value != null) value.toString
    else null
  }

  private def bindStickiness(boundStatement: BoundStatement, node: Node):BoundStatement = {
    if (node.stickiness != null)
      boundStatement.setString(NodeStickinessPeriod, stringOrNull(node.stickiness.period))
        .setDate(NodeStickinessStopTime, node.stickiness.stopTime).setString(NodeStickinessHostname, node.stickiness.hostname)
    else
      boundStatement
  }

  private def bindRuntime(boundStatement: BoundStatement, node: Node): BoundStatement = {
    if (node.runtime != null) {

      boundStatement
        .setString(NodeRuntimeTaskId, node.runtime.taskId).setString(NodeRuntimeExecutorId, node.runtime.executorId)
        .setString(NodeRuntimeSlaveId, node.runtime.slaveId).setString(NodeRuntimeHostname, node.runtime.hostname)
        .setString(NodeRuntimeAddress, node.runtime.address).setList(NodeRuntimeSeeds, if (node.runtime.seeds != null) node.runtime.seeds.asJava else null)

      if (node.runtime.reservation != null) {
        val reservationPorts =
          if (node.runtime.reservation != null) node.runtime.reservation.ports.map { case (v, r) => v.toString -> r }.asJava
          else null
        val reservationIgnoredPorts =
          if (node.runtime.reservation.ignoredPorts != null) node.runtime.reservation.ignoredPorts.map(stringOrNull).asJava
          else null
        boundStatement
          .setDouble(NodeRuntimeReservationCpus, node.runtime.reservation.cpus).setInt(NodeRuntimeReservationMem, node.runtime.reservation.mem.toInt)
          .setMap(NodeRuntimeReservationPorts, reservationPorts).setList(NodeRuntimeReservationIgnoredPorts, reservationIgnoredPorts)
      }

      boundStatement.setMap(NodeRuntimeAttributes, if (node.runtime.attributes != null) node.runtime.attributes.asJava else null)
    } else boundStatement
  }

  private def bindFailover(boundStatement: BoundStatement, node: Node): BoundStatement = {
    boundStatement
      .setString(NodeFailoverDelay, stringOrNull(node.failover.delay))
      .setString(NodeFailoverMaxDelay, stringOrNull(node.failover.maxDelay))
      .setInt(NodeFailoverFailures, node.failover.failures)
      .setDate(NodeFailoverFailureTime, node.failover.failureTime)

    if (node.failover.maxTries != null) boundStatement.setInt(NodeFailoverMaxTries, node.failover.maxTries)

    boundStatement
  }

  override def save(): Unit = {
    // go with default - atomic batches
    val batch = new BatchStatement()

    val boundStatement = DeletionPs.bind()
      .setLong(UsingTimestamp, tg.next()).setString(Namespace, Nodes.namespace)

    batch.add(boundStatement)

    for (cluster <- Nodes.clusters){

      if (cluster.getNodes.isEmpty) {
        val clusterPorts = cluster.ports.map{case (v, r) => v.toString -> Option(r).map(_.toString).getOrElse("")}.asJava
        val boundStatement =
          // size + 1 for USING TIMESTAMP clause
          InsertionPs.bind(List.fill(CassandraStorage.Fields.size + 1)(null) :_*)
          .setString(Namespace, Nodes.namespace).setString(FrameworkId, Nodes.frameworkId).setString(ClusterId, cluster.id)
          .setString(ClusterBindAddress, stringOrNull(cluster.bindAddress)).setMap[String, String](ClusterPorts, clusterPorts)
          .setBool(ClusterJmxRemote, cluster.jmxRemote).setString(ClusterJmxUser, cluster.jmxUser).setString(ClusterJmxPassword, cluster.jmxPassword)
          // nr_of_nodes set to 0, this will indicate node_id has a stub value, just to avoid null (as node_id is part of PK)
          .setInt(NrOfNodes, 0).setString(NodeId, "undefined")
          .setLong(UsingTimestamp, tg.next())

        batch.add(boundStatement)
      } else {
        // a workaround since C* doesn't support null values in maps
        val clusterPorts = cluster.ports.map { case (v, r) => v.toString -> Option(r).map(_.toString).getOrElse("") }.asJava

        for (node <- cluster.getNodes) {
          val boundStatement =
            InsertionPs.bind(List.fill(CassandraStorage.Fields.size + 1)(null): _*)
              .setString(Namespace, Nodes.namespace).setString(FrameworkId, Nodes.frameworkId)
              // cluster
              .setString(ClusterId, cluster.id).setString(ClusterBindAddress, stringOrNull(cluster.bindAddress)).setMap(ClusterPorts, clusterPorts)
              .setBool(ClusterJmxRemote, cluster.jmxRemote).setString(ClusterJmxUser, cluster.jmxUser).setString(ClusterJmxPassword, cluster.jmxPassword)
              .setInt(NrOfNodes, cluster.getNodes.size)
              // node
              .setString(NodeId, node.id).setString(NodeState, stringOrNull(node.state))

          bindStickiness(boundStatement, node)
          bindRuntime(boundStatement, node)
          bindFailover(boundStatement, node)

          boundStatement
            // node
            .setDouble(NodeCpu, node.cpu).setInt(NodeMem, node.mem.toInt).setBool(NodeSeed, node.seed)
            .setString(NodeJvmOptions, node.jvmOptions).setString(NodeRack, node.rack).setString(NodeDc, node.dc).setString(NodeConstraints, Util.formatConstraints(node.constraints))
            .setString(NodeSeedConstraints, Util.formatConstraints(node.seedConstraints)).setString(NodeDataFileDirs, node.dataFileDirs).setString(NodeCommitLogDir, node.commitLogDir)
            .setString(NodeSavedCachesDir, node.savedCachesDir).setMap(NodeCassandraDotYaml, node.cassandraDotYaml.asJava).setMap(NodeAddressDotYaml, node.addressDotYaml.asJava)
            .setString(NodeCassandraJvmOptions, node.cassandraJvmOptions).setBool(NodeModified, node.modified)
            .setLong(UsingTimestamp, tg.next())

          batch.add(boundStatement)
        }
      }
    }

    batch.setConsistencyLevel(ConsistencyLevel.ONE)
    session.execute(batch)
  }

  private def extractCluster(row: Row): Cluster = {
    val cluster = new Cluster()

    cluster.id = row.getString(ClusterId)
    val clusterBindAddress = row.getString(ClusterBindAddress)
    if (clusterBindAddress != null)
      cluster.bindAddress = new BindAddress(clusterBindAddress)

    cluster.resetPorts

    val clusterPorts = row.getMap(ClusterPorts, classOf[String], classOf[String]).asScala
    for ((port, range) <- clusterPorts){
      // a workaround since C* doesn't support null values in maps
      if (range == "")
        cluster.ports(Node.Port.withName(port)) = null
      else
        cluster.ports(Node.Port.withName(port)) = new Util.Range(range)
    }

    cluster.jmxRemote = row.getBool(ClusterJmxRemote)
    cluster.jmxUser = row.getString(ClusterJmxUser)
    cluster.jmxPassword = row.getString(ClusterJmxPassword)

    cluster
  }

  private def reservation(row: Row): Reservation = {
    val reservationPortMap = row.getMap(NodeRuntimeReservationPorts, classOf[String], classOf[java.lang.Integer]).asScala
    val reservationPorts = reservationPortMap.map { case (p, value) => Node.Port.withName(p) -> value.intValue() }.toMap

    val ignoredPortsList = row.getList(NodeRuntimeReservationIgnoredPorts, classOf[String]).asScala
    val ignoredPorts: List[Node.Port.Value] = ignoredPortsList.map(Node.Port.withName).toList

    new Reservation(cpus = row.getDouble(NodeRuntimeReservationCpus), mem = row.getInt(NodeRuntimeReservationMem).toLong,
      ports = reservationPorts, ignoredPorts = ignoredPorts)
  }

  private def runtime(row: Row): Runtime ={
    val attributes = row.getMap(NodeRuntimeAttributes, classOf[String], classOf[String]).asScala.toMap
    val seeds = row.getList(NodeRuntimeSeeds, classOf[String]).asScala.toList

    new Runtime(taskId = row.getString(NodeRuntimeTaskId), executorId = row.getString(NodeRuntimeExecutorId),
      slaveId = row.getString(NodeRuntimeSlaveId), hostname = row.getString(NodeRuntimeHostname), seeds = seeds,
      reservation = reservation(row), attributes = attributes)
  }

  private def stickiness(row: Row): Stickiness = {
    val stickiness = new Stickiness(new Period(row.getString(NodeStickinessPeriod)))
    stickiness.stopTime = row.getDate(NodeStickinessStopTime)
    stickiness.hostname = row.getString(NodeStickinessHostname)

    stickiness
  }

  private def failover(row: Row): Failover = {
    val failover = new Failover()
    failover.delay = new Period(row.getString(NodeFailoverDelay))
    failover.maxDelay = new Period(row.getString(NodeFailoverMaxDelay))
    if (row.getInt(NodeFailoverMaxTries) != 0) failover.maxTries = row.getInt(NodeFailoverMaxTries)
    failover.failures = row.getInt(NodeFailoverFailures)
    if (row.getDate(NodeFailoverFailureTime) != null) failover.failureTime = row.getDate(NodeFailoverFailureTime)

    failover
  }

  private def extractNode(row: Row, cluster: Cluster): Node = {
    val node = new Node(row.getString(NodeId))
    node.state = Node.State.withName(row.getString(NodeState))
    node.cluster = cluster

    node.stickiness = stickiness(row)

    val runtimeTaskId = row.getString(NodeRuntimeTaskId)
    if (runtimeTaskId == null) {
      node.runtime = null
    } else {
      node.runtime = runtime(row)
    }

    node.cpu = row.getDouble(NodeCpu)
    node.mem = row.getInt(NodeMem).toLong
    node.seed = row.getBool(NodeSeed)
    node.jvmOptions = row.getString(NodeJvmOptions)
    node.rack = row.getString(NodeRack)
    node.dc = row.getString(NodeDc)

    node.constraints.clear()
    node.constraints ++= Constraint.parse(row.getString(NodeConstraints))
    node.seedConstraints.clear()
    node.seedConstraints ++= Constraint.parse(row.getString(NodeSeedConstraints))

    node.dataFileDirs = row.getString(NodeDataFileDirs)
    node.commitLogDir = row.getString(NodeCommitLogDir)
    node.savedCachesDir = row.getString(NodeSavedCachesDir)
    node.commitLogDir = row.getString(NodeCommitLogDir)
    node.cassandraDotYaml.clear()
    node.cassandraDotYaml ++= row.getMap(NodeCassandraDotYaml, classOf[String], classOf[String]).asScala
    node.addressDotYaml.clear()
    node.addressDotYaml ++= row.getMap(NodeAddressDotYaml, classOf[String], classOf[String]).asScala
    node.cassandraJvmOptions = row.getString(NodeCassandraJvmOptions)
    node.modified = row.getBool(NodeModified)

    node.failover = failover(row)

    node
  }

  override def load(): Boolean = {
    val boundStatement = SelectPs.bind().setString(Namespace, Config.namespace).setConsistencyLevel(ConsistencyLevel.ONE)

    val rows = session.execute(boundStatement).all().asScala

    if (rows.nonEmpty) {
      val frameworkId = rows.headOption.map(_.getString(FrameworkId))
        .getOrElse(throw new IllegalStateException(s"FrameworkId is null for namespace ${Config.namespace}"))

      val clusters = new ListBuffer[Cluster]
      val nodes = new ListBuffer[Node]

      for (row <- rows) {
        val cluster = extractCluster(row)
        // due to data model the same cluster appears number of times equals to number of nodes in that cluster
        if (!clusters.exists(_.id == cluster.id))
          clusters += cluster

        val nrOfNodes = row.getInt(NrOfNodes)
        if (nrOfNodes != 0){
          val node = extractNode(row, cluster)
          nodes += node
        }
      }

      Nodes.namespace = Config.namespace
      Nodes.frameworkId = frameworkId

      Nodes.clusters.clear()
      clusters.foreach(Nodes.addCluster)

      Nodes.nodes.clear()
      nodes.foreach(Nodes.addNode)

      true
    } else {
      false
    }
  }

  override def close(): Unit = {
    Try {
      if (session != null)
        session.close()
    }
  }
}

object CassandraStorage{
  val Namespace = "namespace"
  val FrameworkId = "framework_id"
  val ClusterId = "cluster_id"
  val ClusterBindAddress = "cluster_bind_address"
  val ClusterPorts = "cluster_ports"
  val ClusterJmxRemote = "cluster_jmx_remote"
  val ClusterJmxUser = "cluster_jmx_user"
  val ClusterJmxPassword = "cluster_jmx_password"
  val NrOfNodes = "nr_of_nodes"
  val NodeId = "node_id"
  val NodeState = "node_state"
  val NodeStickinessPeriod = "node_stickiness_period"
  val NodeStickinessStopTime = "node_stickiness_stopTime"
  val NodeStickinessHostname = "node_stickiness_hostname"
  val NodeRuntimeTaskId = "node_runtime_task_id"
  val NodeRuntimeExecutorId = "node_runtime_executor_id"
  val NodeRuntimeSlaveId = "node_runtime_slave_id"
  val NodeRuntimeHostname = "node_runtime_hostname"
  val NodeRuntimeAddress = "node_runtime_address"
  val NodeRuntimeSeeds = "node_runtime_seeds"
  val NodeRuntimeReservationCpus = "node_runtime_reservation_cpus"
  val NodeRuntimeReservationMem = "node_runtime_reservation_mem"
  val NodeRuntimeReservationPorts = "node_runtime_reservation_ports"
  val NodeRuntimeReservationIgnoredPorts = "node_runtime_reservation_ignored_ports"
  val NodeRuntimeAttributes = "node_runtime_attributes"
  val NodeCpu = "node_cpu"
  val NodeMem = "node_mem"
  val NodeSeed = "node_seed"
  val NodeJvmOptions = "node_jvm_options"
  val NodeRack = "node_rack"
  val NodeDc = "node_dc"
  val NodeConstraints = "node_constraints"
  val NodeSeedConstraints = "node_seed_constraints"
  val NodeDataFileDirs = "node_data_file_dirs"
  val NodeCommitLogDir = "node_commit_log_dir"
  val NodeSavedCachesDir = "node_saved_caches_dir"
  val NodeCassandraDotYaml = "node_cassandra_dot_yaml"
  val NodeAddressDotYaml = "node_address_dot_yaml"
  val NodeCassandraJvmOptions = "node_cassandra_jvm_options"
  val NodeModified = "node_modified"
  val NodeFailoverDelay = "node_failover_delay"
  val NodeFailoverMaxDelay = "node_failover_max_delay"
  val NodeFailoverMaxTries = "node_failover_max_tries"
  val NodeFailoverFailures = "node_failover_failures"
  val NodeFailoverFailureTime = "node_failover_failure_time"

  // not part of the table schema
  val UsingTimestamp = "using_timestamp"

  val Fields = Seq(
    Namespace,
    FrameworkId,
    ClusterId,
    ClusterBindAddress,
    ClusterPorts,
    ClusterJmxRemote,
    ClusterJmxUser,
    ClusterJmxPassword,
    NrOfNodes,
    NodeId,
    NodeState,
    NodeStickinessPeriod,
    NodeStickinessStopTime,
    NodeStickinessHostname,
    NodeRuntimeTaskId,
    NodeRuntimeExecutorId,
    NodeRuntimeSlaveId,
    NodeRuntimeHostname,
    NodeRuntimeAddress,
    NodeRuntimeSeeds,
    NodeRuntimeReservationCpus,
    NodeRuntimeReservationMem,
    NodeRuntimeReservationPorts,
    NodeRuntimeReservationIgnoredPorts,
    NodeRuntimeAttributes,
    NodeCpu,
    NodeMem,
    NodeSeed,
    NodeJvmOptions,
    NodeRack,
    NodeDc,
    NodeConstraints,
    NodeSeedConstraints,
    NodeDataFileDirs,
    NodeCommitLogDir,
    NodeSavedCachesDir,
    NodeCassandraDotYaml,
    NodeAddressDotYaml,
    NodeCassandraJvmOptions,
    NodeModified,
    NodeFailoverDelay,
    NodeFailoverMaxDelay,
    NodeFailoverMaxTries,
    NodeFailoverFailures,
    NodeFailoverFailureTime
  )

  private def `:`(field: String) = ":" + field

  def insertionQuery(table: String) =
    s"INSERT INTO $table(${Fields.mkString(",")}) VALUES (${Fields.map(`:`).mkString(",")}) USING TIMESTAMP ${`:`(UsingTimestamp)}"

  def deleteQuery(table: String) =
    s"DELETE FROM $table USING TIMESTAMP ${`:`(UsingTimestamp)} WHERE $Namespace = ${`:`(Namespace)}"

  def selectQuery(table: String) =
    s"SELECT * FROM $table WHERE $Namespace = ${`:`(Namespace)}"
}
