/**
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
import net.elodina.mesos.dse.Node.{Reservation, Runtime, Stickiness}
import net.elodina.mesos.dse.Util.{Period, BindAddress}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Try

class CassandraStorage(port: Int, contactPoints: String, keyspace: String, stateTable: String) extends Storage {

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
      boundStatement.setString(8, stringOrNull(node.stickiness.period)).setDate(9, node.stickiness.stopTime).setString(10, node.stickiness.hostname)
    else
      boundStatement
  }

  private def bindRuntime(boundStatement: BoundStatement, node: Node): BoundStatement = {
    if (node.runtime != null) {

      boundStatement
        .setString(11, node.runtime.taskId).setString(12, node.runtime.executorId).setString(13, node.runtime.slaveId)
        .setString(14, node.runtime.hostname).setString(15, node.runtime.address).setList(16, if (node.runtime.seeds != null) node.runtime.seeds.asJava else null)

      if (node.runtime.reservation != null) {
        val reservationPorts =
          if (node.runtime.reservation != null) node.runtime.reservation.ports.map { case (v, r) => v.toString -> r }.asJava
          else null
        val reservationIgnoredPorts =
          if (node.runtime.reservation.ignoredPorts != null) node.runtime.reservation.ignoredPorts.map(stringOrNull).asJava
          else null
        boundStatement
          .setDouble(17, node.runtime.reservation.cpus).setInt(18, node.runtime.reservation.mem.toInt).setMap(19, reservationPorts)
          .setList(20, reservationIgnoredPorts)
      }

      boundStatement.setMap(21, if (node.runtime.attributes != null) node.runtime.attributes.asJava else null)
    } else boundStatement
  }

  override def save(): Unit = {
    // go with default - atomic batches
    val batch = new BatchStatement()

    val boundStatement = DeletionPs.bind()
      .setLong(0, tg.next()).setString(1, Nodes.namespace)

    batch.add(boundStatement)

    for (cluster <- Nodes.clusters){

      if (cluster.getNodes.isEmpty) {
        val clusterPorts = cluster.ports.map{case (v, r) => v.toString -> Option(r).map(_.toString).getOrElse("")}.asJava
        val boundStatement =
          // size + 1 for USING TIMESTAMP clause
          InsertionPs.bind(List.fill(CassandraStorage.Fields.size + 1)(null) :_*)
          .setString(0, Nodes.namespace).setString(1, Nodes.frameworkId).setString(2, cluster.id)
          .setString(3, stringOrNull(cluster.bindAddress)).setMap[String, String](4, clusterPorts)
          // nr_of_nodes set to 0, this will indicate node_id has a stub value, just to avoid null (as node_id is part of PK)
          .setInt(5, 0).setString(6, "-1")
          .setLong(35, tg.next())

        batch.add(boundStatement)
      } else {
        // a workaround since C* doesn't support null values in maps
        val clusterPorts = cluster.ports.map { case (v, r) => v.toString -> Option(r).map(_.toString).getOrElse("") }.asJava

        for (node <- cluster.getNodes) {
          val boundStatement =
            InsertionPs.bind(List.fill(CassandraStorage.Fields.size + 1)(null): _*)
              .setString(0, Nodes.namespace).setString(1, Nodes.frameworkId)
              // cluster
              .setString(2, cluster.id).setString(3, stringOrNull(cluster.bindAddress)).setMap(4, clusterPorts)
              .setInt(5, cluster.getNodes.size)
              // node
              .setString(6, node.id).setString(7, stringOrNull(node.state))

          bindStickiness(boundStatement, node)
          bindRuntime(boundStatement, node)

          boundStatement
            // node
            .setDouble(22, node.cpu).setInt(23, node.mem.toInt).setBool(24, node.seed).setString(25, node.replaceAddress)
            .setString(26, node.jvmOptions).setString(27, node.rack).setString(28, node.dc).setString(29, Util.formatConstraints(node.constraints))
            .setString(30, Util.formatConstraints(node.seedConstraints)).setString(31, node.dataFileDirs).setString(32, node.commitLogDir)
            .setString(33, node.savedCachesDir).setMap(34, node.cassandraDotYaml.asJava).setLong(35, tg.next())

          batch.add(boundStatement)
        }
      }
    }

    batch.setConsistencyLevel(ConsistencyLevel.ONE)
    session.execute(batch)
  }

  private def extractCluster(row: Row): Cluster = {
    val cluster = new Cluster()

    cluster.id = row.getString("cluster_id")
    val clusterBindAddress = row.getString("cluster_bind_address")
    if (clusterBindAddress != null)
      cluster.bindAddress = new BindAddress(clusterBindAddress)

    cluster.resetPorts

    val clusterPorts = row.getMap("cluster_ports", classOf[String], classOf[String]).asScala
    for ((port, range) <- clusterPorts){
      // a workaround since C* doesn't support null values in maps
      if (range == "")
        cluster.ports(Node.Port.withName(port)) = null
      else
        cluster.ports(Node.Port.withName(port)) = new Util.Range(range)
    }

    cluster
  }

  private def reservation(row: Row): Reservation = {
    val reservationPortMap = row.getMap("node_runtime_reservation_ports", classOf[String], classOf[java.lang.Integer]).asScala
    val reservationPorts = reservationPortMap.map { case (p, value) => Node.Port.withName(p) -> value.intValue() }.toMap

    val ignoredPortsList = row.getList("node_runtime_reservation_ignored_ports", classOf[String]).asScala
    val ignoredPorts: List[Node.Port.Value] = ignoredPortsList.map(Node.Port.withName).toList

    new Reservation(cpus = row.getDouble("node_runtime_reservation_cpus"), mem = row.getInt("node_runtime_reservation_mem").toLong,
      ports = reservationPorts, ignoredPorts = ignoredPorts)
  }

  private def runtime(row: Row): Runtime ={
    val attributes = row.getMap("node_runtime_attributes", classOf[String], classOf[String]).asScala.toMap
    val seeds = row.getList("node_runtime_seeds", classOf[String]).asScala.toList

    new Runtime(taskId = row.getString("node_runtime_task_id"), executorId = row.getString("node_runtime_executor_id"),
      slaveId = row.getString("node_runtime_slave_id"), hostname = row.getString("node_runtime_hostname"), seeds = seeds,
      reservation = reservation(row), attributes = attributes)
  }

  private def stickiness(row: Row): Stickiness = {
    val stickiness = new Stickiness(new Period(row.getString("node_stickiness_period")))
    stickiness.stopTime = row.getDate("node_stickiness_stopTime")
    stickiness.hostname = row.getString("node_stickiness_hostname")

    stickiness
  }

  private def extractNode(row: Row, cluster: Cluster): Node = {
    val node = new Node(row.getString("node_id"))
    node.state = Node.State.withName(row.getString("node_state"))
    node.cluster = cluster

    node.stickiness = stickiness(row)

    val runtimeTaskId = row.getString("node_runtime_task_id")
    if (runtimeTaskId == null) {
      node.runtime = null
    } else {
      node.runtime = runtime(row)
    }

    node.cpu = row.getDouble("node_cpu")
    node.mem = row.getInt("node_mem").toLong
    node.seed = row.getBool("node_seed")
    node.replaceAddress = row.getString("node_replace_address")
    node.jvmOptions = row.getString("node_jvm_options")
    node.rack = row.getString("node_rack")
    node.dc = row.getString("node_dc")

    node.constraints.clear()
    node.constraints ++= Constraint.parse(row.getString("node_constraints"))
    node.seedConstraints.clear()
    node.seedConstraints ++= Constraint.parse(row.getString("node_seed_constraints"))

    node.dataFileDirs = row.getString("node_data_file_dirs")
    node.commitLogDir = row.getString("node_commit_log_dir")
    node.savedCachesDir = row.getString("node_saved_caches_dir")
    node.commitLogDir = row.getString("node_commit_log_dir")
    node.cassandraDotYaml.clear()
    node.cassandraDotYaml ++= row.getMap("node_cassandra_dot_yaml", classOf[String], classOf[String]).asScala

    node
  }

  override def load(): Boolean = {
    val boundStatement = SelectPs.bind().setString(0, Config.namespace).setConsistencyLevel(ConsistencyLevel.ONE)

    val rows = session.execute(boundStatement).all().asScala

    if (rows.nonEmpty) {
      val frameworkId = rows.headOption.map(_.getString("framework_id"))
        .getOrElse(throw new IllegalStateException(s"FrameworkId is null for namespace ${Config.namespace}"))

      val clusters = new ListBuffer[Cluster]
      val nodes = new ListBuffer[Node]

      for (row <- rows) {
        val cluster = extractCluster(row)
        // due to data model the same cluster appears number of times equals to number of nodes in that cluster
        if (!clusters.exists(_.id == cluster.id))
          clusters += cluster

        val nrOfNodes = row.getInt("nr_of_nodes")
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
  val Fields = Seq(
    /*0*/"namespace",
    /*1*/"framework_id",
    /*2*/"cluster_id",
    /*3*/"cluster_bind_address",
    /*4*/"cluster_ports",
    /*5*/"nr_of_nodes",
    /*6*/"node_id",
    /*7*/"node_state",
    /*8*/"node_stickiness_period",
    /*9*/"node_stickiness_stopTime",
    /*10*/"node_stickiness_hostname",
    /*11*/"node_runtime_task_id",
    /*12*/"node_runtime_executor_id",
    /*13*/"node_runtime_slave_id",
    /*14*/"node_runtime_hostname",
    /*15*/"node_runtime_address",
    /*16*/"node_runtime_seeds",
    /*17*/"node_runtime_reservation_cpus",
    /*18*/"node_runtime_reservation_mem",
    /*19*/"node_runtime_reservation_ports",
    /*20*/"node_runtime_reservation_ignored_ports",
    /*21*/"node_runtime_attributes",
    /*22*/"node_cpu",
    /*23*/"node_mem",
    /*24*/"node_seed",
    /*25*/"node_replace_address",
    /*26*/"node_jvm_options",
    /*27*/"node_rack",
    /*28*/"node_dc",
    /*29*/"node_constraints",
    /*30*/"node_seed_constraints",
    /*31*/"node_data_file_dirs",
    /*32*/"node_commit_log_dir",
    /*33*/"node_saved_caches_dir",
    /*34*/"node_cassandra_dot_yaml"
  )

  def insertionQuery(table: String) = 
    s"INSERT INTO $table(${Fields.mkString(",")}) VALUES (${List.fill(Fields.size)("?").mkString(",")}) USING TIMESTAMP ?"

  def deleteQuery(table: String) =
    s"DELETE FROM $table USING TIMESTAMP ? WHERE namespace = ?"

  def selectQuery(table: String) =
    s"SELECT * FROM $table WHERE namespace = ?"
}
