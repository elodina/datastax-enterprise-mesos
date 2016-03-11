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

import scala.collection.JavaConverters._
import net.elodina.mesos.dse.Util.Version
import com.datastax.driver.core._

trait Migration {
  val version: Version
  def migrateJson(json: Map[String, Any]): Map[String, Any]
  def migrateCassandra(session: Session): Unit
}

object Migration {
  import org.apache.log4j.Logger
  private val logger = Logger.getLogger(Migration.getClass)

  def migrate(from: Version, to: Version, migrations: Seq[Migration], updateVersion: Version => Unit, apply: Migration => Unit): Unit = {
    if (from == to) {
      logger.info("storage schema is up to date")
      return
    }

    val applicable = migrations
      .filter { m => m.version > from && m.version <= to}
      .sortBy(_.version)

    logger.info(s"migrating storage from $from to $to")

    for (m <- applicable) {
      logger.info("applying migration for version " + m.version)
      apply(m)
      updateVersion(m.version)
    }
    logger.info(s"migration from $from to $to completed")
  }

  val migrations = Seq[Migration](
    new Migration0_2_1_3(Config.cassandraKeyspace, Config.cassandraTable)
  )
}

class Migration0_2_1_3(keyspace: String, stateTable: String) extends Migration {
  override val version: Version = new Version("0.2.1.3")

  override def migrateJson(json: Map[String, Any]): Map[String, Any] = {
    // update cluster, add jmxRemote false
    val json1 = if (json.contains("clusters")) {
      val clusters = json("clusters").asInstanceOf[List[Map[String, Object]]]
      json.updated("clusters", clusters.map { cluster => cluster.updated("jmxRemote", false) })
    } else json

    // update nodes, add failover delay 3m and maxDelay 30m
    val json2 = if (json1.contains("nodes")) {
      val nodes = json1("nodes").asInstanceOf[List[Map[String, Object]]]
      json1.updated("nodes", nodes.map { node => node.updated("failover", Map("delay" -> "3m", "maxDelay" -> "30m")) })
    } else json1

    json2
  }

  val alters = Seq(
    s"alter table $keyspace.$stateTable add cluster_jmx_remote boolean",
    s"alter table $keyspace.$stateTable add cluster_jmx_user text",
    s"alter table $keyspace.$stateTable add cluster_jmx_password text",

    s"alter table $keyspace.$stateTable add node_failover_delay text",
    s"alter table $keyspace.$stateTable add node_failover_max_delay text",
    s"alter table $keyspace.$stateTable add node_failover_max_tries int",
    s"alter table $keyspace.$stateTable add node_failover_failures int",
    s"alter table $keyspace.$stateTable add node_failover_failure_time timestamp"
  )

  override def migrateCassandra(session: Session): Unit = {
    alters.foreach(session.execute)

    val updatePs = session.prepare(s"""
          UPDATE $keyspace.$stateTable
          SET
          cluster_jmx_remote = false,
          node_failover_delay = '3m',
          node_failover_max_delay = '30m',
          node_failover_failures = 0
          WHERE
          namespace = :namespace AND
            framework_id = :framework_id AND
            cluster_id = :cluster_id AND
            node_id = :node_id
        """)

    val selectPs = session.prepare(s"select namespace, framework_id, cluster_id, node_id, nr_of_nodes from $keyspace.$stateTable")

    import CassandraStorage._

    val batch = new BatchStatement()
    batch.setConsistencyLevel(ConsistencyLevel.ONE)
    val rows = session.execute(selectPs.bind()).all().asScala
    for (row <- rows) {
      val update = updatePs.bind()
        .setString(Namespace, row.getString(Namespace))
        .setString(FrameworkId, row.getString(FrameworkId))
        .setString(ClusterId, row.getString(ClusterId))
        .setString(NodeId, row.getString(NodeId))

      batch add update
    }

    session execute batch
  }
}

