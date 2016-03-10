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

import java.io.File
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer
import net.elodina.mesos.dse.Util.Version
import scala.util.parsing.json.JSONObject

trait Migration {
  val version: Version
  def apply(): Unit
}

trait Migrator {
  def migrate: Unit
}

object Migrator {
  import org.apache.log4j.Logger
  private val logger = Logger.getLogger(Migrator.getClass)

  def migrate(from: Version, to: Version, migrations: Seq[Migration], updateVersion: Version => Unit): Unit = {
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
      m()
      updateVersion(m.version)
    }
    logger.info(s"migration from $from to $to completed")
  }
}

class FileStorageMigrator(file: File) extends Migrator {
  def migrate = {
    if (file.exists()) {
      var schemaVersion: Version = null
      withJson { json =>
        val versioned = if (!json.contains("version")) json.updated("version", "0.2.1.2") else json
        schemaVersion = new Version(versioned("version").asInstanceOf[String])
        versioned
      }

      val migrations = Seq(
        mkMigration("0.2.1.3")(JSONMigrations.v0_2_1_3)
      )

      def updateVersion(v: Version): Unit = withJson { json => json.updated("version", v.toString) }

      Migrator.migrate(schemaVersion, Scheduler.version, migrations, updateVersion)

      updateVersion(Scheduler.version)
    }
  }

  def withJson(f: Map[String, Any] => Map[String, Any]): Unit = {
    if (file.exists()) {
      val content = Util.IO.readFile(file)
      val json = Util.parseJsonAsMap(content)
      Util.IO.writeFile(file, new JSONObject(f(json)).toString(Util.jsonFormatter))
    }
  }

  def mkMigration(v: String)(f: Map[String, Any] => Map[String, Any]): Migration = new Migration {
    override val version: Version = new Version(v)

    override def apply(): Unit = withJson(f)
  }
}

object JSONMigrations {
  def v0_2_1_3(json: Map[String, Any]): Map[String, Any] = {
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
}

trait ZkMigratorHelpers {
  val path: String

  def withJson(client: ZkClient)(f: Map[String, Any] => Map[String, Any]) {
    val json = {
      val bytes: Array[Byte] = client.readData(path, true).asInstanceOf[Array[Byte]]
      if (bytes != null) Util.parseJsonAsMap(new String(bytes, "utf-8"))
      else Map.empty[String, Any]
    }

    val json1 = f(json)

    val encoded = new JSONObject(json1).toString(Util.jsonFormatter).getBytes("utf-8")
    try {
      client.createPersistent(path, encoded)
    } catch {
      case e: ZkNodeExistsException => client.writeData(path, encoded)
    }
  }
}

class ZkStorageMigrator(zk: String) extends Migrator with ZkMigratorHelpers {
  val (zkConnect, path) = zk.span(_ != '/')

  override def migrate: Unit = {
    withClient { client =>
      if (path != "") client.createPersistent(path, true)

      var schemaVersion: Version = null
      withJson(client) { json =>
        val versioned = if (!json.contains("version")) json.updated("version", "0.2.1.2") else json
        schemaVersion = new Version(versioned("version").asInstanceOf[String])
        versioned
      }

      val migrations = Seq(
        new ZkMigraiton0_2_1_3(client, path)
      )

      def updateVersion(v: Version) = withJson(client) { json => json.updated("version", v.toString) }

      Migrator.migrate(schemaVersion, Scheduler.version, migrations, updateVersion)

      updateVersion(Scheduler.version)
    }
  }

  private[dse] def withClient[T](f: ZkClient => T): T = {
    val c = zkClient
    try { f(c) } finally { c.close() }
  }

  private[dse] def zkClient: ZkClient = new ZkClient(zkConnect, 30000, 30000, new BytesPushThroughSerializer)
}

class ZkMigraiton0_2_1_3(client: ZkClient, override val path: String) extends Migration with ZkMigratorHelpers {
  override val version: Version = new Version("0.2.1.3")

  override def apply(): Unit = {
    withJson(client)(JSONMigrations.v0_2_1_3)
  }
}

