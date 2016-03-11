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

trait Storage {
  def save()

  /**
   * Load the state of the framework into Nodes.
   * @return true if there was a state to load (file/znode existed, db tables weren't empty)
   */
  def load(): Boolean

  def close(): Unit = {
    // default is no-op
  }
}

case class FileStorage(file: File) extends Storage {
  override def save() {
    val json = Nodes.toJson
    Util.IO.writeFile(file, json.toString())
  }

  override def load(): Boolean = {
    if (file.exists()) {
      migrate
      val json = Util.IO.readFile(file)
      Nodes.fromJson(Util.parseJsonAsMap(json))
      true
    } else false
  }

  private[dse] def migrate: Unit = {
    if (file.exists()) {
      var schemaVersion: Version = null
      withJson { json =>
        val versioned = if (!json.contains("version")) json.updated("version", "0.2.1.2") else json
        schemaVersion = new Version(versioned("version").asInstanceOf[String])
        versioned
      }

      def updateVersion(v: Version): Unit = withJson { json => json.updated("version", v.toString) }

      var json: Map[String, Any] = readJson
      Migration.migrate(schemaVersion, Scheduler.version, Migration.migrations, v => (), m => json = m.migrateJson(json))
      writeJson(json)

      updateVersion(Scheduler.version)
    }
  }

  def readJson: Map[String, Any] =
    if (file.exists()) Util.parseJsonAsMap(Util.IO.readFile(file))
    else Map.empty[String, Any]

  def writeJson(json: Map[String, Any]): Unit = {
    Util.IO.writeFile(file, new JSONObject(json).toString(Util.jsonFormatter))
  }

  def withJson(f: Map[String, Any] => Map[String, Any]): Unit = {
    if (file.exists()) Util.IO.writeFile(file, new JSONObject(f(readJson)).toString(Util.jsonFormatter))
  }
}

case class ZkStorage[T](zk: String) extends Storage {
  val (zkConnect, path) = zk.span(_ != '/')
  createChrootIfRequired()

  override def save() {
    withClient { client => write(client, Nodes.toJson.toString().getBytes("utf-8")) }
  }

  override def load(): Boolean = {
    migrate

    withClient { client =>
      val json = readJson(client)
      if (json.nonEmpty) {
        Nodes.fromJson(json)
        true
      } else false
    }
  }

  private[dse] def migrate: Unit = {
    withClient { client =>
      var schemaVersion: Version = null
      withJson(client) { json =>
        val versioned = if (!json.contains("version")) json.updated("version", "0.2.1.2") else json
        schemaVersion = new Version(versioned("version").asInstanceOf[String])
        versioned
      }

      def updateVersion(v: Version) = withJson(client) { json => json.updated("version", v.toString) }

      var json = readJson(client)
      Migration.migrate(schemaVersion, Scheduler.version, Migration.migrations, v => (), m => json = m.migrateJson(json))
      writeJson(client, json)

      updateVersion(Scheduler.version)
    }
  }

  private def createChrootIfRequired(): Unit = if (path != "") withClient(_.createPersistent(path, true))

  private[dse] def zkClient: ZkClient = new ZkClient(zkConnect, 30000, 30000, new BytesPushThroughSerializer)

  private[dse] def withClient[T](f: ZkClient => T): T = {
    val c = zkClient
    try { f(c) } finally { c.close() }
  }

  private def write(client: ZkClient, bytes: Array[Byte]) {
    try {
      client.createPersistent(path, bytes)
    } catch {
      case e: ZkNodeExistsException => client.writeData(path, bytes)
    }
  }

  private def readJson(client: ZkClient): Map[String, Any] = {
    val bytes: Array[Byte] = client.readData(path, true).asInstanceOf[Array[Byte]]
    if (bytes != null) Util.parseJsonAsMap(new String(bytes, "utf-8"))
    else Map.empty[String, Any]
  }

  private def writeJson(client: ZkClient, json: Map[String, Any]): Unit = {
    write(client, new JSONObject(json).toString(Util.jsonFormatter).getBytes("utf-8"))
  }

  private def withJson(client: ZkClient)(f: Map[String, Any] => Map[String, Any]): Unit = writeJson(client, f(readJson(client)))
}

