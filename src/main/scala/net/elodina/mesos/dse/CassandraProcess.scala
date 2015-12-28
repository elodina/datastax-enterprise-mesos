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

import java.io._
import java.nio.file._
import java.nio.file.attribute.PosixFileAttributeView
import java.util
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.cassandra.tools.NodeProbe
import org.apache.log4j.Logger
import org.apache.mesos.Protos.TaskInfo
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source
import scala.language.postfixOps

case class CassandraProcess(node: Node, taskInfo: TaskInfo, address: String, env: Map[String, String] = Map.empty) {
  private val logger = Logger.getLogger(this.getClass)

  private val started = new AtomicBoolean(false)
  private[dse] var stopped: Boolean = false

  private var process: Process = null

  def start() {
    if (started.getAndSet(true)) throw new IllegalStateException(s"Process already started")
    logger.info("Starting Cassandra process")

    makeDataDirs()
    redirectCassandraLogs()
    editCassandraConfigs()

    process = startProcess()
  }

  private def startProcess(): Process = {
    var cmd: List[String] = null
    if (Executor.dseDir != null) cmd = List("" + new File(Executor.dseDir, "bin/dse"), "cassandra", "-f")
    else cmd = List("" + new File(Executor.cassandraDir, "bin/cassandra"), "-f")

    val builder: ProcessBuilder = new ProcessBuilder(cmd)
      .redirectOutput(new File(Executor.dir, "cassandra.out"))
      .redirectError(new File(Executor.dir, "cassandra.err"))

    if (node.replaceAddress != null)
      builder.environment().put("JVM_OPTS", s"-Dcassandra.replace_address=${node.replaceAddress}")

    builder.environment().putAll(env)
    builder.start()
  }

  def awaitConsistentState(): Boolean = {
    while (!stopped) {
      try {
        val probe = new NodeProbe("localhost", node.runtime.reservation.ports(Node.Port.JMX))

        val initialized = probe.isInitialized
        val joined = probe.isJoined
        val starting = probe.isStarting
        val joiningNodes = probe.getJoiningNodes.toList
        val movingNodes = probe.getMovingNodes.toList
        val leavingNodes = probe.getMovingNodes.toList
        val operationMode = probe.getOperationMode

        if (initialized && joined && !starting) {
          if (joiningNodes.nonEmpty) logger.info(s"Node is live but there are joining nodes $joiningNodes, waiting...")
          else if (movingNodes.nonEmpty) logger.info(s"Node is live but there are moving nodes $movingNodes, waiting...")
          else if (leavingNodes.nonEmpty) logger.info(s"Node is live but there are leaving nodes $leavingNodes, waiting...")
          else if (operationMode != "NORMAL") logger.info(s"Node is live but its operation mode is $operationMode, waiting...")
          else {
            logger.info("Node jumped to normal state")
            return true
          }
        } else logger.info(s"Node is live but still initializing, joining or starting. Initialized: $initialized, Joined: $joined, Started: ${!starting}. Retrying...")
      } catch {
        case e: IOException =>
          logger.debug("Failed to connect via JMX, retrying...")
          logger.trace("", e)
      }

      Thread.sleep(5000)
    }

    false
  }

  def await(): String = {
    val code = process.waitFor()
    if ((code == 0 || code == 143) && stopped) null
    else s"exitCode=$code"
  }

  def stop() {
    this.synchronized {
      if (!stopped) {
        logger.info(s"Stopping Cassandra process")

        stopped = true
        process.destroy()
      }
    }
  }

  private def makeDataDirs() {
    if (node.dataFileDirs == null) node.dataFileDirs = "" + new File(Executor.dir, "data/storage")
    if (node.commitLogDir == null) node.commitLogDir = "" + new File(Executor.dir, "data/commit_log")
    if (node.savedCachesDir == null) node.savedCachesDir = "" + new File(Executor.dir, "data/saved_caches")

    makeDir(new File(Executor.dir, "data/log"))
    node.dataFileDirs.split(",").foreach(dir => makeDir(new File(dir)))
    makeDir(new File(node.commitLogDir))
    makeDir(new File(node.savedCachesDir))
  }

  private def makeDir(dir: File) {
    dir.mkdirs()
    val userPrincipal = FileSystems.getDefault.getUserPrincipalLookupService.lookupPrincipalByName(System.getProperty("user.name"))
    Files.getFileAttributeView(dir.toPath, classOf[PosixFileAttributeView], LinkOption.NOFOLLOW_LINKS).setOwner(userPrincipal)
  }

  private def redirectCassandraLogs() {
    if (Executor.cassandraDir != null)
      Util.IO.replaceInFile(new File(Executor.cassandraDir, "bin/cassandra"), Map("(.*)-Dcassandra.logdir=\\$CASSANDRA_HOME/logs" -> s"$$1-Dcassandra.logdir=${Executor.dir}/data/log"))
    else
      Util.IO.replaceInFile(new File(Executor.dseDir, "bin/dse.in.sh"), Map("CASSANDRA_LOG_DIR=.*" -> s"CASSANDRA_LOG_DIR=${Executor.dir}/data/log"))
  }

  private def editCassandraConfigs() {
    val confDir = if (Executor.dseDir != null) new File(Executor.dseDir, "resources/cassandra/conf") else new File(Executor.cassandraDir, "conf")

    editCassandraYaml(new File(confDir , "cassandra.yaml"))
    Util.IO.replaceInFile(new File(confDir, "cassandra-rackdc.properties"), Map("dc=.*" -> s"dc=${node.dc}", "rack=.*" -> s"rack=${node.rack}"))
    Util.IO.replaceInFile(new File(confDir, "cassandra-env.sh"), Map("JMX_PORT=.*" -> s"JMX_PORT=${node.runtime.reservation.ports(Node.Port.JMX)}"))
  }

  private def editCassandraYaml(file: File) {
    val yaml = new Yaml()
    val cassandraYaml = mutable.Map(yaml.load(Source.fromFile(file).reader()).asInstanceOf[util.Map[String, AnyRef]].toSeq: _*)

    cassandraYaml.put("cluster_name", node.cluster.id)
    cassandraYaml.put("data_file_directories", node.dataFileDirs.split(","))
    cassandraYaml.put("commitlog_directory", Array(node.commitLogDir))
    cassandraYaml.put("saved_caches_directory", Array(node.savedCachesDir))
    cassandraYaml.put("listen_address", address)
    cassandraYaml.put("rpc_address", address)

    val portKeys = Map("internal" -> "storage_port", "cql" -> "native_transport_port", "thrift" -> "rpc_port")
    for ((port, value) <- node.runtime.reservation.ports)
      if (portKeys.contains("" + port))
        cassandraYaml.put(portKeys("" + port), value.asInstanceOf[AnyRef])

    setSeeds(cassandraYaml, if (!node.runtime.seeds.isEmpty) node.runtime.seeds.mkString(",") else address)
    cassandraYaml.put("broadcast_address", address)
    cassandraYaml.put("endpoint_snitch", "GossipingPropertyFileSnitch")

    val writer = new FileWriter(file)
    try { yaml.dump(mapAsJavaMap(cassandraYaml), writer)}
    finally { writer.close() }
  }

  private def setSeeds(cassandraYaml: mutable.Map[String, AnyRef], seeds: String) {
    val seedProviders = cassandraYaml("seed_provider").asInstanceOf[util.List[AnyRef]].toList
    seedProviders.foreach { rawSeedProvider =>
      val seedProvider = rawSeedProvider.asInstanceOf[util.Map[String, AnyRef]].toMap
      val parameters = seedProvider("parameters").asInstanceOf[util.List[AnyRef]].toList
      parameters.foreach { param =>
        val paramMap = param.asInstanceOf[util.Map[String, AnyRef]]
        paramMap.put("seeds", seeds)
      }
    }
  }
}
