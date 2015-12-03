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

import java.io.{File, FileNotFoundException, FileWriter, IOException}
import java.net.NetworkInterface
import java.nio.file._
import java.nio.file.attribute.PosixFileAttributeView
import java.util
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.cassandra.tools.NodeProbe
import org.apache.log4j.Logger
import org.apache.mesos.ExecutorDriver
import org.apache.mesos.Protos.TaskInfo
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source
import scala.language.postfixOps

case class DSEProcess(node: Node, driver: ExecutorDriver, taskInfo: TaskInfo, hostname: String, env: Map[String, String] = Map.empty) {
  private val logger = Logger.getLogger(this.getClass)

  private val started = new AtomicBoolean(false)
  private[dse] var stopped: Boolean = false

  private var process: Process = null

  def start() {
    if (started.getAndSet(true)) throw new IllegalStateException(s"Process ${node.id} already started")
    logger.info(s"Starting process ${node.id}")

    val dseDir = DSEProcess.findDSEDir()
    val workDir = new File(".")
    makeDseDirs(workDir)
    editCassandraYaml(new File(dseDir, DSEProcess.CASSANDRA_YAML_LOCATION))

    process = startProcess(node, dseDir)
  }

  private def startProcess(node: Node, dseDir: File): Process = {
    val cmd = util.Arrays.asList("" + new File(dseDir, DSEProcess.DSE_CMD), "cassandra", "-f")

    val builder: ProcessBuilder = new ProcessBuilder(cmd)
      .redirectOutput(new File(node.nodeOut))
      .redirectError(new File(node.nodeOut))

    if (node.replaceAddress != "")
      builder.environment().put("JVM_OPTS", s"-Dcassandra.replace_address=${node.replaceAddress}")

    builder.environment().putAll(env)
    builder.start()
  }

  def awaitConsistentState(): Boolean = {
    while (!stopped) {
      try {
        val probe = new NodeProbe("localhost", node.jmxPort)

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

      Thread.sleep(node.awaitConsistentStateBackoff.toMillis)
    }

    false
  }

  def await(): Int = {
    process.waitFor()
  }

  def stop() {
    this.synchronized {
      if (!stopped) {
        logger.info(s"Stopping process ${node.id}")

        stopped = true
        process.destroy()
      }
    }
  }

  private def makeDseDirs(currentDir: File) {
    makeDir(new File(currentDir, DSEProcess.CASSANDRA_LIB_DIR)) //TODO Cassandra/Spark lib/log dirs look unnecessary, remove them a bit later
    makeDir(new File(currentDir, DSEProcess.CASSANDRA_LOG_DIR))
    makeDir(new File(currentDir, DSEProcess.SPARK_LIB_DIR))
    makeDir(new File(currentDir, DSEProcess.SPARK_LOG_DIR))

    if (node.dataFileDirs.isEmpty) node.dataFileDirs = "" + new File(currentDir, DSEProcess.DSE_DATA_DIR)
    if (node.commitLogDir.isEmpty) node.commitLogDir = "" + new File(currentDir, DSEProcess.COMMIT_LOG_DIR)
    if (node.savedCachesDir.isEmpty) node.savedCachesDir = "" + new File(currentDir, DSEProcess.SAVED_CACHES_DIR)

    node.dataFileDirs.split(",").foreach(dir => makeDir(new File(dir)))
    makeDir(new File(node.commitLogDir))
    makeDir(new File(node.savedCachesDir))
  }

  private def makeDir(dir: File) {
    dir.mkdirs()
    val userPrincipal = FileSystems.getDefault.getUserPrincipalLookupService.lookupPrincipalByName(System.getProperty("user.name"))
    Files.getFileAttributeView(dir.toPath, classOf[PosixFileAttributeView], LinkOption.NOFOLLOW_LINKS).setOwner(userPrincipal)
  }

  private def editCassandraYaml(file: File) {
    val yaml = new Yaml()
    val cassandraYaml = mutable.Map(yaml.load(Source.fromFile(file).reader()).asInstanceOf[util.Map[String, AnyRef]].toSeq: _*)

    cassandraYaml.put(DSEProcess.CLUSTER_NAME_KEY, node.clusterName)
    cassandraYaml.put(DSEProcess.DATA_FILE_DIRECTORIES_KEY, node.dataFileDirs.split(","))
    cassandraYaml.put(DSEProcess.COMMIT_LOG_DIRECTORY_KEY, Array(node.commitLogDir))
    cassandraYaml.put(DSEProcess.SAVED_CACHES_DIRECTORY_KEY, Array(node.savedCachesDir))
    cassandraYaml.put(DSEProcess.LISTEN_ADDRESS_KEY, hostname)
    cassandraYaml.put(DSEProcess.RPC_ADDRESS_KEY, hostname)

    for ((key, port) <- node.ports if key != DSEProcess.JMX_PORT) {
      cassandraYaml.put(key, port.asInstanceOf[AnyRef])
    }

    setSeeds(cassandraYaml, node.seeds)
    if (node.broadcast != "") {
      val ip = getIP(node.broadcast)
      cassandraYaml.put(DSEProcess.BROADCAST_ADDRESS_KEY, ip)
    }

    val writer = new FileWriter(file)
    try {
      yaml.dump(mapAsJavaMap(cassandraYaml), writer)
    } finally {
      writer.close()
    }
  }

  private def getIP(networkInterface: String): String = {
    val iface = NetworkInterface.getByName(networkInterface)
    if (iface == null) throw new IllegalArgumentException(s"Unknown network interface $networkInterface")

    val enumeration = iface.getInetAddresses
    if (!enumeration.hasMoreElements) throw new IllegalArgumentException(s"Network interface $networkInterface does not have any IP address assigned to it")

    enumeration.nextElement().getHostAddress
  }

  private def setSeeds(cassandraYaml: mutable.Map[String, AnyRef], seeds: String) {
    val seedProviders = cassandraYaml(DSEProcess.SEED_PROVIDER_KEY).asInstanceOf[util.List[AnyRef]].toList
    seedProviders.foreach { rawSeedProvider =>
      val seedProvider = rawSeedProvider.asInstanceOf[util.Map[String, AnyRef]].toMap
      val parameters = seedProvider(DSEProcess.PARAMETERS_KEY).asInstanceOf[util.List[AnyRef]].toList
      parameters.foreach { param =>
        val paramMap = param.asInstanceOf[util.Map[String, AnyRef]]
        paramMap.put(DSEProcess.SEEDS_KEY, seeds)
      }
    }
  }
}

object DSEProcess {
  final val STORAGE_PORT: String = "storage_port"
  final val SSL_STORAGE_PORT: String = "ssl_storage_port"
  final val NATIVE_TRANSPORT_PORT: String = "native_transport_port"
  final val RPC_PORT: String = "rpc_port"
  final val JMX_PORT: String = "jmx_port"

  final private val CASSANDRA_LIB_DIR = "lib/cassandra"
  final private val CASSANDRA_LOG_DIR = "log/cassandra"
  final private val SPARK_LIB_DIR = "lib/spark"
  final private val SPARK_LOG_DIR = "log/spark"
  final private val DSE_DATA_DIR = "dse-data"
  final private val COMMIT_LOG_DIR = "dse-data/commitlog"
  final private val SAVED_CACHES_DIR = "dse-data/saved_caches"

  final private val CASSANDRA_YAML_LOCATION = "resources/cassandra/conf/cassandra.yaml"

  final private val DATA_FILE_DIRECTORIES_KEY = "data_file_directories"
  final private val COMMIT_LOG_DIRECTORY_KEY = "commitlog_directory"
  final private val SAVED_CACHES_DIRECTORY_KEY = "saved_caches_directory"
  final private val LISTEN_ADDRESS_KEY = "listen_address"
  final private val LISTEN_INTERFACE_KEY = "listen_interface"
  final private val SEED_PROVIDER_KEY = "seed_provider"
  final private val PARAMETERS_KEY = "parameters"
  final private val SEEDS_KEY = "seeds"
  final private val RPC_ADDRESS_KEY = "rpc_address"
  final private val RPC_INTERFACE_KEY = "rpc_interface"
  final private val CLUSTER_NAME_KEY = "cluster_name"
  final private val BROADCAST_ADDRESS_KEY = "broadcast_address"

  final private val DSE_CMD = "bin/dse"
  final private[dse] val DSE_AGENT_CMD = "datastax-agent/bin/datastax-agent"

  private[dse] def findDSEDir(): File = {
    for (file <- new File(".").listFiles()) {
      if (file.isDirectory && file.getName.matches(Config.dseDirMask) && file.getName != DSEProcess.DSE_DATA_DIR)
        return file
    }

    throw new FileNotFoundException(s"${Config.dseDirMask} not found in current directory")
  }
}
