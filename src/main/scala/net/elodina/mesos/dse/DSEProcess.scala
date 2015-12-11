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
import java.util.Properties

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

    editCassandraYaml(new File(dseDir, "resources/cassandra/conf/cassandra.yaml"))
    editCassandraEnvSh(new File(dseDir, "resources/cassandra/conf/cassandra-env.sh"))
    editRackDcProps(new File(dseDir, "resources/cassandra/conf/cassandra-rackdc.properties"))

    process = startProcess(node, dseDir)
  }

  private def startProcess(node: Node, dseDir: File): Process = {
    val cmd = util.Arrays.asList("" + new File(dseDir, "bin/dse"), "cassandra", "-f")

    val out: File = new File("dse.log")
    val builder: ProcessBuilder = new ProcessBuilder(cmd)
      .redirectOutput(out)
      .redirectError(out)

    if (node.replaceAddress != null)
      builder.environment().put("JVM_OPTS", s"-Dcassandra.replace_address=${node.replaceAddress}")

    builder.environment().putAll(env)
    builder.start()
  }

  def awaitConsistentState(): Boolean = {
    while (!stopped) {
      try {
        val probe = new NodeProbe("localhost", node.runtime.reservation.ports("jmx"))

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
    makeDir(new File(currentDir, "lib/cassandra")) //TODO Cassandra/Spark lib/log dirs look unnecessary, remove them a bit later
    makeDir(new File(currentDir, "log/cassandra"))
    makeDir(new File(currentDir, "lib/spark"))
    makeDir(new File(currentDir, "log/spark"))

    if (node.dataFileDirs == null) node.dataFileDirs = "" + new File(currentDir, "dse-data")
    if (node.commitLogDir == null) node.commitLogDir = "" + new File(currentDir, "dse-data/commitlog")
    if (node.savedCachesDir == null) node.savedCachesDir = "" + new File(currentDir, "dse-data/saved_caches")

    node.dataFileDirs.split(",").foreach(dir => makeDir(new File(dir)))
    makeDir(new File(node.commitLogDir))
    makeDir(new File(node.savedCachesDir))
  }

  private def makeDir(dir: File) {
    dir.mkdirs()
    val userPrincipal = FileSystems.getDefault.getUserPrincipalLookupService.lookupPrincipalByName(System.getProperty("user.name"))
    Files.getFileAttributeView(dir.toPath, classOf[PosixFileAttributeView], LinkOption.NOFOLLOW_LINKS).setOwner(userPrincipal)
  }

  final private val PORT_KEYS = Map("storage" -> "storage_port", "native" -> "native_transport_port", "rpc" -> "rpc_port")

  private def editCassandraYaml(file: File) {
    val yaml = new Yaml()
    val cassandraYaml = mutable.Map(yaml.load(Source.fromFile(file).reader()).asInstanceOf[util.Map[String, AnyRef]].toSeq: _*)

    cassandraYaml.put("cluster_name", if (node.ring.name != null) node.ring.name else node.ring.id)
    cassandraYaml.put("data_file_directories", node.dataFileDirs.split(","))
    cassandraYaml.put("commitlog_directory", Array(node.commitLogDir))
    cassandraYaml.put("saved_caches_directory", Array(node.savedCachesDir))
    cassandraYaml.put("listen_address", hostname)
    cassandraYaml.put("rpc_address", hostname)

    for ((key, port) <- node.runtime.reservation.ports)
      if (PORT_KEYS.contains(key))
        cassandraYaml.put(PORT_KEYS(key), port.asInstanceOf[AnyRef])

    setSeeds(cassandraYaml, node.runtime.seeds.mkString(","))
    if (node.broadcast != null) {
      val ip = getIP(node.broadcast)
      cassandraYaml.put("broadcast_address", ip)
    }

    cassandraYaml.put("endpoint_snitch", "GossipingPropertyFileSnitch")

    val writer = new FileWriter(file)
    try { yaml.dump(mapAsJavaMap(cassandraYaml), writer)}
    finally { writer.close() }
  }

  private def editCassandraEnvSh(file: File) {
    val buffer = new ByteArrayOutputStream()
    Util.copyAndClose(new FileInputStream(file), buffer)

    var content = buffer.toString("utf-8")
    content = content.replaceAll("JMX_PORT=.*", "JMX_PORT=" + node.runtime.reservation.ports("jmx"))

    Util.copyAndClose(new ByteArrayInputStream(content.getBytes("utf-8")), new FileOutputStream(file))
  }

  private def editRackDcProps(file: File) {
    val props = new Properties()
    props.put("dc", node.dc)
    props.put("rack", node.rack)

    val writer = new FileWriter(file)
    try { props.store(writer, "") }
    finally { writer.close() }
  }

  private def getIP(networkInterface: String): String = {
    val iface = NetworkInterface.getByName(networkInterface)
    if (iface == null) throw new IllegalArgumentException(s"Unknown network interface $networkInterface")

    val enumeration = iface.getInetAddresses
    if (!enumeration.hasMoreElements) throw new IllegalArgumentException(s"Network interface $networkInterface does not have any IP address assigned to it")

    enumeration.nextElement().getHostAddress
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

object DSEProcess {
  private[dse] def findDSEDir(): File = {
    for (file <- new File(".").listFiles()) {
      if (file.isDirectory && file.getName.matches(Config.dseDirMask) && file.getName != "dse-data")
        return file
    }

    throw new FileNotFoundException(s"${Config.dseDirMask} not found in current directory")
  }
}
