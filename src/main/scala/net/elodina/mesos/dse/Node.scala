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

import com.google.protobuf.ByteString
import net.elodina.mesos.dse.cli.AddOptions
import org.apache.mesos.Protos
import org.apache.mesos.Protos._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.parsing.json.JSONObject

class Node extends Constrained {
  var id: String = null
  var state: Node.State.Value = Node.State.Inactive
  var runtime: Node.Runtime = null

  var cpu: Double = 0.5
  var mem: Long = 512
  var broadcast: String = ""
  var nodeOut: String = ""
  var agentOut: String = ""
  var clusterName: String = ""
  var seed: Boolean = false
  var replaceAddress: String = ""
  var seeds: String = ""
  var constraints: mutable.Map[String, List[Constraint]] = new mutable.HashMap[String, List[Constraint]]
  var seedConstraints: mutable.Map[String, List[Constraint]] = new mutable.HashMap[String, List[Constraint]]

  var dataFileDirs: String = ""
  var commitLogDir: String = ""
  var savedCachesDir: String = ""
  var awaitConsistentStateBackoff: Duration = Duration("3 seconds")

  var storagePort = 31000
  var sslStoragePort = 31001
  var jmxPort = 31099
  var nativeTransportPort = 31042
  var rpcPort = 31060

  def this(id: String) = {
    this
    this.id = id
  }

  def this(json: Map[String, Any]) = {
    this
    fromJson(json)
  }

  override def attribute(name: String): Option[String] = {
    if (runtime == null) return None
    if (name == "hostname") Some(runtime.hostname)
    else Some(runtime.attributes(name))
  }

  def matches(offer: Offer): Option[String] = {
    val offerResources = offer.getResourcesList.toList.map(res => res.getName -> res).toMap

    offerResources.get("cpus") match {
      case Some(cpusResource) => if (cpusResource.getScalar.getValue < cpu) return Some(s"cpus ${cpusResource.getScalar.getValue} < $cpu")
      case None => return Some("no cpus")
    }

    offerResources.get("mem") match {
      case Some(memResource) => if (memResource.getScalar.getValue.toLong < mem) return Some(s"mem ${memResource.getScalar.getValue.toLong} < $mem")
      case None => return Some("no mem")
    }

    offerResources.get("ports") match {
      case Some(portsResource) =>
        val ranges = portsResource
          .getRanges
          .getRangeList
          .map(r => Range.inclusive(r.getBegin.toInt, r.getEnd.toInt))
          .sortBy(_.start)

        for (port <- ports) {
          if (!ranges.exists(r => r contains port)) {
            return Some(s"unavailable port $port")
          }
        }
      case None => return Some("no ports")
    }

    None
  }

  def ports: Seq[Int] = {
    Seq(storagePort, sslStoragePort, jmxPort, nativeTransportPort, rpcPort)
  }

  def portMappings: Map[String, Int] =
    Map(
      Node.STORAGE_PORT -> storagePort,
      Node.SSL_STORAGE_PORT -> sslStoragePort,
      Node.JMX_PORT -> jmxPort,
      Node.NATIVE_TRANSPORT_PORT -> nativeTransportPort,
      Node.RPC_PORT -> rpcPort
    )

  def createTaskInfo(offer: Offer): TaskInfo = {
    val name = s"node-${this.id}"
    val id = s"$name-${System.currentTimeMillis()}"

    Scheduler.setSeedNodes(this, offer.getHostname)
    TaskInfo.newBuilder().setName(name).setTaskId(TaskID.newBuilder().setValue(id).build()).setSlaveId(offer.getSlaveId)
      .setExecutor(createExecutorInfo(name))
      .setData(ByteString.copyFromUtf8("" + this.toJson))
      .addResources(Protos.Resource.newBuilder().setName("cpus").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(this.cpu)))
      .addResources(Protos.Resource.newBuilder().setName("mem").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(this.mem)))
      .addResources(
        Protos.Resource.newBuilder()
          .setName("ports")
          .setType(Protos.Value.Type.RANGES)
          .setRanges(
            Protos.Value.Ranges.newBuilder()
              .addAllRange(
                ports.map { port => Protos.Value.Range.newBuilder().setBegin(port.toLong).setEnd(port.toLong).build()}
              )
              .build()
          )
      )
      .build()
  }

  private def createExecutorInfo(name: String): ExecutorInfo = {
    var java = "java"
    val commandBuilder = CommandInfo.newBuilder()
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/dse/" + Config.dse.getName))

    if (Config.jre != null) {
      commandBuilder.addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/jre/" + Config.jre.getName))
      java = "$(find jre* -maxdepth 0 -type d)/bin/java"
    }

    commandBuilder
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/jar/" + Config.jar.getName))
      .setValue(s"$java -cp ${Config.jar.getName}${if (Config.debug) " -Ddebug" else ""} net.elodina.mesos.dse.Executor")

    ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder().setValue(s"$name-${System.currentTimeMillis()}"))
      .setCommand(commandBuilder)
      .setName(name)
      .build
  }

  def waitFor(state: Node.State.Value, timeout: Duration): Boolean = {
    var t = timeout.toMillis
    while (t > 0 && this.state != state) {
      val delay = Math.min(100, t)
      Thread.sleep(delay)
      t -= delay
    }

    this.state == state
  }

  def fromJson(json: Map[String, Any]): Unit = {
    id = json("id").asInstanceOf[String]
    state = Node.State.withName(json("state").asInstanceOf[String])
    if (json.contains("runtime")) runtime = new Node.Runtime(json("runtime").asInstanceOf[Map[String, Any]])

    cpu = json("cpu").asInstanceOf[Number].doubleValue()
    mem = json("mem").asInstanceOf[Number].longValue()

    broadcast = json("broadcast").asInstanceOf[String]
    nodeOut = json("nodeOut").asInstanceOf[String]
    agentOut = json("agentOut").asInstanceOf[String]

    clusterName = json("clusterName").asInstanceOf[String]
    seed = json("seed").asInstanceOf[Boolean]
    replaceAddress = json("replaceAddress").asInstanceOf[String]
    seeds = json("seeds").asInstanceOf[String]

    constraints.clear()
    constraints ++= Constraint.parse(json("constraints").asInstanceOf[String])

    seedConstraints.clear()
    seedConstraints ++= Constraint.parse(json("seedConstraints").asInstanceOf[String])

    dataFileDirs = json("dataFileDirs").asInstanceOf[String]
    commitLogDir = json("commitLogDir").asInstanceOf[String]
    savedCachesDir = json("savedCachesDir").asInstanceOf[String]
    awaitConsistentStateBackoff = Duration.create(json("awaitConsistentStateBackoff").asInstanceOf[String])
  }

  def toJson: JSONObject = {
    val json = new mutable.LinkedHashMap[String, Any]()

    json("id") = id
    json("state") = "" + state
    if (runtime != null) json("runtime") = runtime.toJson

    json("cpu") = cpu
    json("mem") = mem

    json("broadcast") = broadcast
    json("nodeOut") = nodeOut
    json("agentOut") = agentOut

    json("clusterName") = clusterName
    json("seed") = seed
    json("replaceAddress") = replaceAddress
    json("seeds") = seeds

    json("constraints") = Util.formatConstraints(constraints)
    json("seedConstraints") = Util.formatConstraints(seedConstraints)

    json("dataFileDirs") = dataFileDirs
    json("commitLogDir") = commitLogDir
    json("savedCachesDir") = savedCachesDir
    json("awaitConsistentStateBackoff") = "" + awaitConsistentStateBackoff

    new JSONObject(json.toMap)
  }

  override def hashCode(): Int = id.hashCode

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[Node]) false
    id == obj.asInstanceOf[Node].id
  }
}

object Node {
  val STORAGE_PORT: String = "storage_port"
  val SSL_STORAGE_PORT: String = "ssl_storage_port"
  val NATIVE_TRANSPORT_PORT: String = "native_transport_port"
  val RPC_PORT: String = "rpc_port"
  val JMX_PORT: String = "jmx_port"

  val defaultPortMappings: Map[String, Int] = Map(
    STORAGE_PORT -> 31000,
    SSL_STORAGE_PORT -> 31001,
    JMX_PORT -> 31099,
    NATIVE_TRANSPORT_PORT -> 31042,
    RPC_PORT -> 31060
  )

  def apply(id: String, opts: AddOptions): Node = {
    val node = new Node(id)

    node.cpu = opts.cpu
    node.mem = opts.mem
    node.broadcast = opts.broadcast
    Constraint.parse(opts.constraints).foreach(node.constraints +=)
    Constraint.parse(opts.seedConstraints).foreach(node.seedConstraints +=)
    node.nodeOut = opts.nodeOut
    node.agentOut = opts.agentOut
    node.clusterName = opts.clusterName
    node.seed = opts.seed
    node.replaceAddress = opts.replaceAddress
    node.dataFileDirs = opts.dataFileDirs
    node.commitLogDir = opts.commitLogDir
    node.savedCachesDir = opts.savedCachesDir
    node.awaitConsistentStateBackoff = opts.awaitConsistentStateBackoff

    node
  }

  def idFromTaskId(taskId: String): String = {
    taskId.split("-", 3) match {
      case Array(_, value, _) => value
      case _ => throw new IllegalArgumentException(taskId)
    }
  }

  object State extends Enumeration {
    val Inactive = Value("Inactive")
    val Stopped = Value("Stopped")
    val Staging = Value("Staging")
    val Starting = Value("Starting")
    val Running = Value("Running")
    val Reconciling = Value("Reconciling")
  }

  class Runtime() {
    var taskId: String = null
    var slaveId: String = null
    var executorId: String = null

    var hostname: String = null
    var attributes: Map[String, String] = null

    def this(taskId: String, slaveId: String, executorId: String, hostname: String, attributes: Map[String, String]) {
      this
      this.taskId = taskId
      this.slaveId = slaveId
      this.executorId = executorId

      this.hostname = hostname
      this.attributes = attributes
    }

    def this(info: TaskInfo, offer: Offer) = {
      this(
        info.getTaskId.getValue, info.getSlaveId.getValue, info.getExecutor.getExecutorId.getValue,
        offer.getHostname, offer.getAttributesList.toList.filter(_.hasText).map(attr => attr.getName -> attr.getText.getValue).toMap
      )
    }

    def this(json: Map[String, Any]) = {
      this
      fromJson(json)
    }

    def fromJson(json: Map[String, Any]): Unit = {
      taskId = json("taskId").asInstanceOf[String]
      slaveId = json("slaveId").asInstanceOf[String]
      executorId = json("executorId").asInstanceOf[String]

      hostname = json("hostname").asInstanceOf[String]
      attributes = json("attributes").asInstanceOf[Map[String, String]]
    }

    def toJson: JSONObject = {
      val json = new mutable.LinkedHashMap[String, Any]()
      json("taskId") = taskId
      json("slaveId") = slaveId
      json("executorId") = executorId

      json("hostname") = hostname
      json("attributes") = new JSONObject(attributes)
      new JSONObject(json.toMap)
    }
  }
}
