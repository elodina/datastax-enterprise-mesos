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
  var ring: Ring = Cluster.defaultRing
  var runtime: Node.Runtime = null

  var cpu: Double = 0.5
  var mem: Long = 512

  var broadcast: String = null
  var clusterName: String = null
  var seed: Boolean = false
  var replaceAddress: String = null

  var seeds: String = ""
  var constraints: mutable.Map[String, List[Constraint]] = new mutable.HashMap[String, List[Constraint]]
  var seedConstraints: mutable.Map[String, List[Constraint]] = new mutable.HashMap[String, List[Constraint]]

  var dataFileDirs: String = null
  var commitLogDir: String = null
  var savedCachesDir: String = null

  var storagePort = 7000
  var sslStoragePort = 7001
  var jmxPort = 7199
  var nativeTransportPort = 9042
  var rpcPort = 9160

  def this(id: String) = {
    this
    this.id = id
  }

  def this(json: Map[String, Any], expanded: Boolean = false) = {
    this
    fromJson(json, expanded)
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
          if (!ranges.exists(r => r contains port._2)) {
            return Some(s"unavailable port $port")
          }
        }
      case None => return Some("no ports")
    }

    None
  }

  def ports: Map[String, Int] =
    Map(
      DSEProcess.STORAGE_PORT -> storagePort,
      DSEProcess.SSL_STORAGE_PORT -> sslStoragePort,
      DSEProcess.JMX_PORT -> jmxPort,
      DSEProcess.NATIVE_TRANSPORT_PORT -> nativeTransportPort,
      DSEProcess.RPC_PORT -> rpcPort
    )

  def createTaskInfo(offer: Offer): TaskInfo = {
    val name = s"node-${this.id}"
    val id = s"$name-${System.currentTimeMillis()}"

    Scheduler.setSeedNodes(this, offer.getHostname)
    TaskInfo.newBuilder().setName(name).setTaskId(TaskID.newBuilder().setValue(id).build()).setSlaveId(offer.getSlaveId)
      .setExecutor(createExecutorInfo(name))
      .setData(ByteString.copyFromUtf8("" + this.toJson(expanded = true)))
      .addResources(Protos.Resource.newBuilder().setName("cpus").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(this.cpu)))
      .addResources(Protos.Resource.newBuilder().setName("mem").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(this.mem)))
      .addResources(
        Protos.Resource.newBuilder()
          .setName("ports")
          .setType(Protos.Value.Type.RANGES)
          .setRanges(
            Protos.Value.Ranges.newBuilder()
              .addAllRange(
                ports.values.toSeq.map { port => Protos.Value.Range.newBuilder().setBegin(port.toLong).setEnd(port.toLong).build()}
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

  def fromJson(json: Map[String, Any], expanded: Boolean = false): Unit = {
    id = json("id").asInstanceOf[String]
    state = Node.State.withName(json("state").asInstanceOf[String])
    ring = if (expanded) new Ring(json("ring").asInstanceOf[Map[String, Any]]) else Cluster.getRing(json("ring").asInstanceOf[String])
    if (json.contains("runtime")) runtime = new Node.Runtime(json("runtime").asInstanceOf[Map[String, Any]])

    cpu = json("cpu").asInstanceOf[Number].doubleValue()
    mem = json("mem").asInstanceOf[Number].longValue()

    if (json.contains("broadcast")) broadcast = json("broadcast").asInstanceOf[String]
    if (json.contains("clusterName")) clusterName = json("clusterName").asInstanceOf[String]
    seed = json("seed").asInstanceOf[Boolean]
    if (json.contains("replaceAddress")) replaceAddress = json("replaceAddress").asInstanceOf[String]
    seeds = json("seeds").asInstanceOf[String]

    constraints.clear()
    if (json.contains("constraints")) constraints ++= Constraint.parse(json("constraints").asInstanceOf[String])

    seedConstraints.clear()
    if (json.contains("seedConstraints")) seedConstraints ++= Constraint.parse(json("seedConstraints").asInstanceOf[String])

    if (json.contains("dataFileDirs")) dataFileDirs = json("dataFileDirs").asInstanceOf[String]
    if (json.contains("commitLogDir")) commitLogDir = json("commitLogDir").asInstanceOf[String]
    if (json.contains("savedCachesDir")) savedCachesDir = json("savedCachesDir").asInstanceOf[String]
  }

  def toJson(expanded: Boolean = false): JSONObject = {
    val json = new mutable.LinkedHashMap[String, Any]()

    json("id") = id
    json("state") = "" + state
    json("ring") = if (expanded) ring.toJson else ring.id
    if (runtime != null) json("runtime") = runtime.toJson

    json("cpu") = cpu
    json("mem") = mem

    if (broadcast != null) json("broadcast") = broadcast
    if (clusterName != null) json("clusterName") = clusterName
    json("seed") = seed
    if (replaceAddress != null) json("replaceAddress") = replaceAddress
    json("seeds") = seeds

    if (!constraints.isEmpty) json("constraints") = Util.formatConstraints(constraints)
    if (!seedConstraints.isEmpty) json("seedConstraints") = Util.formatConstraints(seedConstraints)

    if (dataFileDirs != null) json("dataFileDirs") = dataFileDirs
    if (commitLogDir != null) json("commitLogDir") = commitLogDir
    if (savedCachesDir != null) json("savedCachesDir") = savedCachesDir

    new JSONObject(json.toMap)
  }

  override def hashCode(): Int = id.hashCode

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[Node]) false
    id == obj.asInstanceOf[Node].id
  }
}

object Node {
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

    def this(taskId: String = null, slaveId: String = null, executorId: String = null, hostname: String = null, attributes: Map[String, String] = Map()) {
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
