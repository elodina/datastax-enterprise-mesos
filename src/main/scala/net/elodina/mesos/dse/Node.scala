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
import org.apache.mesos.Protos.NetworkInfo.IPAddress
import org.apache.mesos.Protos._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.parsing.json.{JSONArray, JSONObject}
import scala.collection.mutable.ListBuffer
import net.elodina.mesos.dse.Util.Range
import net.elodina.mesos.dse.Node.Reservation
import java.util.{TimeZone, Date}
import Util.Period
import java.text.SimpleDateFormat
import Math._
import Util.Size

class Node extends Constrained {
  var id: String = null
  @volatile var state: Node.State.Value = Node.State.IDLE
  var cluster: Cluster = Nodes.defaultCluster
  var stickiness: Node.Stickiness = new Node.Stickiness()
  var runtime: Node.Runtime = null

  var cpu: Double = 0.5
  var mem: Long = 512

  var seed: Boolean = false
  var jvmOptions: String = null

  var rack: String = "default"
  var dc: String = "default"

  var constraints: mutable.Map[String, List[Constraint]] = new mutable.HashMap[String, List[Constraint]]
  var seedConstraints: mutable.Map[String, List[Constraint]] = new mutable.HashMap[String, List[Constraint]]

  var dataFileDirs: String = null
  var commitLogDir: String = null
  var savedCachesDir: String = null

  var cassandraDotYaml: mutable.Map[String, String] = new mutable.HashMap[String, String]
  var addressDotYaml: mutable.Map[String, String] = new mutable.HashMap[String, String]
  var cassandraJvmOptions: String = null

  // node has pending update, true when node updated in none idle state, once stopped becomes false
  var modified: Boolean = false

  var failover: Node.Failover = new Node.Failover()

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

  def idle: Boolean = state == Node.State.IDLE
  def active: Boolean = !idle

  def matches(offer: Offer, now: Date = new Date()): String = {
    val reservation: Reservation = reserve(offer)

    if (reservation.cpus < cpu) return s"cpus < $cpu"
    if (reservation.mem < mem) return s"mem < $mem"

    for (port <- Node.Port.values)
      if (reservation.ports(port) == -1) return s"no suitable $port port"

    if (!stickiness.allowsHostname(offer.getHostname, now))
      return "hostname != stickiness hostname"

    null
  }

  def reserve(offer: Offer): Node.Reservation = {
    val resources = offer.getResourcesList.toList.map(res => res.getName -> res).toMap

    // cpu
    var reservedCpus = 0d
    val cpusResource = resources.getOrElse("cpus", null)
    if (cpusResource != null) reservedCpus = Math.min(cpusResource.getScalar.getValue, cpu)

    // mem
    var reservedMem = 0l
    val memResource = resources.getOrElse("mem", null)
    if (memResource != null) reservedMem = Math.min(memResource.getScalar.getValue.toLong, mem)

    // ports
    var reservedPorts = new mutable.HashMap[Node.Port.Value, Int]
    reservedPorts ++= reservePorts(offer)

    // ignore storage/agent port reservation for collocated instances
    var ignoredPorts = new ListBuffer[Node.Port.Value]
    val collocatedNode = cluster.getNodes.find(n => n.runtime != null && n.runtime.hostname == offer.getHostname).getOrElse(null)

    def ignorePortIfRequired(port: Node.Port.Value) {
      if (reservedPorts(port) != -1 || collocatedNode == null) return
      ignoredPorts += port

      val value: Int = collocatedNode.runtime.reservation.ports(port)
      reservedPorts += (port -> value)
    }

    ignorePortIfRequired(Node.Port.STORAGE)
    ignorePortIfRequired(Node.Port.AGENT)

    // return reservation
    new Reservation(reservedCpus, reservedMem, reservedPorts.toMap, ignoredPorts.toList)
  }

  private[dse] def reservePorts(offer: Offer): Map[Node.Port.Value, Int] = {
    val result = new mutable.HashMap[Node.Port.Value, Int]()
    Node.Port.values.foreach(result(_) = -1)

    val resource = offer.getResourcesList.toList.find(_.getName == "ports").getOrElse(null)
    if (resource == null) return result.toMap

    var availPorts: ListBuffer[Range] = new ListBuffer[Range]()
    availPorts ++= resource.getRanges.getRangeList.map(r => new Util.Range(r.getBegin.toInt, r.getEnd.toInt)).sortBy(_.start)

    for (port <- Node.Port.values) {
      var range: Range = cluster.ports(port)

      // use same storage/agent port for the whole cluster
      val activeNode = cluster.getNodes.find(_.runtime != null).getOrElse(null)
      val samePortRequired = port == Node.Port.STORAGE || port == Node.Port.AGENT

      if (samePortRequired && activeNode != null) {
        val value = activeNode.runtime.reservation.ports(port)
        range = new Range(value, value)
      }

      result(port) = reservePort(range, availPorts)
    }

    result.toMap
  }

  private[dse] def reservePort(range: Range, availPorts: ListBuffer[Range]): Int = {
    var r: Range = null

    if (range == null) r = availPorts.headOption.getOrElse(null)       // take first avail range
    else r = availPorts.find(range.overlap(_) != null).getOrElse(null) // take first range overlapping with ports

    if (r == null) return -1
    val port = if (range != null) r.overlap(range).start else r.start

    // remove allocated port
    val idx = availPorts.indexOf(r)
    availPorts -= r
    availPorts.insertAll(idx, r.split(port))

    port
  }

  def registerStart(hostname: String, ipAddress: String): Unit = {
    stickiness.registerStart(hostname, ipAddress)
    failover.resetFailures()
  }

  def registerStop(now: Date = new Date(), failed: Boolean = false): Unit = {
    if (!failed || failover.failures == 0) stickiness.registerStop(now)

    if (failed) failover.registerFailure(now)
    else failover.resetFailures()
  }

  private[dse] def newTask(): TaskInfo = {
    if (runtime == null) throw new IllegalStateException("runtime == null")

    TaskInfo.newBuilder()
      .setName(s"cassandra-$id")
      .setTaskId(TaskID.newBuilder().setValue(runtime.taskId).build())
      .setSlaveId(SlaveID.newBuilder().setValue(runtime.slaveId))
      .setExecutor(newExecutor())
      .setData(ByteString.copyFromUtf8("" + toJson(expanded = true)))
      .addAllResources(runtime.reservation.toResources)
      .build()
  }

  private[dse] def newExecutor(): ExecutorInfo = {
    if (runtime == null) throw new IllegalStateException("runtime == null")

    var java = "java"
    val commandBuilder = CommandInfo.newBuilder()

    if (Config.cassandra != null) commandBuilder.addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/cassandra/" + Config.cassandra.getName))
    else commandBuilder.addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/dse/" + Config.dse.getName))

    if (Config.jre != null) {
      commandBuilder.addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/jre/" + Config.jre.getName))
      java = "$(find jre* -maxdepth 0 -type d)/bin/java"
    }

    var cmd: String = ""
    if (cluster.ipPerContainerEnabled) {
      // after container gets ip address from calico it has only one network interface (192.168.* by default)
      // we need default 127.0.0.1 loopback interface, otherwise DSE JMX server won't be able to bind to 127.0.0.1 and start

      // make sure sudo doesn't require tty - otherwise adding loopback will fail
      cmd += "sudo ifconfig lo inet 127.0.0.1 netmask 255.0.0.0 up ; ifconfig ; "
    }
    cmd += s"$java -cp ${Config.jar.getName}"
    if (jvmOptions != null) cmd += " " + jvmOptions
    if (Config.debug) cmd += " -Ddebug"
    cmd += " net.elodina.mesos.dse.Executor"

    commandBuilder
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/jar/" + Config.jar.getName).setExtract(false))
      .setValue(cmd)

    val executorInfoBuilder =
      ExecutorInfo.newBuilder()
        .setExecutorId(ExecutorID.newBuilder().setValue(runtime.executorId))
        .setCommand(commandBuilder)
        .setName(s"cassandra-$id")

    if (cluster.ipPerContainerEnabled) {
      if (stickiness.ipAddress != null) {
        val networkInfo = NetworkInfo.newBuilder().addIpAddresses(
          IPAddress.newBuilder().setProtocol(NetworkInfo.Protocol.IPv4).setIpAddress(stickiness.ipAddress))

        executorInfoBuilder.setContainer(ContainerInfo.newBuilder().addNetworkInfos(networkInfo).setType(ContainerInfo.Type.MESOS))
      } else {
        val networkInfo = NetworkInfo.newBuilder().addIpAddresses(
          IPAddress.newBuilder().setProtocol(NetworkInfo.Protocol.IPv4))

        executorInfoBuilder.setContainer(ContainerInfo.newBuilder().addNetworkInfos(networkInfo).setType(ContainerInfo.Type.MESOS))
      }
    }

    executorInfoBuilder.build()
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
    cluster = if (expanded) new Cluster(json("cluster").asInstanceOf[Map[String, Any]]) else Nodes.getCluster(json("cluster").asInstanceOf[String])
    stickiness = new Node.Stickiness(json("stickiness").asInstanceOf[Map[String, Any]])
    if (json.contains("failover")) failover = new Node.Failover(json("failover").asInstanceOf[Map[String, Any]])
    if (json.contains("runtime")) runtime = new Node.Runtime(json("runtime").asInstanceOf[Map[String, Any]])

    cpu = json("cpu").asInstanceOf[Number].doubleValue()
    mem = json("mem").asInstanceOf[Number].longValue()

    seed = json("seed").asInstanceOf[Boolean]
    if (json.contains("jvmOptions")) jvmOptions = json("jvmOptions").asInstanceOf[String]

    rack = json("rack").asInstanceOf[String]
    dc = json("dc").asInstanceOf[String]

    constraints.clear()
    if (json.contains("constraints")) constraints ++= Constraint.parse(json("constraints").asInstanceOf[String])

    seedConstraints.clear()
    if (json.contains("seedConstraints")) seedConstraints ++= Constraint.parse(json("seedConstraints").asInstanceOf[String])

    if (json.contains("dataFileDirs")) dataFileDirs = json("dataFileDirs").asInstanceOf[String]
    if (json.contains("commitLogDir")) commitLogDir = json("commitLogDir").asInstanceOf[String]
    if (json.contains("savedCachesDir")) savedCachesDir = json("savedCachesDir").asInstanceOf[String]

    cassandraDotYaml.clear()
    if (json.contains("cassandraDotYaml")) cassandraDotYaml ++= json("cassandraDotYaml").asInstanceOf[Map[String, String]]

    addressDotYaml.clear()
    if (json.contains("addressDotYaml")) addressDotYaml ++= json("addressDotYaml").asInstanceOf[Map[String, String]]

    if (json.contains("cassandraJvmOptions")) cassandraJvmOptions = json("cassandraJvmOptions").asInstanceOf[String]

    modified = json("modified").asInstanceOf[Boolean]
  }

  def toJson(expanded: Boolean = false): JSONObject = {
    val json = new mutable.LinkedHashMap[String, Any]()

    json("id") = id
    json("state") = "" + state
    json("cluster") = if (expanded) cluster.toJson else cluster.id
    json("stickiness") = stickiness.toJson
    json("failover") = failover.toJson
    if (runtime != null) json("runtime") = runtime.toJson

    json("cpu") = cpu
    json("mem") = mem

    json("seed") = seed
    if (jvmOptions != null) json("jvmOptions") = jvmOptions

    json("rack") = rack
    json("dc") = dc

    if (!constraints.isEmpty) json("constraints") = Util.formatConstraints(constraints)
    if (!seedConstraints.isEmpty) json("seedConstraints") = Util.formatConstraints(seedConstraints)

    if (dataFileDirs != null) json("dataFileDirs") = dataFileDirs
    if (commitLogDir != null) json("commitLogDir") = commitLogDir
    if (savedCachesDir != null) json("savedCachesDir") = savedCachesDir

    if (!cassandraDotYaml.isEmpty) json("cassandraDotYaml") = new JSONObject(cassandraDotYaml.toMap)
    if (!addressDotYaml.isEmpty) json("addressDotYaml") = new JSONObject(addressDotYaml.toMap)
    if (cassandraJvmOptions != null) json("cassandraJvmOptions") = cassandraJvmOptions

    json("modified") = modified

    new JSONObject(json.toMap)
  }

  override def hashCode(): Int = id.hashCode

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[Node]) false
    else id == obj.asInstanceOf[Node].id
  }

  override def toString: String = id

  def maxHeap: Size =
    if (cassandraJvmOptions != null && cassandraJvmOptions.contains("-Xmx"))
      new Size(cassandraJvmOptions.split(" ").find(_.startsWith("-Xmx")).get.substring(4))
    else
      new Size(max(min(0.5 * mem, 1024.0), min(0.25 * mem, 8 * 1024.0)).toLong + "M")

  def youngGen: Size =
    if (cassandraJvmOptions != null && cassandraJvmOptions.contains("-Xmn"))
      new Size(cassandraJvmOptions.split(" ").find(_.startsWith("-Xmn")).get.substring(4))
    else
      new Size(min(100.0 * cpu, 0.25 * maxHeap.toM.value).toLong + "M")
}

object Node {
  def idFromTaskId(taskId: String): String = {
    taskId.split("-", 3) match {
      case Array(_, value, _) => value
      case _ => throw new IllegalArgumentException(taskId)
    }
  }

  private def dateTimeFormat: SimpleDateFormat = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    format.setTimeZone(TimeZone.getTimeZone("UTC-0"))
    format
  }

  object State extends Enumeration {
    val IDLE = Value("idle")
    val STARTING = Value("starting")
    val RUNNING = Value("running")
    val STOPPING = Value("stopping")
    val RECONCILING = Value("reconciling")
  }

  object Port extends Enumeration {
    val STORAGE = Value("storage")
    val JMX = Value("jmx")
    val CQL = Value("cql")
    val THRIFT = Value("thrift")
    val AGENT = Value("agent")
  }

  class Runtime() {
    var taskId: String = null
    var executorId: String = null

    var slaveId: String = null
    var hostname: String = null
    var address: String = null

    var seeds: List[String] = null
    var reservation: Reservation = null
    var attributes: Map[String, String] = null

    def this(taskId: String = null, executorId: String = null, slaveId: String = null, hostname: String = null,
             seeds: List[String] = null, reservation: Reservation = new Reservation(), attributes: Map[String, String] = Map()) {
      this
      this.taskId = taskId
      this.executorId = executorId

      this.slaveId = slaveId
      this.hostname = hostname

      this.seeds = seeds
      this.reservation = reservation
      this.attributes = attributes
    }

    def this(taskId: String, execId: String, seeds: List[String], reservation: Reservation, offer: Offer) = {
      this(
        taskId, execId, offer.getSlaveId.getValue, offer.getHostname, seeds, reservation,
        offer.getAttributesList.toList.filter(_.hasText).map(attr => attr.getName -> attr.getText.getValue).toMap
      )
    }

    def this(node: Node, offer: Offer) = {
      this(null, null, null, null, offer)

      seeds = node.cluster.availSeeds
      if (seeds.isEmpty) {
        Scheduler.logger.info(s"No seed nodes available in cluster ${node.cluster.id}. Forcing seed==true for node ${node.id}")
        node.seed = true
      }

      reservation = node.reserve(offer)

      taskId = "cassandra-" + node.id + "-" + System.currentTimeMillis()
      executorId = "cassandra-" + node.id + "-" + System.currentTimeMillis()
    }

    def this(json: Map[String, Any]) = {
      this
      fromJson(json)
    }

    def fromJson(json: Map[String, Any]): Unit = {
      taskId = json("taskId").asInstanceOf[String]
      executorId = json("executorId").asInstanceOf[String]

      slaveId = json("slaveId").asInstanceOf[String]
      hostname = json("hostname").asInstanceOf[String]
      if (json.contains("address")) address = json("address").asInstanceOf[String]

      seeds = json("seeds").asInstanceOf[List[String]]
      reservation = new Reservation(json("reservation").asInstanceOf[Map[String, Any]])
      attributes = json("attributes").asInstanceOf[Map[String, String]]
    }

    def toJson: JSONObject = {
      val json = new mutable.LinkedHashMap[String, Any]()
      json("taskId") = taskId
      json("executorId") = executorId

      json("slaveId") = slaveId
      json("hostname") = hostname
      if (address != null) json("address") = address

      json("seeds") = new JSONArray(seeds)
      json("reservation") = reservation.toJson
      json("attributes") = new JSONObject(attributes)
      new JSONObject(json.toMap)
    }
  }

  class Reservation {
    var cpus: Double = 0
    var mem: Long = 0

    var ports: mutable.HashMap[Node.Port.Value, Int] = new mutable.HashMap[Node.Port.Value, Int]()
    var ignoredPorts: mutable.ListBuffer[Node.Port.Value] = new mutable.ListBuffer[Node.Port.Value]

    resetPorts()

    def this(cpus: Double = 0, mem: Long = 0, ports: Map[Node.Port.Value, Int] = Map(), ignoredPorts: List[Node.Port.Value] = List()) {
      this
      this.cpus = cpus
      this.mem = mem

      this.resetPorts()
      this.ports ++= ports
      this.ignoredPorts ++= ignoredPorts
    }

    def this(json: Map[String, Any]) {
      this
      fromJson(json)
    }

    def resetPorts() {
      ports.clear()
      Node.Port.values.foreach(ports(_) = -1)
    }

    def toResources: List[Resource] = {
      def cpusResource(value: Double): Resource = {
        Resource.newBuilder
          .setName("cpus")
          .setType(Value.Type.SCALAR)
          .setScalar(Value.Scalar.newBuilder.setValue(value))
          .setRole("*")
          .build()
      }

      def memResource(value: Long): Resource = {
        Resource.newBuilder
          .setName("mem")
          .setType(Value.Type.SCALAR)
          .setScalar(Value.Scalar.newBuilder.setValue(value))
          .setRole("*")
          .build()
      }

      def portResource(value: Long): Resource = {
        Resource.newBuilder
          .setName("ports")
          .setType(Value.Type.RANGES)
          .setRanges(Value.Ranges.newBuilder.addRange(Value.Range.newBuilder().setBegin(value).setEnd(value)))
          .setRole("*")
          .build()
      }

      val resources: ListBuffer[Resource] = new ListBuffer[Resource]()

      if (cpus > 0) resources += cpusResource(cpus)
      if (mem > 0) resources += memResource(mem)

      for (port <- Node.Port.values) {
        val value = ports(port)
        if (value != -1 && !ignoredPorts.contains(port))
          resources += portResource(value)
      }

      resources.toList
    }

    def fromJson(json: Map[String, Any]): Unit = {
      cpus = json("cpus").asInstanceOf[Number].doubleValue()
      mem = json("mem").asInstanceOf[Number].longValue()

      resetPorts()
      for ((port, value) <- json("ports").asInstanceOf[Map[String, Number]])
        ports += Node.Port.withName(port) -> value.intValue

      ignoredPorts.clear()
      if (json.contains("ignoredPorts"))
        ignoredPorts ++= json("ignoredPorts").asInstanceOf[List[String]].map(Node.Port.withName)
    }

    def toJson: JSONObject = {
      val json = new mutable.LinkedHashMap[String, Any]()

      json("cpus") = cpus
      json("mem") = mem

      val portsJson = new mutable.HashMap[String, Any]()
      for ((port, value) <- ports) portsJson += "" + port -> value
      json("ports") = new JSONObject(portsJson.toMap)

      json("ignoredPorts") = new JSONArray(ignoredPorts.toList.map("" + _))
      new JSONObject(json.toMap)
    }
  }

  class Stickiness(_period: Period = new Period("30m")) {
    var period: Period = _period
    @volatile var hostname: String = null
    // "sticky" ip address that the node gets as part of ip per container support when the task is started
    @volatile var ipAddress: String = null
    @volatile var stopTime: Date = null

    def this(json: Map[String, Any]) {
      this
      fromJson(json)
    }

    def expires: Date = if (stopTime != null) new Date(stopTime.getTime + period.ms) else null

    def registerStart(hostname: String, ipAddress: String): Unit = {
      this.hostname = hostname
      this.ipAddress = ipAddress
      this.stopTime = null
    }

    def registerStop(now: Date = new Date()): Unit = {
      this.stopTime = now
    }

    def allowsHostname(hostname: String, now: Date = new Date()): Boolean = {
      if (this.hostname == null) return true
      if (stopTime == null || now.getTime - stopTime.getTime >= period.ms) return true
      this.hostname == hostname
    }

    def fromJson(json: Map[String, Any]): Unit = {
      period = new Period(json("period").asInstanceOf[String])
      if (json.contains("stopTime")) stopTime = dateTimeFormat.parse(json("stopTime").asInstanceOf[String])
      if (json.contains("hostname")) hostname = json("hostname").asInstanceOf[String]
    }

    def toJson: JSONObject = {
      val obj = new collection.mutable.LinkedHashMap[String, Any]()

      obj("period") = "" + period
      if (stopTime != null) obj("stopTime") = dateTimeFormat.format(stopTime)
      if (hostname != null) obj("hostname") = hostname

      new JSONObject(obj.toMap)
    }
  }

  class Failover(var delay: Period = new Period("3m"), var maxDelay: Period = new Period("30m"), var maxTries: Integer = null) {

    @volatile var failures: Int = 0
    @volatile var failureTime: Date = null

    def this(json: Map[String, Any]) {
      this
      fromJson(json)
    }

    def currentDelay: Period = {
      if (failures == 0) return new Period("0")

      val multiplier = 1 << (failures - 1)
      val d = delay.ms * multiplier

      if (d > maxDelay.ms) maxDelay else new Period(delay.value * multiplier + delay.unit)
    }

    def delayExpires: Date = {
      if (failures == 0) return new Date(0)
      new Date(failureTime.getTime + currentDelay.ms)
    }

    def isWaitingDelay(now: Date = new Date()): Boolean = delayExpires.getTime > now.getTime

    def isMaxTriesExceeded: Boolean = {
      if (maxTries == null) return false
      failures >= maxTries
    }

    def registerFailure(now: Date = new Date()): Unit = {
      failures += 1
      failureTime = now
    }

    def resetFailures(): Unit = {
      failures = 0
      failureTime = null
    }

    def fromJson(node: Map[String, Any]): Unit = {
      delay = new Period(node("delay").asInstanceOf[String])
      maxDelay = new Period(node("maxDelay").asInstanceOf[String])
      if (node.contains("maxTries")) maxTries = node("maxTries").asInstanceOf[Number].intValue()

      if (node.contains("failures")) failures = node("failures").asInstanceOf[Number].intValue()
      if (node.contains("failureTime")) failureTime = dateTimeFormat.parse(node("failureTime").asInstanceOf[String])
    }

    def toJson: JSONObject = {
      val obj = new collection.mutable.LinkedHashMap[String, Any]()

      obj("delay") = "" + delay
      obj("maxDelay") = "" + maxDelay
      if (maxTries != null) obj("maxTries") = maxTries

      if (failures != 0) obj("failures") = failures
      if (failureTime != null) obj("failureTime") = dateTimeFormat.format(failureTime)

      new JSONObject(obj.toMap)
    }
  }

}
