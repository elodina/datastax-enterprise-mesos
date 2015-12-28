package net.elodina.mesos.dse

import org.junit.Test
import org.junit.Assert._
import org.apache.mesos.Protos.{TaskInfo, CommandInfo, ExecutorInfo}
import scala.concurrent.duration.Duration
import net.elodina.mesos.dse.Node.{Reservation, Runtime, Stickiness, Port}
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import Util.Range
import java.util.Date

class NodeTest extends MesosTestCase {
  @Test
  def matches {
    val node = new Node("0")
    node.cpu = 0.5
    node.mem = 500
    node.cluster.resetPorts

    assertEquals(s"cpus < 0.5", node.matches(offer(resources = "cpus:0.1")))
    assertEquals(s"mem < 500", node.matches(offer(resources = "cpus:0.5; mem:400")))

    assertEquals("no suitable jmx port", node.matches(offer(resources = "cpus:0.5; mem:500; ports:5..5")))
    assertEquals("no suitable thrift port", node.matches(offer(resources = "cpus:0.5; mem:500; ports:5..7")))

    assertNull(node.matches(offer(resources = "cpus:0.5; mem:500; ports:0..4")))
  }

  @Test
  def matches_stickiness {
    val node = new Node("0")
    val host0 = "host0"
    val host1 = "host1"
    val resources = s"cpus:${node.cpu};mem:${node.mem};ports:0..10"

    assertEquals(null, node.matches(offer(hostname = host0, resources = resources), new Date(0)))
    assertEquals(null, node.matches(offer(hostname = host1, resources = resources), new Date(0)))

    node.registerStart(host0)
    node.registerStop(new Date(0))

    assertEquals(null, node.matches(offer(hostname = host0, resources = resources), new Date(0)))
    assertEquals("hostname != stickiness hostname", node.matches(offer(hostname = host1, resources = resources), new Date(0)))
    assertEquals(null, node.matches(offer(hostname = host1, resources = resources), new Date(node.stickiness.period.ms)))
  }

  @Test
  def reserve {
    val node = new Node("0")
    node.cpu = 0.5
    node.mem = 400

    // incomplete reservation
    var reservation = node.reserve(offer(resources = "cpus:0.3;mem:300;ports:0..1"))
    assertEquals(0.3d, reservation.cpus, 0.001)
    assertEquals(300, reservation.mem)
    assertEquals(Map(Port.STORAGE -> 0, Port.JMX -> 1, Port.CQL -> -1, Port.THRIFT -> -1), reservation.ports)

    // complete reservation
    reservation = node.reserve(offer(resources = "cpus:0.7;mem:1000;ports:0..10"))
    assertEquals(node.cpu, reservation.cpus, 0.001)
    assertEquals(node.mem, reservation.mem)
    assertEquals(Map(Port.STORAGE -> 0, Port.JMX -> 1, Port.CQL -> 2, Port.THRIFT -> 3), reservation.ports)
  }

  @Test
  def reserve_ignoredPorts {
    val node0 = Nodes.addNode(new Node("0"))
    val node1 = Nodes.addNode(new Node("1"))

    // storage port available
    var reservation: Reservation = node1.reserve(offer(hostname = "slave0", resources = "ports:0..200"))
    assertEquals(0, reservation.ports(Port.STORAGE))
    assertTrue(reservation.ignoredPorts.isEmpty)

    // storage port unavailable, have collocated, running node
    node0.state = Node.State.RUNNING
    node0.runtime = new Node.Runtime(hostname = "slave0", reservation = new Node.Reservation(ports = Map(Port.STORAGE -> 100)))
    
    reservation = node1.reserve(offer(hostname = "slave0", resources = "ports:0..99,101..200"))
    assertEquals(100, reservation.ports(Port.STORAGE))
    assertEquals(List(Port.STORAGE), reservation.ignoredPorts)
  }

  @Test
  def reservePorts {
    val node: Node = Nodes.addNode(new Node("0"))

    def test(portsDef: String, resources: String, expected: Map[Port.Value, Int]) {
      // parses expr like: storage=0..4,jmx=5,cql=100..110
      def parsePortsDef(s: String): Map[Port.Value, Range] = {
        val ports = new mutable.HashMap[Port.Value, Range]()
        Port.values.foreach(ports(_) = null)

        for ((k,v) <- Util.parseMap(s))
          ports(Port.withName(k)) = new Range(v)

        ports.toMap
      }

      node.cluster.ports.clear()
      node.cluster.ports ++= parsePortsDef(portsDef)
      val ports: Map[Port.Value, Int] = node.reservePorts(offer(resources = resources))

      for ((port, value) <- expected)
        assertTrue(s"portsDef:$portsDef, resources:$resources, expected:$expected, actual:$ports", ports.getOrElse(port, null) == value)
    }

    // any ports
    test("", "ports:0", Map(Port.STORAGE -> 0, Port.JMX -> -1, Port.CQL -> -1, Port.THRIFT -> -1))
    test("", "ports:0..2", Map(Port.STORAGE -> 0, Port.JMX -> 1, Port.CQL -> 2, Port.THRIFT -> -1))
    test("", "ports:0..1,10..20", Map(Port.STORAGE -> 0, Port.JMX   -> 1, Port.CQL -> 10, Port.THRIFT -> 11))

    // single port
    test("storage=0", "ports:0", Map(Port.STORAGE -> 0))
    test("storage=1000,jmx=1001", "ports:1000..1001", Map(Port.STORAGE -> 1000, Port.JMX -> 1001))
    test("storage=1000,jmx=1001", "ports:999..1000;ports:1002..1010", Map(Port.STORAGE -> 1000, Port.JMX -> -1))
    test("storage=1000,jmx=1001,cql=1005,thrift=1010", "ports:1001..1008,1011..1100", Map(Port.STORAGE -> -1, Port.JMX -> 1001, Port.CQL -> 1005, Port.THRIFT -> -1))

    // port ranges
    test("storage=10..20", "ports:15..25", Map(Port.STORAGE -> 15))
    test("storage=10..20,jmx=100..200", "ports:15..25,150..160", Map(Port.STORAGE -> 15, Port.JMX -> 150))

    // cluster has active node
    node.state = Node.State.RUNNING
    node.runtime = new Runtime(reservation = new Reservation(ports = Map(Port.STORAGE -> 100)))

    test("storage=10..20", "ports:0..1000", Map(Port.STORAGE -> 100))
    test("", "ports:0..1000", Map(Port.STORAGE -> 100))
    test("storage=10..20", "ports:0..99", Map(Port.STORAGE -> -1))
  }

  @Test
  def reservePort {
    val node = new Node("0")
    var ports = new ListBuffer[Range]()
    ports += new Range("0..100")

    assertEquals(10, node.reservePort(new Range("10..20"), ports))
    assertEquals(List(new Range("0..9"), new Range("11..100")), ports.toList)

    assertEquals(0, node.reservePort(new Range("0..0"), ports))
    assertEquals(List(new Range("1..9"), new Range("11..100")), ports.toList)

    assertEquals(100, node.reservePort(new Range("100..200"), ports))
    assertEquals(List(new Range("1..9"), new Range("11..99")), ports.toList)

    assertEquals(50, node.reservePort(new Range("50..60"), ports))
    assertEquals(List(new Range("1..9"), new Range("11..49"), new Range("51..99")), ports.toList)
  }

  @Test
  def newTask {
    val node = new Node("0")
    node.cpu = 0.1
    node.mem = 500
    node.runtime = new Runtime(node, offer(resources = "cpus:1;mem:1000;ports:0..10"))

    val task: TaskInfo = node.newTask()
    assertEquals(node.runtime.taskId, task.getTaskId.getValue)
    assertEquals(s"cassandra-${node.id}", task.getName)
    assertEquals(node.runtime.slaveId, task.getSlaveId.getValue)

    val data: String = task.getData.toStringUtf8
    val read: Node = new Node(Util.parseJsonAsMap(data), expanded = true)
    assertEquals(node, read)

    assertEquals(resources("cpus:0.1; mem:500; ports:0..0; ports:1..1; ports:2..2; ports:3..3"), task.getResourcesList)
  }

  @Test
  def newExecutor {
    val node = new Node("0")
    node.runtime = new Runtime(node, offer())

    val executor: ExecutorInfo = node.newExecutor()
    assertEquals(node.runtime.executorId, executor.getExecutorId.getValue)
    assertEquals(s"cassandra-${node.id}", executor.getName)

    val command: CommandInfo = executor.getCommand

    val cmd: String = command.getValue
    assertTrue(cmd, cmd.contains("java"))
    assertTrue(cmd, cmd.contains(Config.jar.getName))
    assertTrue(cmd, cmd.contains(Executor.getClass.getName.replace("$", "")))
  }

  @Test(timeout = 5000)
  def waitFor {
    val node = new Node("0")

    def deferStateSwitch(state: Node.State.Value, delay: Long) {
      new Thread() {
        override def run() {
          setName(classOf[Node].getSimpleName + "-scheduleState")
          Thread.sleep(delay)
          node.state = state
        }
      }.start()
    }

    deferStateSwitch(Node.State.RUNNING, 100)
    assertTrue(node.waitFor(Node.State.RUNNING, Duration("200ms")))

    deferStateSwitch(Node.State.IDLE, 100)
    assertTrue(node.waitFor(Node.State.IDLE, Duration("200ms")))

    // timeout
    assertFalse(node.waitFor(Node.State.RUNNING, Duration("50ms")))
  }

  @Test
  def toJSON_fromJSON {
    val node: Node = new Node("1")
    var read = new Node(Util.parseJsonAsMap("" + node.toJson()))
    assertNodeEquals(node, read)

    node.state = Node.State.RUNNING
    node.cluster = Nodes.addCluster(new Cluster("0"))
    node.stickiness.hostname = "host"
    node.stickiness.stopTime = new Date()
    node.runtime = new Runtime("task", "executor", "slave", "host", List("n0", "n1"), new Node.Reservation(), Map("a" -> "1"))

    node.cpu = 1
    node.mem = 1024

    node.seed = true
    node.replaceAddress = "127.0.0.2"
    node.jvmOptions = "options"

    node.rack = "r"
    node.dc = "d"

    node.constraints ++= Constraint.parse("hostname=like:master")
    node.seedConstraints ++= Constraint.parse("hostname=like:master")

    node.dataFileDirs = "dataDir"
    node.commitLogDir = "logDir"
    node.savedCachesDir = "saveCachesDir"

    read = new Node(Util.parseJsonAsMap("" + node.toJson()))
    assertNodeEquals(read, node)
  }

  @Test
  def Node_idFromTaskId {
    assertEquals("id", Node.idFromTaskId("node-id-timestamp"))

    try { Node.idFromTaskId("node"); fail() }
    catch { case e: IllegalArgumentException => }

    try { Node.idFromTaskId("node-id"); fail() }
    catch { case e: IllegalArgumentException => }
  }

  // Runtime
  @Test
  def Runtime_toJson_fromJson {
    val runtime = new Runtime("task", "executor", "slave", "host", List("n0", "n1"), new Node.Reservation(), Map("a" -> "1"))
    runtime.address = "address"

    val read = new Runtime(Util.parseJsonAsMap(runtime.toJson.toString()))
    assertRuntimeEquals(runtime, read)
  }

  @Test
  def Reservation_toJson_fromJson {
    val reservation = new Reservation(1.0, 256, Map(Port.STORAGE -> 7000), List(Node.Port.JMX))
    val read = new Reservation(Util.parseJsonAsMap("" + reservation.toJson))
    assertReservationEquals(reservation, read)
  }

  @Test
  def Reservation_toResources {
    assertEquals(resources("").toList, new Reservation().toResources)
    assertEquals(resources("cpus:0.5;mem:500;ports:1000..1000;ports:2000").toList, new Reservation(0.5, 500, Map(Port.STORAGE -> 1000, Port.JMX -> 2000)).toResources)

    // ignore storage port
    assertEquals(resources("ports:2000").toList, new Reservation(ports = Map(Port.STORAGE -> 1000, Port.JMX -> 2000), ignoredPorts = List(Port.STORAGE)).toResources)
  }

  // Stickiness
  @Test
  def Stickiness_allowsHostname {
    val stickiness = new Stickiness()
    assertTrue(stickiness.allowsHostname("host0", new Date(0)))
    assertTrue(stickiness.allowsHostname("host1", new Date(0)))

    stickiness.registerStart("host0")
    stickiness.registerStop(new Date(0))
    assertTrue(stickiness.allowsHostname("host0", new Date(0)))
    assertFalse(stickiness.allowsHostname("host1", new Date(0)))
    assertTrue(stickiness.allowsHostname("host1", new Date(stickiness.period.ms)))
  }

  @Test
  def Stickiness_registerStart_registerStop {
    val stickiness = new Stickiness()
    assertNull(stickiness.hostname)
    assertNull(stickiness.stopTime)

    stickiness.registerStart("host")
    assertEquals("host", stickiness.hostname)
    assertNull(stickiness.stopTime)

    stickiness.registerStop(new Date(0))
    assertEquals("host", stickiness.hostname)
    assertEquals(new Date(0), stickiness.stopTime)

    stickiness.registerStart("host1")
    assertEquals("host1", stickiness.hostname)
    assertNull(stickiness.stopTime)
  }

  @Test
  def Stickiness_toJson_fromJson {
    val stickiness = new Stickiness()
    stickiness.registerStart("localhost")
    stickiness.registerStop(new Date(0))

    val read: Stickiness = new Stickiness()
    read.fromJson(Util.parseJsonAsMap("" + stickiness.toJson))

    assertStickinessEquals(stickiness, read)
  }

  def assertNodeEquals(expected: Node, actual: Node) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.id, actual.id)
    assertEquals(expected.state, actual.state)
    assertEquals(expected.cluster, actual.cluster)
    assertStickinessEquals(expected.stickiness, actual.stickiness)
    assertRuntimeEquals(expected.runtime, actual.runtime)

    assertEquals(expected.cpu, actual.cpu, 0.001)
    assertEquals(expected.mem, actual.mem)
    assertEquals(expected.seed, actual.seed)

    assertEquals(expected.replaceAddress, actual.replaceAddress)
    assertEquals(expected.jvmOptions, actual.jvmOptions)

    assertEquals(expected.rack, actual.rack)
    assertEquals(expected.dc, actual.dc)

    assertEquals(expected.constraints, actual.constraints)
    assertEquals(expected.seedConstraints, actual.seedConstraints)

    assertEquals(expected.dataFileDirs, actual.dataFileDirs)
    assertEquals(expected.commitLogDir, actual.commitLogDir)
    assertEquals(expected.savedCachesDir, actual.savedCachesDir)
  }

  def assertRuntimeEquals(expected: Runtime, actual: Runtime) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.taskId, actual.taskId)
    assertEquals(expected.executorId, actual.executorId)

    assertEquals(expected.slaveId, actual.slaveId)
    assertEquals(expected.hostname, actual.hostname)
    assertEquals(expected.address, actual.address)

    assertEquals(expected.seeds, actual.seeds)
    assertReservationEquals(expected.reservation, actual.reservation)
    assertEquals(expected.attributes, actual.attributes)
  }

  def assertReservationEquals(expected: Reservation, actual: Reservation) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.cpus, actual.cpus, 0.001)
    assertEquals(expected.mem, actual.mem)
    assertEquals(expected.ports, actual.ports)
    assertEquals(expected.ignoredPorts, actual.ignoredPorts)
  }

  def assertStickinessEquals(expected: Stickiness, actual: Stickiness) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.period, actual.period)
    assertEquals(expected.hostname, actual.hostname)
    assertEquals(expected.stopTime, actual.stopTime)
  }

  private def checkNulls(expected: Object, actual: Object): Boolean = {
    if (expected == actual) return true
    if (expected == null) throw new AssertionError("actual != null")
    if (actual == null) throw new AssertionError("actual == null")
    false
  }
}
