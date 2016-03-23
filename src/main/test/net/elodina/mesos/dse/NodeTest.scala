package net.elodina.mesos.dse

import org.junit.Test
import org.junit.Assert._
import org.apache.mesos.Protos.{TaskInfo, CommandInfo, ExecutorInfo}
import scala.concurrent.duration.Duration
import net.elodina.mesos.dse.Node._
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import net.elodina.mesos.util.{Strings, Period, Range}
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
    assertEquals(Map(Port.STORAGE -> 0, Port.JMX -> 1, Port.CQL -> -1, Port.THRIFT -> -1, Port.AGENT -> -1), reservation.ports)

    // complete reservation
    reservation = node.reserve(offer(resources = "cpus:0.7;mem:1000;ports:0..10"))
    assertEquals(node.cpu, reservation.cpus, 0.001)
    assertEquals(node.mem, reservation.mem)
    assertEquals(Map(Port.STORAGE -> 0, Port.JMX -> 1, Port.CQL -> 2, Port.THRIFT -> 3, Port.AGENT -> 4), reservation.ports)
  }

  @Test
  def reserve_ignoredPorts {
    val node0 = Nodes.addNode(new Node("0"))
    val node1 = Nodes.addNode(new Node("1"))

    // storage/agent ports available
    var reservation: Reservation = node1.reserve(offer(hostname = "slave0", resources = "ports:0..200"))
    assertEquals(0, reservation.ports(Port.STORAGE))
    assertTrue(reservation.ignoredPorts.isEmpty)

    // storage port unavailable, have collocated, running node
    node0.state = Node.State.RUNNING
    node0.runtime = new Node.Runtime(hostname = "slave0", reservation = new Node.Reservation(ports = Map(Port.STORAGE -> 100, Port.AGENT -> 101)))
    
    reservation = node1.reserve(offer(hostname = "slave0", resources = "ports:0..99,102..200"))
    assertEquals(100, reservation.ports(Port.STORAGE))
    assertEquals(101, reservation.ports(Port.AGENT))
    assertEquals(List(Port.STORAGE, Port.AGENT), reservation.ignoredPorts)
  }

  @Test
  def reservePorts {
    val node: Node = Nodes.addNode(new Node("0"))

    def test(portsDef: String, resources: String, expected: Map[Port.Value, Int]) {
      // parses expr like: storage=0..4,jmx=5,cql=100..110
      def parsePortsDef(s: String): Map[Port.Value, Range] = {
        val ports = new mutable.HashMap[Port.Value, Range]()
        Port.values.foreach(ports(_) = null)

        for ((k,v) <- Strings.parseMap(s))
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
    node.runtime = new Runtime(reservation = new Reservation(ports = Map(Port.STORAGE -> 100, Port.AGENT -> 200)))

    test("storage=10..20,agent=30..40", "ports:0..1000", Map(Port.STORAGE -> 100, Port.AGENT -> 200))
    test("", "ports:0..1000", Map(Port.STORAGE -> 100, Port.AGENT -> 200))
    test("storage=10..20,agent=30..40", "ports:0..99", Map(Port.STORAGE -> -1, Port.AGENT -> -1))
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

    assertEquals(resources("cpus:0.1; mem:500; ports:0..0; ports:1..1; ports:2..2; ports:3..3; ports:4..4"), task.getResourcesList)
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

    delay("100ms") { node.state = Node.State.RUNNING }
    assertTrue(node.waitFor(Node.State.RUNNING, Duration("200ms")))

    delay("100ms") { node.state = Node.State.IDLE }
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
    node.jvmOptions = "options"

    node.rack = "r"
    node.dc = "d"

    node.constraints ++= Constraint.parse("hostname=like:master")
    node.seedConstraints ++= Constraint.parse("hostname=like:master")

    node.dataFileDirs = "dataDir"
    node.commitLogDir = "logDir"
    node.savedCachesDir = "saveCachesDir"
    node.cassandraDotYaml = new mutable.HashMap() ++= Seq("hinted_handoff_enabled" -> "false", "num_tokens" -> "312")
    node.cassandraJvmOptions = "-Dcassandra.partitioner=p1 -Dcassandra.ring_delay_ms=100"

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

  @Test
  def Node_maxHeap {
    def test(mem: Int, expected: String, jvmOpts: String = "") {
      val node: Node = new Node("0")
      node.mem = mem
      node.cassandraJvmOptions = jvmOpts
      assertEquals(expected, "" + node.maxHeap)
    }

    test(1024, "512M")
    test(2048, "1024M")
    test(4096, "1024M")

    test(8192, "2048M")
    test(16384, "4096M")
    test(32768, "8192M")

    test(65536, "8192M")

    // mx specified
    test(1024, "8G", "-Xmx8G")
  }

  @Test
  def Node_youngGen {
    def test(heap: Int, cpu: Double, expected: String, jvmOpts: String = "") {
      val node: Node = new Node("0")
      node.cpu = cpu
      node.mem = heap
      node.cassandraJvmOptions = s"$jvmOpts -Xmx${heap}M"
      assertEquals(expected, "" + node.youngGen)
    }

    test(1024, 1, "100M")
    test(1024, 2, "200M")
    test(1024, 4, "256M")

    test(4096, 1, "100M")
    test(4096, 2, "200M")
    test(4096, 8, "800M")
    test(4096, 16, "1024M")

    // mn specified
    test(1024, 1, "256M", "-Xmn256M")
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

  // Failover
  @Test
  def Failover_currentDelay {
    val failover = new Failover(new Period("1s"), new Period("5s"))

    failover.failures = 0
    assertEquals(new Period("0s"), failover.currentDelay)

    failover.failures = 1
    assertEquals(new Period("1s"), failover.currentDelay)

    failover.failures = 2
    assertEquals(new Period("2s"), failover.currentDelay)

    failover.failures = 3
    assertEquals(new Period("4s"), failover.currentDelay)

    failover.failures = 4
    assertEquals(new Period("5s"), failover.currentDelay)

    failover.failures = 100
    assertEquals(new Period("5s"), failover.currentDelay)
  }

  @Test
  def Failover_delayExpires {
    val failover = new Failover(new Period("1s"))
    assertEquals(new Date(0), failover.delayExpires)

    failover.registerFailure(new Date(0))
    assertEquals(new Date(1000), failover.delayExpires)

    failover.failureTime = new Date(1000)
    assertEquals(new Date(2000), failover.delayExpires)
  }

  @Test
  def Failover_isWaitingDelay {
    val failover = new Failover(new Period("1s"))
    assertFalse(failover.isWaitingDelay(new Date(0)))

    failover.registerFailure(new Date(0))

    assertTrue(failover.isWaitingDelay(new Date(0)))
    assertTrue(failover.isWaitingDelay(new Date(500)))
    assertTrue(failover.isWaitingDelay(new Date(999)))
    assertFalse(failover.isWaitingDelay(new Date(1000)))
  }

  @Test
  def Failover_isMaxTriesExceeded {
    val failover = new Failover()

    failover.failures = 100
    assertFalse(failover.isMaxTriesExceeded)

    failover.maxTries = 50
    assertTrue(failover.isMaxTriesExceeded)
  }

  @Test
  def Failover_registerFailure_resetFailures {
    val failover = new Failover()
    assertEquals(0, failover.failures)
    assertNull(failover.failureTime)

    failover.registerFailure(new Date(1))
    assertEquals(1, failover.failures)
    assertEquals(new Date(1), failover.failureTime)

    failover.registerFailure(new Date(2))
    assertEquals(2, failover.failures)
    assertEquals(new Date(2), failover.failureTime)

    failover.resetFailures()
    assertEquals(0, failover.failures)
    assertNull(failover.failureTime)

    failover.registerFailure()
    assertEquals(1, failover.failures)
  }

  @Test
  def Failover_toJson_fromJson {
    val failover = new Failover(new Period("1s"), new Period("5s"))
    failover.maxTries = 10
    failover.resetFailures()
    failover.registerFailure(new Date(0))

    val read: Failover = new Failover()
    read.fromJson(Util.parseJson("" + failover.toJson).asInstanceOf[Map[String, Object]])

    assertFailoverEquals(failover, read)
  }
}
