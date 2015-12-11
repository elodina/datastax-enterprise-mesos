package net.elodina.mesos.dse

import org.junit.Test
import org.junit.Assert._
import org.apache.mesos.Protos.{TaskInfo, CommandInfo, ExecutorInfo}
import scala.concurrent.duration.Duration
import net.elodina.mesos.dse.Node.Reservation
import scala.collection.mutable
import scala.collection.JavaConversions._

class NodeTest extends MesosTestCase {
  @Test
  def matches {
    val node = new Node("0")
    node.cpu = 0.5
    node.mem = 500
    node.ring.resetPorts

    assertEquals(s"cpus < 0.5", node.matches(offer(resources = "cpus:0.1")))
    assertEquals(s"mem < 500", node.matches(offer(resources = "cpus:0.5; mem:400")))

    assertEquals("no suitable jmx port", node.matches(offer(resources = "cpus:0.5; mem:500; ports:5..5")))
    assertEquals("no suitable thrift port", node.matches(offer(resources = "cpus:0.5; mem:500; ports:5..7")))

    assertNull(node.matches(offer(resources = "cpus:0.5; mem:500; ports:0..4")))
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
    assertEquals(Map("internal" -> 0, "jmx" -> 1, "cql" -> -1, "thrift" -> -1), reservation.ports)

    // complete reservation
    reservation = node.reserve(offer(resources = "cpus:0.7;mem:1000;ports:0..10"))
    assertEquals(node.cpu, reservation.cpus, 0.001)
    assertEquals(node.mem, reservation.mem)
    assertEquals(Map("internal" -> 0, "jmx" -> 1, "cql" -> 2, "thrift" -> 3), reservation.ports)
  }

  @Test
  def reservePorts {
    val node: Node = new Node("0")

    def test(portsDef: String, resources: String, expected: Map[String, Int]) {
      // parses expr like: storage=0..4,jmx=5,cql=100..110
      def parsePortsDef(s: String): Map[String, Util.Range] = {
        val ports = new mutable.HashMap[String, Util.Range]()
        Node.portNames.foreach(ports(_) = null)

        for ((k,v) <- Util.parseMap(s))
          ports(k) = new Util.Range(v)

        ports.toMap
      }

      node.ring.ports.clear()
      node.ring.ports ++= parsePortsDef(portsDef)
      val ports: Map[String, Int] = node.reservePorts(offer(resources = resources))

      for ((name, port) <- expected)
        assertTrue(s"portsDef:$portsDef, resources:$resources, expected:$expected, actual:$ports", ports.getOrElse(name, null) == port)
    }

    // any ports
    test("", "ports:0", Map("internal" -> 0, "jmx" -> -1, "cql" -> -1, "thrift" -> -1))
    test("", "ports:0..2", Map("internal" -> 0, "jmx" -> 1, "cql" -> 2, "thrift" -> -1))
    test("", "ports:0..1,10..20", Map("internal" -> 0, "jmx" -> 1, "cql" -> 10, "thrift" -> 11))

    // single port
    test("internal=0", "ports:0", Map("internal" -> 0))
    test("internal=1000,jmx=1001", "ports:1000..1001", Map("internal" -> 1000, "jmx" -> 1001))
    test("internal=1000,jmx=1001", "ports:999..1000;ports:1002..1010", Map("internal" -> 1000, "jmx" -> -1))
    test("internal=1000,jmx=1001,cql=1005,thrift=1010", "ports:1001..1008,1011..1100", Map("internal" -> -1, "jmx" -> 1001, "cql" -> 1005, "thrift" -> -1))

    // port ranges
    test("internal=10..20", "ports:15..25", Map("internal" -> 15))
    test("internal=10..20,jmx=100..200", "ports:15..25,150..160", Map("internal" -> 15, "jmx" -> 150))
  }

  @Test
  def newTask {
    val node = new Node("0")
    node.cpu = 0.1
    node.mem = 500
    node.runtime = new Node.Runtime(node, offer(resources = "cpus:1;mem:1000;ports:0..10"))

    val task: TaskInfo = node.newTask()
    assertEquals(node.runtime.taskId, task.getTaskId.getValue)
    assertEquals(node.runtime.taskId, task.getName)
    assertEquals(node.runtime.slaveId, task.getSlaveId.getValue)

    val data: String = task.getData.toStringUtf8
    val read: Node = new Node(Util.parseJsonAsMap(data), expanded = true)
    assertEquals(node, read)

    assertEquals(resources("cpus:0.1; mem:500; ports:0..0; ports:1..1; ports:2..2; ports:3..3"), task.getResourcesList)
  }

  @Test
  def newExecutor {
    val node = new Node("0")
    node.runtime = new Node.Runtime(node, offer())

    val executor: ExecutorInfo = node.newExecutor()
    assertEquals(node.runtime.executorId, executor.getExecutorId.getValue)
    assertEquals(node.runtime.executorId, executor.getName)

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

    deferStateSwitch(Node.State.Running, 100)
    assertTrue(node.waitFor(Node.State.Running, Duration("200ms")))

    deferStateSwitch(Node.State.Inactive, 100)
    assertTrue(node.waitFor(Node.State.Inactive, Duration("200ms")))

    // timeout
    assertFalse(node.waitFor(Node.State.Running, Duration("50ms")))
  }

  @Test
  def toJSON_fromJSON {
    val node: Node = new Node("1")
    var read = new Node(Util.parseJsonAsMap("" + node.toJson()))
    assertNodeEquals(node, read)

    node.state = Node.State.Running
    node.runtime = new Node.Runtime("task", "executor", "slave", "host", List("n0", "n1"), new Node.Reservation(), Map("a" -> "1"))

    node.cpu = 1
    node.mem = 1024
    node.broadcast = "127.0.0.1"

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

  @Test
  def Runtime_toJson_fromJson {
    val runtime = new Node.Runtime("task", "executor", "slave", "host", List("n0", "n1"), new Node.Reservation(), Map("a" -> "1"))
    val read = new Node.Runtime(Util.parseJsonAsMap(runtime.toJson.toString()))
    assertRuntimeEquals(runtime, read)
  }

  @Test
  def Reservation_toJson_fromJson {
    val reservation = new Reservation(1.0, 256, Map("internal" -> 7000))
    val read = new Reservation(Util.parseJsonAsMap("" + reservation.toJson))
    assertReservationEquals(reservation, read)
  }

  @Test
  def Reservation_toResources {
    assertEquals(resources("").toList, new Reservation().toResources)
    assertEquals(resources("cpus:0.5;mem:500;ports:1000..1000;ports:2000").toList, new Reservation(0.5, 500, Map("internal" -> 1000, "jmx" -> 2000)).toResources)
  }

  def assertNodeEquals(expected: Node, actual: Node) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.id, actual.id)
    assertEquals(expected.state, actual.state)
    assertRuntimeEquals(expected.runtime, actual.runtime)

    assertEquals(expected.cpu, actual.cpu, 0.001)
    assertEquals(expected.mem, actual.mem)
    assertEquals(expected.broadcast, actual.broadcast)

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

  def assertRuntimeEquals(expected: Node.Runtime, actual: Node.Runtime) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.taskId, actual.taskId)
    assertEquals(expected.executorId, actual.executorId)

    assertEquals(expected.slaveId, actual.slaveId)
    assertEquals(expected.hostname, actual.hostname)

    assertEquals(expected.seeds, actual.seeds)
    assertReservationEquals(expected.reservation, actual.reservation)
    assertEquals(expected.attributes, actual.attributes)
  }

  def assertReservationEquals(expected: Reservation, actual: Reservation) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.cpus, actual.cpus, 0.001)
    assertEquals(expected.mem, actual.mem)
    assertEquals(expected.ports, actual.ports)
  }

  private def checkNulls(expected: Object, actual: Object): Boolean = {
    if (expected == actual) return true
    if (expected == null) throw new AssertionError("actual != null")
    if (actual == null) throw new AssertionError("actual == null")
    false
  }
}
