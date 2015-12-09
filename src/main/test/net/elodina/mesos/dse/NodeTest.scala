package net.elodina.mesos.dse

import org.junit.Test
import org.junit.Assert._
import org.apache.mesos.Protos.{TaskInfo, CommandInfo, ExecutorInfo}
import scala.concurrent.duration.Duration

class NodeTest extends MesosTestCase {
  @Test
  def matches {
    val node = new Node("0")
    node.cpu = 0.5
    node.mem = 500

    node.storagePort = 0
    node.sslStoragePort = 1
    node.jmxPort = 2
    node.nativeTransportPort = 3
    node.rpcPort = 4

    assertEquals("no cpus", node.matches(offer()))
    assertEquals(s"cpus 0.1 < 0.5", node.matches(offer(resources = "cpus:0.1")))

    assertEquals("no mem", node.matches(offer(resources = "cpus:0.5")))
    assertEquals(s"mem 400 < 500", node.matches(offer(resources = "cpus:0.5; mem:400")))

    assertEquals("no ports", node.matches(offer(resources = "cpus:0.5; mem:500")))
    assertEquals("unavailable port 0", node.matches(offer(resources = "cpus:0.5; mem:500; ports:5..5")))

    assertNull(node.matches(offer(resources = "cpus:0.5; mem:500; ports:0..4")))
  }

  @Test
  def newTask {
    val node = new Node("0")
    node.cpu = 0.1
    node.mem = 500

    node.storagePort = 0
    node.sslStoragePort = 1
    node.jmxPort = 2
    node.nativeTransportPort = 3
    node.rpcPort = 4

    node.runtime = new Node.Runtime(node, offer())

    val task: TaskInfo = node.newTask()
    assertEquals(node.runtime.taskId, task.getTaskId.getValue)
    assertEquals(node.runtime.taskId, task.getName)
    assertEquals(node.runtime.slaveId, task.getSlaveId.getValue)

    val data: String = task.getData.toStringUtf8
    val read: Node = new Node(Util.parseJsonAsMap(data), expanded = true)
    assertEquals(node, read)

    assertEquals(resources("cpus:0.1; mem:500; ports:0..0; ports:1..1; ports:2..2; ports:3..3; ports:4..4"), task.getResourcesList)
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
    node.runtime = new Node.Runtime("task", "executor", "slave", "host", List("n0", "n1"), Map("a" -> "1"))

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

  def Runtime_toJson_fromJson {
    val runtime = new Node.Runtime("task", "executor", "slave", "host", List("n0", "n1"), Map("a" -> "1"))
    val read = new Node.Runtime(Util.parseJsonAsMap(runtime.toJson.toString()))
    assertRuntimeEquals(runtime, read)
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
    assertEquals(expected.attributes, actual.attributes)
  }

  private def checkNulls(expected: Object, actual: Object): Boolean = {
    if (expected == actual) return true
    if (expected == null) throw new AssertionError("actual != null")
    if (actual == null) throw new AssertionError("actual == null")
    false
  }
}
