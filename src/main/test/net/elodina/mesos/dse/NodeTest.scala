package net.elodina.mesos.dse

import org.junit.Test
import org.junit.Assert._
import org.apache.mesos.Protos.{TaskInfo, CommandInfo, ExecutorInfo}

class NodeTest extends MesosTestCase{
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
