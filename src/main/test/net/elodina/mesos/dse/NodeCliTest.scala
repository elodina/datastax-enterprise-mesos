package net.elodina.mesos.dse

import net.elodina.mesos.dse.Node.Failover
import net.elodina.mesos.util.Period
import org.junit.{Test, Before, After}
import org.junit.Assert._
import scala.concurrent.duration._
import org.apache.mesos.Protos.TaskState
import scala.collection.JavaConversions._


class NodeCliTest extends MesosTestCase with CliTestCase {
  def cli = NodeCli.handle(_: Array[String])

  @Before
  override def before {
    super.before
    Nodes.reset()
    HttpServer.start()
  }

  @After
  override def after {
    super.after
    Nodes.reset()
    HttpServer.stop()
  }

  @Test
  def handle {
    assertCliError(Array(), "command required")

    val argumentRequiredCommands = List("add", "update", "remove", "start", "stop")
    for(command <- argumentRequiredCommands) assertCliError(Array(command), "argument required")

    assertCliError(Array("wrong_command", "arg"), "unsupported node command wrong_command")
  }

  @Test
  def handleList {
    assertCliResponse(Array("list"), "no nodes")

    val node = new Node("0")
    Nodes.addNode(node)
    Nodes.save()
    val response = "node:\n" + outputToString { NodeCli.printNode(node, 1) }
    assertCliResponse(Array("list"), response)

    // communicate back to user that node has pending update
    for(modified <- Seq(true, false)) {
      node.modified = modified
      assertEquals(modified, outputToString { NodeCli.printNode(node, 1) }.contains("(modified, needs restart)"))
    }
  }

  @Test
  def handleAddUpdate() = {
    {
      val node = new Node("0")
      Nodes.addNode(node)
      Nodes.save()
      val defaultAddNodeResponse = "node added:\n" + outputToString { NodeCli.printNode(node, 1) }
      Nodes.removeNode(node)
      Nodes.save()

      assertCliResponse(Array("add", "0"), defaultAddNodeResponse)
      assertEquals(Nodes.getNodes.size, 1)

      val cluster = new Cluster("test-cluster")
      val args = Array("update", "0", "--cluster", cluster.id)

      assertCliError(args, "cluster not found")

      Nodes.addCluster(cluster)
      Nodes.save()
      cli(args)
      assertEquals(cluster.id, Nodes.getNode("0").cluster.id)
    }

    val options = Array(
      "--cpu", "10",
      "--mem", "10",
      "--stickiness-period", "60m",
      "--rack", "rack",
      "--dc", "dc",
      "--seed", "true",
      "--jvm-options", "-Dfile.encoding=UTF8",
      "--data-file-dirs", "/tmp/datadir",
      "--commit-log-dir", "/tmp/commitlog",
      "--saved-caches-dir", "/tmp/caches",
      "--cassandra-yaml-configs", "num_tokens=312,hinted_handoff=false",
      "--address-yaml-configs", "stomp_interface=11.22.33.44",
      "--cassandra-jvm-options", "-Dcassandra.replace_address=127.0.0.1 -Dcassandra.ring_delay_ms=15000"
    )
    cli(Array("update", "0") ++ options)

    val node = Nodes.getNode("0")
    assertEquals(10.0, node.cpu, 0.001)
    assertEquals(10, node.mem)
    assertEquals("60m", node.stickiness.period.toString)
    assertEquals("rack", node.rack)
    assertEquals("dc", node.dc)
    assertEquals(true, node.seed)
    assertEquals("-Dfile.encoding=UTF8", node.jvmOptions)
    assertEquals("/tmp/datadir", node.dataFileDirs)
    assertEquals("/tmp/commitlog", node.commitLogDir)
    assertEquals("/tmp/caches", node.savedCachesDir)
    assertEquals(Map("num_tokens" -> "312", "hinted_handoff" -> "false"), node.cassandraDotYaml.toMap)
    assertEquals(Map("stomp_interface" -> "11.22.33.44"), node.addressDotYaml.toMap)
    assertEquals("-Dcassandra.replace_address=127.0.0.1 -Dcassandra.ring_delay_ms=15000", node.cassandraJvmOptions)
  }

  @Test
  def handleAddUpdate_failover: Unit = {
    val outputAdd = outputToString { cli("add 0 --cpu 1.0 --mem 1024 --failover-delay 2m --failover-max-delay 25m --failover-max-tries 9".split(" ")) }

    val node = Nodes.getNode("0")
    assertFailoverEquals(new Failover(new Period("2m"), new Period("25m"), 9), node.failover)
    assertTrue(outputAdd.contains("delay:2m, max-delay:25m, max-tries:9"))

    cli("update 0 --failover-delay 5m --failover-max-delay 45m --failover-max-tries 7".split(" "))
    assertFailoverEquals(new Failover(new Period("5m"), new Period("45m"), 7), node.failover)

    // reset failover max tries
    cli("update 0 --failover-max-tries".split(" ") ++ Array(""))
    assertNull(node.failover.maxTries)
  }

  @Test
  def handleRemove {
    assertCliError(Array("remove", ""), "node required")
    assertCliError(Array("remove", "+"), "invalid node expr")
    assertCliError(Array("remove", "0"), "node 0 not found")

    val node = new Node("0")
    node.state = Node.State.RUNNING
    Nodes.addNode(node)
    Nodes.save()
    assertCliError(Array("remove", "0"), "node 0 should be idle")

    node.state = Node.State.IDLE
    Nodes.save()
    assertCliResponse(Array("remove", "0"), "node removed")
  }

  @Test
  def handleStartStop {
    assertCliError(Array("start", ""), "node required")
    assertCliError(Array("start", "+"), "invalid node expr")
    assertCliError(Array("start", "0"), "node 0 not found")

    val id = "0"
    val node = new Node(id)
    Nodes.addNode(node)
    Nodes.save()

    assertCliError(Array("start", "0", "--timeout", "+"), "invalid timeout")

    val actualResponse = outputToString { cli(Array("start", "0", "--timeout", "0ms")) }
    val expectedResponse = "node scheduled to start:\n" + outputToString { NodeCli.printNode(node, 1) } + "\n"
    assertEquals(expectedResponse, actualResponse)
    assertTrue(Nodes.getNodes.forall(_.state == Node.State.STARTING))

    // scheduler disconnected from the master
    import Node.State._
    started(node, immediately = true)
    Scheduler.disconnected(schedulerDriver)
    assertCliErrorContains("stop 0 --timeout 1s".split(" "), "scheduler disconnected from the master")
    assertEquals(RUNNING, node.state)
  }

  def assertCliErrorContains(args: Array[String], str: String) = {
    try { cli(args); fail() }
    catch { case e: Cli.Error => assertTrue(s"${args.mkString(" ")} has to contain '$str' in '${e.getMessage}'", e.getMessage.contains(str)) }
  }

  def stopped(node: Node) = {
    assertEquals(Node.State.STOPPING, node.state)
    Scheduler.onTaskStopped(node, taskStatus(node.runtime.taskId, TaskState.TASK_FINISHED))
    assertEquals(Node.State.IDLE, node.state)
    assertNull(node.runtime)
  }

  def started(node: Node, immediately: Boolean = false) = {
    assertEquals(Node.State.STARTING, node.state)
    Scheduler.resourceOffers(schedulerDriver, List(offer(resources = "cpus:2.0;mem:20480;ports:0..65000")))
    def confirm = {
      Scheduler.onTaskStarted(node, taskStatus(node.runtime.taskId, TaskState.TASK_RUNNING))
      assertEquals(Node.State.RUNNING, node.state)
    }
    if (immediately) confirm
    else delay("100ms") { confirm }
  }

  @Test(timeout = 9000)
  def handleRestart {
    Nodes.reset()

    // help
    val helpNodeContent = outputToString { Cli.exec("help node".split(" ")) }
    assert(helpNodeContent.contains("restart"), "has restart cmd")
    assert(helpNodeContent.contains("restart node"), "describe restart cmd")

    try { Cli.exec("help node restart".split(" ")) }
    catch { case e: Cli.Error => fail(e.message) }

    val helpContent = outputToString { Cli.exec("help node restart".split(" ")) }
    assert(helpContent.contains("Restart node"), "has title")
    assert(helpContent.contains("Usage: node restart <id> [options]"), "has usage")
    assert(helpContent.contains("--timeout"), "has --timeout option")

    // node have to exist
    assertCliErrorContains("restart 0".split(" "), "node 0 not found")

    val node0 = Nodes.addNode(new Node("0"))

    // node have to be not in idle and not in reconciling state to be restarted
    import Node.State._
    for(state <- Seq(IDLE, RECONCILING)) {
      node0.state = state
      assertCliErrorContains("restart 0".split(" "), s"node 0 is $state")
    }

    node0.state = Node.State.RUNNING
    node0.runtime = new Node.Runtime(node0, offer())
    delay("200ms") {
      assert(schedulerDriver.killedTasks.contains(node0.runtime.taskId))
      schedulerDriver.killedTasks.clear()

      stopped(node0)
    }
    delay("400ms") {
      started(node0)
    }

    var output = outputToString(cli("restart 0 --timeout 1s".split(" ")))
    assertTrue(output.contains("node restarted:"))
    assertTrue(output.contains("id: 0"))

    // two nodes
    val node1 = Nodes.addNode(new Node("1"))
    node1.state = Node.State.RUNNING
    node1.runtime = new Node.Runtime(node1, offer())

    // 0..1 restart
    // 0 stop timeout
    delay("100ms") {
      // http server changed node state from running to stopping
      assertEquals(Node.State.STOPPING, node0.state)
      // executor doesn't send status update, thus runtime remains not null
      assertNotNull(node0.runtime)
    }
    assertCliErrorContains("restart 0..1 --timeout 300ms".split(" "), "node 0 timeout on stop")

    // 0 start timeout
    node0.state = RUNNING
    delay("100ms") {
      stopped(node0)

      delay("100ms") { assertEquals(Node.State.STARTING, node0.state) }
    }
    assertCliErrorContains("restart 0..1 --timeout 500ms".split(" "), "node 0 timeout on start")

    // 0 stop & start ok, but 1 is starting
    started(node0)
    node0.waitFor(RUNNING, Duration("200ms"))

    delay("100ms") {
      stopped(node0)
      delay("100ms") {
        // something happened with node1 while node0 was about to get starting
        Scheduler.onTaskStopped(node1, taskStatus(node1.runtime.taskId, TaskState.TASK_FAILED))
        assertEquals(STARTING, node1.state)

        started(node0)
      }
    }
    assertCliErrorContains("restart 0..1 --timeout 1s".split(" "), "node 1 timeout on start")

    // 0 stop & start ok, but 1 is idle (perhaps reached max tries)
    delay("100ms") {
      stopped(node0)
      delay("100ms") {
        // something happened with node1 while node0 was about to get starting
        Scheduler.stopNode(node1.id)
        assertEquals(IDLE, node1.state)

        started(node0)
      }
    }
    assertCliErrorContains("restart 0..1 --timeout 1s".split(" "), "node 1 is idle")

    // 0 stop & start ok, but 1 is idle (perhaps reached max tries)
    node1.state = STARTING
    started(node1, immediately = true)

    delay("100ms") {
      stopped(node0)
      delay("100ms") {
        // something happened with node1 while node0 was has started
        started(node0, immediately = true)

        node1.state = RECONCILING
      }
    }
    assertCliErrorContains("restart 0..1 --timeout 1s".split(" "), "node 1 is reconciling")
    Scheduler.onTaskStopped(node1, taskStatus(node1.runtime.taskId, TaskState.TASK_KILLED))

    // 1 stop timeout
    node1.state = STARTING
    started(node1, immediately = true)
    assertEquals(RUNNING, node0.state)

    delay("100ms") {
      stopped(node0)
      delay("100ms") {
        started(node0)
      }
    }
    assertCliErrorContains("restart 0..1 --timeout 1s".split(" "), "node 1 timeout on stop")

    // 1 start timeout
    stopped(node1)
    node1.state = STARTING
    started(node1)
    node1.waitFor(RUNNING, Duration("200ms"))
    assertEquals(RUNNING, node1.state)

    delay("100ms") {
      stopped(node0)
      delay("100ms") {
        started(node0)
        delay("250ms") {
          stopped(node1)
        }
      }
    }
    assertCliErrorContains("restart 0..1 --timeout 1s".split(" "), "node 1 timeout on start")

    // both success restart
    started(node1, immediately = true)
    assertEquals(RUNNING, node1.state)

    delay("100ms") {
      stopped(node0)
      delay("100ms") {
        started(node0, immediately = true)
        delay("200ms") {
          stopped(node1)
          delay("100ms") {
            started(node1)
          }
        }
      }
    }

    output = outputToString(cli("restart 0..1 --timeout 1s".split(" ")))
    assertTrue(output.contains("nodes restarted:"))
    assertTrue(output.contains("id: 0"))
    assertTrue(output.contains("id: 1"))

    // scheduler disconnected from the master
    Scheduler.disconnected(schedulerDriver)
    assertCliErrorContains("restart 0..1 --timeout 1s".split(" "), "scheduler disconnected from the master")
    assertEquals(RUNNING, node0.state)
  }

  @Test
  def handleRestart_progress: Unit = {
    val node0 = Nodes.addNode(new Node("0"))

    node0.state = Node.State.STARTING
    started(node0, immediately = true)

    val baos = new java.io.ByteArrayOutputStream()
    val out = new java.io.PrintStream(baos, false)

    def capture(cmd: => Unit) = {
      out.flush()
      baos.reset()
      Cli.out = out
      try {
        cmd
      } finally {
        Cli.out = System.out
      }
      baos.toString
    }

    def captured = {
      out.flush()
      baos.toString
    }

    delay("450ms") {
      assertTrue(captured == "stopping node 0 ... ")

      stopped(node0)
    }

    delay("550ms") {
      assertTrue(captured == "stopping node 0 ... done\nstarting node 0 ... ")

      started(node0)
    }

    capture { cli("restart 0 --timeout 1s".split(" ")) }

    assertTrue(captured.startsWith("stopping node 0 ... done\nstarting node 0 ... done\n"))
  }
}
