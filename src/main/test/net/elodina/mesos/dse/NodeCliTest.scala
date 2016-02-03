package net.elodina.mesos.dse

import org.junit.{Test, Before, After}
import org.junit.Assert._
import scala.concurrent.duration._
import org.apache.mesos.Protos.TaskState


class NodeCliTest extends MesosTestCase with CliTestCase {
  def cli = NodeCli.handle(_: Array[String])

  @Before
  override def before = {
    super.before
    Nodes.reset()
    HttpServer.start()
  }

  @After
  override def after = {
    super.after
    Nodes.reset()
    HttpServer.stop()
  }

  @Test
  def handle() = {
    assertCliError(Array(), "command required")

    val argumentRequiredCommands = List("add", "update", "remove", "start", "stop")
    for(command <- argumentRequiredCommands) assertCliError(Array(command), "argument required")

    assertCliError(Array("wrong_command", "arg"), "unsupported node command wrong_command")
  }

  @Test
  def handleList() = {
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
      "--cassandra-jvm-options", "-Dcassandra.replace_address=127.0.0.1 -Dcassandra.ring_delay_ms=15000"
    )
    cli(Array("update", "0") ++ options)

    {
      val node = Nodes.getNode("0")
      assertEquals(node.cpu, 10.0, 0)
      assertEquals(node.mem, 10.0, 0)
      assertEquals(node.stickiness.period.toString, "60m")
      assertEquals(node.rack, "rack")
      assertEquals(node.dc, "dc")
      assertEquals(node.seed, true)
      assertEquals(node.jvmOptions, "-Dfile.encoding=UTF8")
      assertEquals(node.dataFileDirs, "/tmp/datadir")
      assertEquals(node.commitLogDir, "/tmp/commitlog")
      assertEquals(node.savedCachesDir, "/tmp/caches")
      assertEquals(node.cassandraDotYaml.toMap, Map("num_tokens" -> "312", "hinted_handoff" -> "false"))
      assertEquals(node.cassandraJvmOptions, "-Dcassandra.replace_address=127.0.0.1 -Dcassandra.ring_delay_ms=15000")
    }

  }

  @Test
  def handleRemove() = {
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
  def handleStart() = {
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
  }

  @Test(timeout = 8000)
  def handleRestart(): Unit = {
    Nodes.reset()

    def assertCliErrorContains(args: Array[String], str: String) = {
      try { cli(args) }
      catch { case e: Cli.Error => assertTrue(s"${args.mkString(" ")} has to contain '$str' in '${e.getMessage}'", e.getMessage.contains(str)) }
    }

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

    // node have to be running
    import Node.State._
    for(state <- Seq(IDLE, STARTING, RUNNING, STOPPING, RECONCILING) if state != RUNNING)
      assertCliErrorContains("restart 0".split(" "), "node 0 should be running")

    def delay(duration: String = "100ms")(f: => Unit) = new Thread {
      override def run(): Unit = {
        Thread.sleep(Duration(duration).toMillis)
        f
      }
    }.start()

    def stopped(node: Node) = {
      assertEquals(Node.State.STOPPING, node.state)
      Scheduler.onTaskStopped(node, taskStatus(node.runtime.taskId, TaskState.TASK_FINISHED))
      assertEquals(Node.State.IDLE, node.state)
      assertNull(node.runtime)
    }
    def started(node: Node, immediately: Boolean = false) = {
      assertEquals(Node.State.STARTING, node.state)
      Scheduler.acceptOffer(offer(resources = "cpus:2.0;mem:20480;ports:0..65000"))
      def confirm = {
        Scheduler.onTaskStarted(node, taskStatus(node.runtime.taskId, TaskState.TASK_RUNNING))
        assertEquals(Node.State.RUNNING, node.state)
      }
      if (immediately) confirm
      else delay("100ms") { confirm }
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
    assertEquals("node restarted:\n" + outputToString { NodeCli.printNode(node0, 1) } + "\n", output)

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

    // 0 stop & start ok, but 1 is not running
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
    assertCliErrorContains("restart 0..1 --timeout 1s".split(" "), "node 1 should be running")

    // 1 stop timeout
    started(node1)
    node1.waitFor(RUNNING, Duration("200ms"))

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
        delay("200ms") {
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
        delay("100ms") {
          stopped(node1)
          delay("100ms") {
            started(node1)
          }
        }
      }
    }

    output = outputToString(cli("restart 0..1 --timeout 1s".split(" ")))
    assertEquals("nodes restarted:\n" +
      outputToString { NodeCli.printNode(node0, 1) } +
      "\n" +
      outputToString { NodeCli.printNode(node1, 1) } +
      "\n", output)
  }
}
