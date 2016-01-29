package net.elodina.mesos.dse

import org.junit.{Test, Before, After}
import org.junit.Assert._


class NodeCliTest extends MesosTestCase with CliTestCase {
  def cli = NodeCli.handle(_: Array[String])

  @Before
  override def before = {
    super.before
    Nodes.frameworkState.reset()
    HttpServer.start()
  }

  @After
  override def after = {
    super.after
    Nodes.frameworkState.reset()
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
      "--replace-address", "1.1.1.1",
      "--jvm-options", "-Dfile.encoding=UTF8",
      "--data-file-dirs", "/tmp/datadir",
      "--commit-log-dir", "/tmp/commitlog",
      "--saved-caches-dir", "/tmp/caches"
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
      assertEquals(node.replaceAddress, "1.1.1.1")
      assertEquals(node.jvmOptions, "-Dfile.encoding=UTF8")
      assertEquals(node.dataFileDirs, "/tmp/datadir")
      assertEquals(node.commitLogDir, "/tmp/commitlog")
      assertEquals(node.savedCachesDir, "/tmp/caches")
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
}
