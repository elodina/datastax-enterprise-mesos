package net.elodina.mesos.dse

import org.junit.{Test, Before, After}
import org.junit.Assert._


class ClusterCliTest extends DseMesosTestCase with CliTestCase {
  def cli = ClusterCli.handle(_: Array[String])

  @Before
  override def before = {
    super.before
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

    val argumentRequiredCommands = List("add", "update", "remove")
    for(command <- argumentRequiredCommands) assertCliError(Array(command), "argument required")

    assertCliError(Array("wrong_command", "arg"), "unsupported cluster command wrong_command")
  }

  @Test
  def handleList() = {
    val defaultCluster = Nodes.getCluster("default")
    val res = "cluster:\n" + outputToString { ClusterCli.printCluster(defaultCluster, 1) }
    assertCliResponse(Array("list"), res)
  }

  @Test
  def handleAddUpdate() = {
    val clusterId = "test-cluster"

    {
      val actualResponse = outputToString { cli(Array("add", clusterId)) }
      val cluster = Nodes.getCluster(clusterId)
      val expectedResponse = "cluster added:\n" + outputToString { ClusterCli.printCluster(cluster, 1) } + "\n"
      assertEquals(expectedResponse, actualResponse)
    }

    {
      val updateArgs = Array("--bind-address", "2.2.2.2", "--jmx-remote", "true", "--storage-port", "9999", "--jmx-port", "666",
        "--cql-port", "777", "--thrift-port", "0000", "--agent-port", "1234"
      )
      val actualResponse = outputToString { cli(Array("update", clusterId) ++ updateArgs) }
      val cluster = Nodes.getCluster(clusterId)
      val expectedResponse = "cluster updated:\n" + outputToString { ClusterCli.printCluster(cluster, 1) } + "\n"
      assertEquals(expectedResponse, actualResponse)
    }
  }

  @Test
  def handleRemove() = {
    val cluster = new Cluster("test-cluster")
    Nodes.addCluster(cluster)

    assertCliError(Array("remove", ""), "cluster required")
    assertCliError(Array("remove", "nonexistent"), "cluster not found")
    assertCliError(Array("remove", "default"), "can't remove default cluster")

    val node = new Node("0")
    node.cluster = cluster
    node.state = Node.State.RUNNING
    Nodes.addNode(node)
    Nodes.save()
    assertCliError(Array("remove", cluster.id), "can't remove cluster with active nodes")

    node.state = Node.State.IDLE
    Nodes.save()
    assertCliResponse(Array("remove", cluster.id), "cluster removed")
  }
}
