package net.elodina.mesos.dse

import org.junit.{Test, Before, After}
import org.junit.Assert._


class ClusterCliTest extends MesosTestCase with CliTestCase {
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

  new Thread() {
    setName("thread-dump-waiter")

    override def run(): Unit = {
      Thread.sleep(3 * 60 * 1000L)
      java.lang.management.ManagementFactory.getThreadMXBean().dumpAllThreads(true, true).foreach(println)
    }
  }.start()

  new Thread() {
    val duration = 4 * 60 * 1000L
    setName(s"system-exit-1-$duration")
    override def run(): Unit = {
      Thread.sleep(duration)
      System.exit(1)
    }
  }.start()

  @Test
  def handleRemove() = {
    import java.util.Date
    println("LOG " + new Date() + " 1 IN handleRemove")
    val cluster = new Cluster("test-cluster")
    Nodes.addCluster(cluster)
    println("LOG " + new Date() + " 2 IN handleRemove")

    assertCliError(Array("remove", ""), "cluster required")
    println("LOG " + new Date() + " 3.1 IN handleRemove")
    assertCliError(Array("remove", "nonexistent"), "cluster not found")
    println("LOG " + new Date() + " 3.2 IN handleRemove")
    assertCliError(Array("remove", "default"), "can't remove default cluster")
    println("LOG " + new Date() + " 3.3 IN handleRemove")

    val node = new Node("0")
    node.cluster = cluster
    node.state = Node.State.RUNNING
    println("LOG " + new Date() + " 4 IN handleRemove")
    Nodes.addNode(node)
    println("LOG " + new Date() + " 5 IN handleRemove")
    Nodes.save()
    println("LOG " + new Date() + " 6 IN handleRemove")
    assertCliError(Array("remove", cluster.id), "can't remove cluster with active nodes")
    println("LOG " + new Date() + " 7 IN handleRemove")

    node.state = Node.State.IDLE
    println("LOG " + new Date() + " 8 IN handleRemove")
    Nodes.save()
    println("LOG " + new Date() + " 9 IN handleRemove")
    assertCliResponse(Array("remove", cluster.id), "cluster removed")
    println("LOG " + new Date() + " 10 IN handleRemove")
  }
}
