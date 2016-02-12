package net.elodina.mesos.dse

import net.elodina.mesos.dse.Util.{Range, BindAddress, parseMap}
import org.junit.{Before, Test, After}
import org.junit.Assert._
import net.elodina.mesos.dse.Cli.sendRequest
import java.io._
import org.apache.mesos.Protos.TaskState
import scala.collection.JavaConversions._

class HttpServerTest extends MesosTestCase {
  @Before
  override def before = {
    super.before
    HttpServer.start()
    Nodes.reset()
  }

  @After
  override def after = {
    super.after
    HttpServer.stop()
    Nodes.reset()
  }

  val getPlainResponse = sendRequest(_: String, parseMap(""), urlPathPrefix = "", parseJson = false)
  val getJsonResponse = sendRequest(_: String, _: Map[String, String])

  def assertErrorResponse(req: => Any, code: Int, msg: String): Unit = {
    try{
      req
    } catch {
      case e: IOException => assertEquals(e.getMessage, s"$code - $msg")
    }
  }

  private def makeTempFile(filename: String) = {
    val file = new File(filename)
    val content = "content"
    val tmpWriter = new PrintWriter(file)
    tmpWriter.write(content)
    tmpWriter.flush()
    tmpWriter.close()

    (file, content)
  }

  @Test
  def health() = {
    val response = getPlainResponse("health")
    assertEquals(response, "ok")
  }

  @Test
  def downloadFileEndpoints() = {
    val fileEndpoints = List("jar", "dse", "cassandra", "jre")

    for(fileEndpoint <- fileEndpoints) {
      val (file, content) = makeTempFile(s"tmp_${fileEndpoint}_file.jar")
      Config.jar = file
      Config.dse = file
      Config.cassandra = file
      Config.jre = file
      val response = getPlainResponse(s"$fileEndpoint/")
      file.delete()

      assertEquals(content, response)
    }
  }

  @Test
  def nodeApiList() = {
    val nodeId = "0"
    val node = new Node(nodeId)
    Nodes.addNode(node)
    val response = getJsonResponse("/node/list", Map()).asInstanceOf[List[Map[String, Object]]]
    assertEquals(response.size, 1)
  }

  @Test
  def nodeApiAddUpdate() = {
    type JsonNodes = List[Map[String, Object]]
    val nodeAdd = getJsonResponse("/node/add", _: Map[String, String])

    assertErrorResponse(nodeAdd(Map("node" -> "")), 400, "node required")

    // check simple use case, when only node id is given
    {
      val response = nodeAdd(Map("node" -> "0"))
      assertEquals(Nodes.getNodes.size, 1)
      assertEquals(response.asInstanceOf[JsonNodes].head("id"), Nodes.getNode("0").id)
    }

    // check all possible parameters, except 'cluster'
    {
      val parameters = Map(
        "node" -> "1",
        "cpu" -> "1",
        "mem" -> "1",
        "stickinessPeriod" -> "30m",
        "rack" -> "rack",
        "dc" -> "dc",
        "constraints" -> "",
        "seedConstraints" -> "",
        "seed" -> "0.0.0.0",
        "jvmOptions" -> "-Dfile.encoding=UTF8",
        "dataFileDirs" -> "/tmp/datadir",
        "commitLogDir" -> "/tmp/commitlog",
        "savedCachesDir" -> "/tmp/caches",
        "cassandraDotYaml" -> "num_tokens=312",
        "addressDotYaml" -> "stomp_interface=10.1.2.3",
        "cassandraJvmOptions" -> "-Dcassandra.ring_delay=15000, -Dcassandra.replace_address=localhost"
      )

      Nodes.reset()
      val response = nodeAdd(parameters).asInstanceOf[JsonNodes].head
      val node = Nodes.getNode("1")
      assertEquals(Nodes.getNodes.size, 1)
      assertNodeEquals(node, new Node(response, expanded = true))
    }

    val nodeUpdate = getJsonResponse("/node/update", _: Map[String, String])
    // allow update for node in running state

    {
      val node1 = Nodes.getNode("1")
      node1.state = Node.State.RUNNING
      node1.runtime = new Node.Runtime(node1, offer(resources = "cpus:2.0;mem:20480;ports:0..65000"))

      // node could be updated while it is running
      try { nodeUpdate(Map("node" -> "1", "cpu" -> "2.0")) }
      catch { case e: IOException => fail(e.getMessage) }
    }

    // modified flag behaviour

    {
      schedulerDriver.launchedTasks.clear()

      val node2 = Nodes.addNode(new Node("2"))
      node2.state = Node.State.IDLE
      assertEquals("node by default don't has pending update", false, node2.modified)

      // modified flag don't change when node being idle
      nodeUpdate(parseMap("node=2,cpu=2.0"))
      assertEquals(false, node2.modified)

      // starting & task running
      sendRequest("/node/start", parseMap("node=2,timeout=0s"))
      assertEquals(Node.State.STARTING, node2.state)
      assertEquals(false, node2.modified)

      nodeUpdate(parseMap("node=2,cpu=1.5,dataFileDirs=/sstable/xvdv"))

      assertEquals(true, node2.modified)

      Scheduler.acceptOffer(offer(resources = "cpus:2.0;mem:20480;ports:0..65000", hostname = "slave2"))

      // modified reset for starting when task is launched
      assertEquals(false, node2.modified)
      assertEquals(1, schedulerDriver.launchedTasks.size())

      // modification between accepting offer and receiving task status running from executor
      nodeUpdate(parseMap("node=2;dataFileDirs=/sstable/xvdv,/sstable/xvdw", ';'))
      assertEquals(true, node2.modified)

      Scheduler.onTaskStarted(node2, taskStatus(node2.runtime.taskId, TaskState.TASK_RUNNING))
      // still holds modified flag
      assertEquals(true, node2.modified)

      assertEquals(Node.State.RUNNING, node2.state)

      // running
      // running then task lost, thus modified reset
      Scheduler.onTaskStopped(node2, taskStatus(node2.runtime.taskId, TaskState.TASK_LOST))
      assertEquals(false, node2.modified)
      assertEquals(Node.State.STARTING, node2.state)

      // running (modified is false) then update
      Scheduler.acceptOffer(offer(resources = "cpus:2.0;mem:20480;ports:0..65000", hostname = "slave2"))
      Scheduler.onTaskStarted(node2, taskStatus(node2.runtime.taskId, TaskState.TASK_RUNNING))
      assertEquals(Node.State.RUNNING, node2.state)

      nodeUpdate(parseMap("node=2;dataFileDirs=/sstable/xvdv,/sstable/xvdw,/sstable/xvdx", ';'))
      assertEquals(true, node2.modified)

      // stopping
      sendRequest("/node/stop", parseMap("node=2,timeout=0s"))
      assertEquals(Node.State.STOPPING, node2.state)
      Scheduler.onTaskStopped(node2, taskStatus(node2.runtime.taskId, TaskState.TASK_FINISHED))
      assertEquals(false, node2.modified)

      sendRequest("/node/start", parseMap("node=2,timeout=0s"))
      Scheduler.acceptOffer(offer(resources = "cpus:2.0;mem:20480;ports:0..65000", hostname = "slave2"))
      Scheduler.onTaskStarted(node2, taskStatus(node2.runtime.taskId, TaskState.TASK_RUNNING))
      assertEquals(Node.State.RUNNING, node2.state)
      sendRequest("/node/stop", parseMap("node=2,timeout=0s"))
      nodeUpdate(parseMap("node=2;dataFileDirs=/sstable/xvdv,/sstable/xvdw,/sstable/xvdx,/sstable/xvdz", ';'))
      assertEquals(true, node2.modified)
      Scheduler.onTaskStopped(node2, taskStatus(node2.runtime.taskId, TaskState.TASK_FINISHED))
      assertEquals(false, node2.modified)
    }
  }

  @Test
  def nodeApiRemove() = {
    val nodeRemove = getJsonResponse("/node/remove", _: Map[String, String])

    assertErrorResponse(nodeRemove(Map("node" -> "")), 400, "node required")
    assertErrorResponse(nodeRemove(Map("node" -> "+")), 400, "invalid node expr")
    assertErrorResponse(nodeRemove(Map("node" -> "1")), 400, "node 1 not found")

    val id = "1"
    val node = new Node(id)
    node.state = Node.State.RUNNING
    Nodes.addNode(node)
    Nodes.save()

    assertErrorResponse(nodeRemove(Map("node" -> "1")), 400, s"node $id should be idle")

    node.state = Node.State.IDLE
    nodeRemove(Map("node" -> "1"))
    assertEquals(Nodes.getNodes.size, 0)
  }

  @Test
  def nodeApiStart() = {
    val nodeStart = getJsonResponse("/node/start", _: Map[String, String])

    assertErrorResponse(nodeStart(Map("node" -> "")), 400, "node required")
    assertErrorResponse(nodeStart(Map("node" -> "+")), 400, "invalid node expr")
    assertErrorResponse(nodeStart(Map("node" -> "1")), 400, "node 1 not found")

    val id = "1"
    val node = new Node(id)
    Nodes.addNode(node)
    Nodes.save()

    assertErrorResponse(nodeStart(Map("node" -> id, "timeout" -> "+")), 400, "invalid timeout")

    val response = nodeStart(Map("node" -> id, "timeout" -> "0 ms")).asInstanceOf[Map[String, Any]]

    assertTrue(Nodes.getNodes.forall(_.state == Node.State.STARTING))
    assertEquals(response("nodes").asInstanceOf[List[Any]].size, 1)
  }

  @Test(timeout = 6000)
  def nodeApiStop: Unit = {
    val nodeStart = getJsonResponse("/node/start", _: Map[String, String])
    val nodeStop = getJsonResponse("/node/stop", _: Map[String, String])

    Nodes.reset()

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

    val node0 = Nodes.addNode(new Node("0"))
    nodeStart(parseMap("node=0,timeout=1s"))

    // no offers or offers that don't have required resource
    // low cpu
    Scheduler.acceptOffer(offer(resources = "cpus:0.1;mem:128;ports:0..65000"))
    // low memory
    Scheduler.acceptOffer(offer(resources = "cpus:2.0;mem:128;ports:0..65000"))
    // missing ports
    Scheduler.acceptOffer(offer(resources = "cpus:2.0;mem:128;ports:0..1"))

    // for sure response status it timeout
    nodeStop(parseMap("node=0,timeout=1s"))
    assertEquals(Node.State.IDLE, node0.state)
    // kill task sent only when node has runtime
    assertEquals(0, schedulerDriver.killedTasks.size())

    // has runtime
    nodeStart(parseMap("node=0,timeout=0s"))
    started(node0, immediately = true)

    delay("100ms") { stopped(node0) }
    nodeStop(parseMap("node=0,timeout=1s"))

    // ability to send kill task multiple times
    nodeStart(parseMap("node=0,timeout=0s"))
    started(node0, immediately = true)

    assertDifference(schedulerDriver.killedTasks.size(), 3) {
      nodeStop(parseMap("node=0,timeout=0s"))
      nodeStop(parseMap("node=0,timeout=0s"))
      nodeStop(parseMap("node=0,timeout=0s"))
      stopped(node0)
    }

    // status is disconnected when trying to stop node while scheduler disconnected from master
    nodeStart(parseMap("node=0,timeout=1s"))
    started(node0, immediately = true)

    Scheduler.disconnected(schedulerDriver)
    val json = nodeStop(parseMap("node=0,timeout=0s")).asInstanceOf[Map[String, Any]]
    assertEquals("disconnected", json("status").asInstanceOf[String])
    assertEquals(1, json("nodes").asInstanceOf[List[Map[String, Any]]].size)
    assertEquals(Node.State.RUNNING, node0.state)

    // what to do when has runtime but not received task on failed/finished/...
  }

  @Test
  def clusterApiList() = {
    val response = getJsonResponse("/cluster/list", Map())

    assertEquals(List(Map("id" -> "default", "ports" -> Map())),
      response.asInstanceOf[List[Map[String, Any]]])
  }

  @Test
  def clusterApiAdd() = {
    def removeCluster(id: String) = {
      Nodes.removeCluster(Nodes.getCluster(id))
      Nodes.save()
    }

    val addCluster = getJsonResponse("/cluster/add", _: Map[String, String]).asInstanceOf[Map[String, Any]]
    val clusterId = "test cluster"

    assertErrorResponse(addCluster(Map()), 400, "cluster required")

    {
      val response = addCluster(Map("cluster" -> clusterId))
      assertEquals(Map("id" -> clusterId, "ports" -> Map()), response)
    }

    // bind address

    // FIXME: actual response is {"id" : "test cluster", "bindAddress" : "+", "ports" : {}}, endpoint doesn't return 400 error
    // in case when bind address is wrong
    // FIXME: uncomment next verification when bind address checking will be fixed
    // removeCluster(clusterId)
    // val wrongBindAddress = "+"
    // assertErrorResponse(addCluster(Map("bindAddress" -> wrongBindAddress)), 400, "invalid bindAddress")

    val correctBindAddress = "0.0.0.0"
    removeCluster(clusterId)
    assertEquals(
      Map("id" -> clusterId, "bindAddress" -> correctBindAddress, "ports" -> Map()),
      addCluster(Map("cluster" -> clusterId, "bindAddress" -> correctBindAddress)))

    // map of tested ports, format:
    // Map(portName -> Tuple3(propertyName, wrongValue, correctValue))
    val testedPorts = Map(
      "storagePort" -> ("storage", "+", "10000..11000"),
      "jmxPort" -> ("jmx", "+", "11111"),
      "cqlPort" -> ("cql", "+", "11111"),
      "thriftPort" -> ("thrift", "+", "11111"),
      "agentPort" -> ("agent", "+", "11111")
    )

    for((portName, (propName, wrong, correct)) <- testedPorts) {
      removeCluster(clusterId)
      assertErrorResponse(
        addCluster(Map(portName -> wrong, "cluster" -> clusterId)), 400, s"invalid $portName")

      assertEquals(
        Map("id" -> clusterId, "ports" -> Map(propName -> correct)),
        addCluster(Map(portName -> correct, "cluster" -> clusterId)))
    }

    assertErrorResponse(addCluster(Map("cluster" -> clusterId)), 400, "duplicate cluster")
  }

  @Test
  def clusterApiUpdate() = {
    val updateCluster = getJsonResponse("/cluster/update", _: Map[String, String])
    val clusterId = "test cluster"
    val cluster = new Cluster(clusterId)
    val (bindAddress, storagePort, jmxPort, cqlPort, thriftPort, agentPort) = ("0.0.0.0", "1111", "1111", "1111", "1111", "1111")
    cluster.bindAddress = new BindAddress(bindAddress)
    cluster.ports(Node.Port.STORAGE) = new Range(storagePort)
    cluster.ports(Node.Port.JMX) = new Range(jmxPort)
    cluster.ports(Node.Port.CQL) = new Range(cqlPort)
    cluster.ports(Node.Port.THRIFT) = new Range(thriftPort)
    cluster.ports(Node.Port.AGENT) = new Range(agentPort)

    Nodes.addCluster(cluster)
    Nodes.save()

    // bind address
    val newBindAddress = "127.0.0.1"
    updateCluster(Map("bindAddress" -> newBindAddress, "cluster" -> clusterId))
    assertEquals(cluster.bindAddress.toString, newBindAddress)

    val ports = Map(
      "storagePort" -> (Node.Port.STORAGE, "2222"),
      "jmxPort" -> (Node.Port.JMX, "2222"),
      "cqlPort" -> (Node.Port.CQL, "2222"),
      "thriftPort"  ->(Node.Port.THRIFT, "2222"),
      "agentPort" -> (Node.Port.AGENT, "2222")
    )

    for ((portName, (portType, newPortValue)) <- ports) {
      updateCluster(Map(portName -> newPortValue, "cluster" -> clusterId))
      assertEquals(cluster.ports(portType).toString, newPortValue)
    }
  }

  @Test
  def clusterApiRemove() = {
    val removeCluster = getJsonResponse("/cluster/remove", _: Map[String, String])

    assertErrorResponse(removeCluster(Map()), 400, "cluster required")
    assertErrorResponse(removeCluster(Map("cluster" -> "noneExistent")), 400, "cluster not found")
    assertErrorResponse(removeCluster(Map("cluster" -> "default")), 400, "can't remove default cluster")

    val cluster = new Cluster("test cluster")
    val node = new Node("0")
    node.cluster = cluster
    node.state = Node.State.RUNNING
    Nodes.addCluster(cluster)
    Nodes.addNode(node)
    Nodes.save()

    assertErrorResponse(removeCluster(Map("cluster" -> cluster.id)), 400, "can't remove cluster with active nodes")

    node.state = Node.State.IDLE
    Nodes.save()
    removeCluster(Map("cluster" -> cluster.id))
    assertEquals(Nodes.getCluster(cluster.id), null)
  }
}
