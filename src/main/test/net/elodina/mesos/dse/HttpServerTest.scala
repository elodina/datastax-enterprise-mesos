package net.elodina.mesos.dse

import javax.servlet.ServletOutputStream

import net.elodina.mesos.dse.Util.{Range, BindAddress}
import org.junit.{Before, Test}
import org.junit.Assert._
import net.elodina.mesos.dse.HttpServer.{Servlet}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.mockito.Mockito._
import java.io._

import scala.util.parsing.json.{JSONObject, JSONArray}


class StubServletOutputStream extends ServletOutputStream {
  val baos = new ByteArrayOutputStream

  def write(i: Int) = {
    baos.write(i)
  }

  def reset() = baos.reset()

  def size() = baos.size()

  override def toString = baos.toString
}

class HttpServerTest extends MesosTestCase {
  val stringWriter = new StringWriter()
  val writer = new PrintWriter(stringWriter)
  val servletOutputStream = new StubServletOutputStream

  private def clearResponse: Unit = {
    stringWriter.getBuffer.setLength(0)
  }

  @Before
  override def before = {
    super.before
    clearResponse
    Nodes.reset()
  }

  private def makeRequestResponse(url: String) = {
    val request = mock(classOf[HttpServletRequest])
    when(request.getRequestURI).thenReturn(url)
    val response = mock(classOf[HttpServletResponse])
    when(response.getWriter).thenReturn(writer)
    servletOutputStream.reset()
    when(response.getOutputStream).thenReturn(servletOutputStream)

    (request, response)
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
    val (request, response) = makeRequestResponse("/health")

    Servlet.doGet(request, response)

    verify(response).setContentType("text/plain; charset=utf-8")
    writer.flush()
    assertTrue(stringWriter.toString.contains("ok"))
  }

  @Test
  def downloadFileJar() = {
    val (request, response) = makeRequestResponse("/jar/")
    val (file, content) = makeTempFile("tmp_jar_file.jar")
    Config.jar = file

    Servlet.doGet(request, response)
    file.delete()

    verify(response).setContentType("application/zip")
    verify(response).setHeader("Content-Length", content.length.toString)
    verify(response).setHeader("Content-Disposition", s"""attachment; filename="${file.getName}"""")
    assertEquals(servletOutputStream.toString, content)
  }

  @Test
  def downloadFileDse() = {
    val (request, response) = makeRequestResponse("/dse/")
    val (file, content) = makeTempFile("tmp_dse_file.tar.gz")
    Config.dse = file

    Servlet.doGet(request, response)
    file.delete()

    verify(response).setContentType("application/zip")
    verify(response).setHeader("Content-Length", content.length.toString)
    verify(response).setHeader("Content-Disposition", s"""attachment; filename="${file.getName}"""")
    assertEquals(servletOutputStream.toString, content)


    Config.dse = null
    Servlet.doGet(request, response)
    verify(response).sendError(404, "not found")
  }

  @Test
  def downloadFileCassandra() = {
    val (request, response) = makeRequestResponse("/cassandra/")
    val (file, content) = makeTempFile("tmp_cassandra_file.tar.gz")
    Config.cassandra = file

    Servlet.doGet(request, response)
    file.delete()

    verify(response).setContentType("application/zip")
    verify(response).setHeader("Content-Length", content.length.toString)
    verify(response).setHeader("Content-Disposition", s"""attachment; filename="${file.getName}"""")
    assertEquals(servletOutputStream.toString, content)

    Config.cassandra = null
    Servlet.doGet(request, response)
    verify(response).sendError(404, "not found")
  }

  @Test
  def downloadFileJre() = {
    val (request, response) = makeRequestResponse("/jre/")
    val (file, content) = makeTempFile("tmp_jre_file.tar.gz")
    Config.jre = file

    Servlet.doGet(request, response)
    file.delete()

    verify(response).setContentType("application/zip")
    verify(response).setHeader("Content-Length", content.length.toString)
    verify(response).setHeader("Content-Disposition", s"""attachment; filename="${file.getName}"""")
    assertEquals(servletOutputStream.toString, content)

    Config.jre = null
    Servlet.doGet(request, response)
    verify(response).sendError(404, "not found")
  }

  @Test
  def nodeApiList() = {
    val (request, response) = makeRequestResponse("/api/node/list")
    Nodes.addNode(new Node("0"))
    val nodesJson = Nodes.getNodes.map(_.toJson(expanded = true))

    Servlet.doGet(request, response)

    assertEquals(stringWriter.toString, new JSONArray(nodesJson.toList).toString() + "\n")

    Nodes.reset()
  }

  @Test
  def nodeApiAddUpdate() = {
    val (request, response) = makeRequestResponse("/api/node/add")

    when(request.getParameter("node")).thenReturn("")
    Servlet.doGet(request, response)
    verify(response).sendError(400, "node required")

    // check simple use case, when only node id is given
    when(request.getParameter("node")).thenReturn("0")
    Servlet.doGet(request, response)
    assertEquals(Nodes.getNodes.size, 1)
    assertEquals(new JSONArray(Nodes.getNodes.map(_.toJson(expanded = true))).toString() + "\n", stringWriter.toString)

    // check all possible parameters, except 'cluster'
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
      "replaceAddress" -> "0.0.0.0",
      "jvmOptions" -> "-Dfile.encoding=UTF8",
      "dataFileDirs" -> "/tmp/datadir",
      "commitLogDir" -> "/tmp/commitlog",
      "savedCachesDir" -> "/tmp/caches"
    )

    for ((pKey, pValue) <- parameters) when(request.getParameter(pKey)).thenReturn(pValue.toString)
    clearResponse
    Nodes.reset()
    Servlet.doGet(request, response)
    assertEquals(Nodes.getNodes.size, 1)
    assertEquals(new JSONArray(Nodes.getNodes.map(_.toJson(expanded = true))).toString() + "\n", stringWriter.toString)

  }

  @Test
  def nodeApiRemove() = {
    val (request, response) = makeRequestResponse("/api/node/remove")

    when(request.getParameter("node")).thenReturn("")
    Servlet.doGet(request, response)
    verify(response).sendError(400, "node required")

    when(request.getParameter("node")).thenReturn("+")
    Servlet.doGet(request, response)
    verify(response).sendError(400, "invalid node expr")

    when(request.getParameter("node")).thenReturn("1")
    Servlet.doGet(request, response)
    verify(response).sendError(400, "node 1 not found")

    val id = "1"
    val node = new Node(id)
    node.state = Node.State.RUNNING
    Nodes.addNode(node)
    Nodes.save()
    when(request.getParameter("node")).thenReturn(id)
    Servlet.doGet(request, response)
    verify(response).sendError(400, s"node $id should be idle")

    node.state = Node.State.IDLE
    when(request.getParameter("node")).thenReturn(id)
    Servlet.doGet(request, response)
    assertEquals(Nodes.getNodes.size, 0)
  }

  @Test
  def nodeApiStart() = {
    val (request, response) = makeRequestResponse("/api/node/start")

    when(request.getParameter("node")).thenReturn("")
    Servlet.doGet(request, response)
    verify(response).sendError(400, "node required")

    when(request.getParameter("node")).thenReturn("+")
    Servlet.doGet(request, response)
    verify(response).sendError(400, "invalid node expr")

    when(request.getParameter("node")).thenReturn("1")
    Servlet.doGet(request, response)
    verify(response).sendError(400, "node 1 not found")

    val id = "1"
    val node = new Node(id)
    Nodes.addNode(node)
    Nodes.save()
    when(request.getParameter("node")).thenReturn(id)

    when(request.getParameter("timeout")).thenReturn("+")
    Servlet.doGet(request, response)
    verify(response).sendError(400, "invalid timeout")

    when(request.getParameter("timeout")).thenReturn("0 ms")
    Servlet.doGet(request, response)

    val resultJsonStart = new JSONObject(Map(
      "status" -> "scheduled",
      "nodes" -> new JSONArray(Nodes.getNodes.map(_.toJson(expanded = true)))
    ))
    assertEquals(resultJsonStart + "\n", stringWriter.toString)
  }

  @Test
  def nodeApiStop() = {
    val (request, response) = makeRequestResponse("/api/node/stop")

    when(request.getParameter("node")).thenReturn("")
    Servlet.doGet(request, response)
    verify(response).sendError(400, "node required")

    when(request.getParameter("node")).thenReturn("+")
    Servlet.doGet(request, response)
    verify(response).sendError(400, "invalid node expr")

    when(request.getParameter("node")).thenReturn("1")
    Servlet.doGet(request, response)
    verify(response).sendError(400, "node 1 not found")

    val id = "1"
    val node = new Node(id)
    node.state = Node.State.RUNNING
    Nodes.addNode(node)
    Nodes.save()
    when(request.getParameter("node")).thenReturn(id)

    when(request.getParameter("timeout")).thenReturn("+")
    Servlet.doGet(request, response)
    verify(response).sendError(400, "invalid timeout")

    when(request.getParameter("timeout")).thenReturn("0 ms")
    Servlet.doGet(request, response)

    val resultJsonStart = new JSONObject(Map(
      "status" -> "scheduled",
      "nodes" -> new JSONArray(Nodes.getNodes.map(_.toJson(expanded = true)))
    ))
    assertEquals(resultJsonStart + "\n", stringWriter.toString)
  }

  @Test
  def clusterApiList() = {
    val (request, response) = makeRequestResponse("/api/cluster/list")

    Servlet.doGet(request, response)
    assertEquals("[{\"id\" : \"default\", \"ports\" : {}}]\n", stringWriter.toString)
  }

  @Test
  def clusterApiAdd() = {
    def removeCluster(id: String) = {
      Nodes.removeCluster(Nodes.getCluster(id))
      Nodes.save()
    }

    val (request, response) = makeRequestResponse("/api/cluster/add")

    Servlet.doGet(request, response)
    verify(response).sendError(400, "cluster required")

    val clusterId = "test cluster"
    when(request.getParameter("cluster")).thenReturn(clusterId)
    Servlet.doGet(request, response)
    assertEquals("{\"id\" : \"test cluster\", \"ports\" : {}}\n", stringWriter.toString)

    // bind address
    clearResponse
    removeCluster(clusterId)
    val wrongBindAddress = "+"
    when(request.getParameter("bindAddress")).thenReturn(wrongBindAddress)
    Servlet.doGet(request, response)
    // FIXME: actual response is {"id" : "test cluster", "bindAddress" : "+", "ports" : {}}, endpoint doesn't return 400 error
    // in case when bind address is wrong
    // FIXME: uncomment next verification when bind address checking will be fixed
    // verify(response).sendError(400, "invalid bindAddress")

    val correctBindAddress = "0.0.0.0"
    clearResponse
    removeCluster(clusterId)
    when(request.getParameter("bindAddress")).thenReturn(correctBindAddress)
    Servlet.doGet(request, response)
    assertEquals(
      "{\"id\" : \"test cluster\", \"bindAddress\" : \"0.0.0.0\", \"ports\" : {}}\n",
      stringWriter.toString)

    // storage port
    clearResponse
    removeCluster(clusterId)
    val wrongStoragePort = "+"
    when(request.getParameter("storagePort")).thenReturn(wrongStoragePort)
    Servlet.doGet(request, response)
    verify(response).sendError(400, "invalid storagePort")

    val correctStoragePort = "10000..11000"
    clearResponse
    when(request.getParameter("storagePort")).thenReturn(correctStoragePort)
    Servlet.doGet(request, response)
    assertEquals(
      "{\"id\" : \"test cluster\", \"bindAddress\" : \"0.0.0.0\", \"ports\" : {\"storage\" : \"10000..11000\"}}\n",
      stringWriter.toString)

    // jmx port
    clearResponse
    removeCluster(clusterId)
    val wrongJmxPort = "+"
    when(request.getParameter("jmxPort")).thenReturn(wrongJmxPort)
    Servlet.doGet(request, response)
    verify(response).sendError(400, "invalid jmxPort")

    val correctJmxPort = "11111"
    clearResponse
    when(request.getParameter("jmxPort")).thenReturn(correctJmxPort)
    Servlet.doGet(request, response)
    assertEquals(
      "{\"id\" : \"test cluster\", \"bindAddress\" : \"0.0.0.0\", \"ports\" : {\"storage\" : \"10000..11000\", \"jmx\" : \"11111\"}}\n",
      stringWriter.toString)

    // cqlPort port
    clearResponse
    removeCluster(clusterId)
    val wrongCqlPort = "+"
    when(request.getParameter("cqlPort")).thenReturn(wrongCqlPort)
    Servlet.doGet(request, response)
    verify(response).sendError(400, "invalid cqlPort")

    val correctCqlPort = "11111"
    clearResponse
    when(request.getParameter("cqlPort")).thenReturn(correctCqlPort)
    Servlet.doGet(request, response)
    assertEquals(
      "{\"id\" : \"test cluster\", \"bindAddress\" : \"0.0.0.0\", \"ports\" : {\"cql\" : \"11111\", \"storage\" : \"10000..11000\", \"jmx\" : \"11111\"}}\n",
      stringWriter.toString)

    // thriftPort port
    clearResponse
    removeCluster(clusterId)
    val wrongThriftPort = "+"
    when(request.getParameter("thriftPort")).thenReturn(wrongThriftPort)
    Servlet.doGet(request, response)
    verify(response).sendError(400, "invalid thriftPort")

    val correctThriftPort = "11111"
    clearResponse
    when(request.getParameter("thriftPort")).thenReturn(correctThriftPort)
    Servlet.doGet(request, response)
    assertEquals(
      "{\"id\" : \"test cluster\", \"bindAddress\" : \"0.0.0.0\", \"ports\" : {\"thrift\" : \"11111\", \"cql\" : \"11111\", \"storage\" : \"10000..11000\", \"jmx\" : \"11111\"}}\n",
      stringWriter.toString)

    // agentPort port
    clearResponse
    removeCluster(clusterId)
    val wrongAgentPort = "+"
    when(request.getParameter("agentPort")).thenReturn(wrongAgentPort)
    Servlet.doGet(request, response)
    verify(response).sendError(400, "invalid agentPort")

    val correctAgentPort = "11111"
    clearResponse
    when(request.getParameter("agentPort")).thenReturn(correctAgentPort)
    Servlet.doGet(request, response)
    assertEquals(
      "{\"id\" : \"test cluster\", \"bindAddress\" : \"0.0.0.0\", \"ports\" : {\"cql\" : \"11111\", \"jmx\" : \"11111\", \"agent\" : \"11111\", \"thrift\" : \"11111\", \"storage\" : \"10000..11000\"}}\n",
      stringWriter.toString)


    Servlet.doGet(request, response)
    verify(response).sendError(400, "duplicate cluster")
  }

  @Test
  def clusterApiUpdate() = {
    val (request, response) = makeRequestResponse("/api/cluster/update")
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
    when(request.getParameter("bindAddress")).thenReturn(newBindAddress)
    when(request.getParameter("cluster")).thenReturn(clusterId)
    Servlet.doGet(request, response)
    assertEquals(cluster.bindAddress.toString, newBindAddress)

    val ports = Map(
      "storagePort" ->(Node.Port.STORAGE, "2222"),
      "jmxPort" ->(Node.Port.JMX, "2222"),
      "cqlPort" ->(Node.Port.CQL, "2222"),
      "thriftPort" ->(Node.Port.THRIFT, "2222"),
      "agentPort" ->(Node.Port.AGENT, "2222")
    )

    for ((portName, (portType, newPortValue)) <- ports) {
      when(request.getParameter(portName)).thenReturn(newPortValue)
      when(request.getParameter("cluster")).thenReturn(clusterId)
      Servlet.doGet(request, response)
      assertEquals(cluster.ports(portType).toString, newPortValue)
    }
  }

  @Test
  def clusterApiRemove() = {
    val (request, response) = makeRequestResponse("/api/cluster/remove")

    Servlet.doGet(request, response)
    verify(response).sendError(400, "cluster required")

    val noneExistentClusterId = "noneExistent"
    when(request.getParameter("cluster")).thenReturn(noneExistentClusterId)
    Servlet.doGet(request, response)
    verify(response).sendError(400, "cluster not found")

    val defaultClusterId = "default"
    when(request.getParameter("cluster")).thenReturn(defaultClusterId)
    Servlet.doGet(request, response)
    verify(response).sendError(400, "can't remove default cluster")

    val cluster = new Cluster("test cluster")
    val node = new Node("0")
    node.cluster = cluster
    node.state = Node.State.RUNNING
    Nodes.addCluster(cluster)
    Nodes.addNode(node)
    Nodes.save()

    when(request.getParameter("cluster")).thenReturn(cluster.id)
    Servlet.doGet(request, response)
    verify(response).sendError(400, "can't remove cluster with active nodes")

    node.state = Node.State.IDLE
    Nodes.save()
    when(request.getParameter("cluster")).thenReturn(cluster.id)
    Servlet.doGet(request, response)
    assertEquals(Nodes.getCluster(cluster.id), null)
  }
}
