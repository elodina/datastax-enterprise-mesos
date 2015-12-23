package net.elodina.mesos.dse

import org.junit.{Before, Test}
import org.junit.Assert._
import scala.util.parsing.json.JSONObject

class NodesTest {
  @Before
  def before {
    Nodes.reset()
  }

  @Test
  def getClusters {
    val rd = Nodes.defaultCluster
    val r0 = Nodes.addCluster(new Cluster("0"))
    val r1 = Nodes.addCluster(new Cluster("1"))
    assertEquals(List(rd, r0, r1), Nodes.getClusters)
  }

  @Test
  def getCluster {
    assertNull(Nodes.getCluster("0"))
    val r0 = Nodes.addCluster(new Cluster("0"))
    assertSame(r0, Nodes.getCluster("0"))
  }

  @Test
  def addCluster {
    val rd = Nodes.defaultCluster
    val r0 = Nodes.addCluster(new Cluster("0"))
    assertEquals(List(rd, r0), Nodes.getClusters)

    try { Nodes.addCluster(new Cluster("0")); fail() }
    catch { case e: IllegalArgumentException => assertTrue(e.getMessage, e.getMessage.contains("duplicate")) }
  }

  @Test
  def removeCluster {
    val rd = Nodes.defaultCluster
    val r0 = Nodes.addCluster(new Cluster("0"))
    val r1 = Nodes.addCluster(new Cluster("1"))
    assertEquals(List(rd, r0, r1), Nodes.getClusters)

    Nodes.removeCluster(r0)
    assertEquals(List(rd, r1), Nodes.getClusters)

    try { Nodes.removeCluster(Nodes.defaultCluster); fail() }
    catch { case e: IllegalArgumentException => assertTrue(e.getMessage, e.getMessage.contains("can't remove default")) }
  }

  @Test
  def getNodes {
    val n0 = Nodes.addNode(new Node("0"))
    val n1 = Nodes.addNode(new Node("1"))
    val n2 = Nodes.addNode(new Node("2"))
    assertEquals(List(n0, n1, n2), Nodes.getNodes)
  }

  @Test
  def getNode {
    assertNull(Nodes.getNode("0"))
    val n0 = Nodes.addNode(new Node("0"))
    assertSame(n0, Nodes.getNode("0"))
  }

  @Test
  def addNode {
    val n0 = Nodes.addNode(new Node("0"))
    assertEquals(List(n0), Nodes.getNodes)

    try { Nodes.addNode(new Node("0")); fail() }
    catch { case e: IllegalArgumentException => assertTrue(e.getMessage, e.getMessage.contains("duplicate")) }
  }

  @Test
  def removeNode {
    val n0 = Nodes.addNode(new Node("0"))
    val n1 = Nodes.addNode(new Node("1"))
    val n2 = Nodes.addNode(new Node("2"))
    assertEquals(List(n0, n1, n2), Nodes.getNodes)

    Nodes.removeNode(n1)
    assertEquals(List(n0, n2), Nodes.getNodes)
  }

  @Test
  def reset {
    assertNull(Nodes.frameworkId)
    assertTrue(Nodes.getNodes.isEmpty)
    assertEquals(List(Nodes.defaultCluster), Nodes.getClusters)

    Nodes.frameworkId = "id"
    Nodes.addNode(new Node("0"))
    Nodes.addCluster(new Cluster("1"))

    Nodes.reset()
    assertNull(Nodes.frameworkId)
    assertTrue(Nodes.getNodes.isEmpty)
    assertEquals(List(Nodes.defaultCluster), Nodes.getClusters)
  }

  @Test
  def toJson_fromJson {
    Nodes.addNode(new Node("1"))
    Nodes.addNode(new Node("2"))

    Nodes.addCluster(new Cluster("1"))
    Nodes.addCluster(new Cluster("2"))

    val json: JSONObject = Nodes.toJson
    Nodes.reset()
    Nodes.fromJson(Util.parseJsonAsMap("" + json))

    assertEquals(2, Nodes.getNodes.size)
    assertEquals(3, Nodes.getClusters.size)
  }
}
