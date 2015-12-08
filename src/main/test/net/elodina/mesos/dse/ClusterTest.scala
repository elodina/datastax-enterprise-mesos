package net.elodina.mesos.dse

import org.junit.{Before, Test}
import org.junit.Assert._
import scala.util.parsing.json.JSONObject

class ClusterTest {
  @Before
  def before {
    Cluster.reset()
  }

  @Test
  def getRings {
    val rd = Cluster.defaultRing
    val r0 = Cluster.addRing(new Ring("0"))
    val r1 = Cluster.addRing(new Ring("1"))
    assertEquals(List(rd, r0, r1), Cluster.getRings)
  }

  @Test
  def getRing {
    assertNull(Cluster.getRing("0"))
    val r0 = Cluster.addRing(new Ring("0"))
    assertSame(r0, Cluster.getRing("0"))
  }

  @Test
  def addRing {
    val rd = Cluster.defaultRing
    val r0 = Cluster.addRing(new Ring("0"))
    assertEquals(List(rd, r0), Cluster.getRings)

    try { Cluster.addRing(new Ring("0")); fail() }
    catch { case e: IllegalArgumentException => assertTrue(e.getMessage, e.getMessage.contains("duplicate")) }
  }

  @Test
  def removeRing {
    val rd = Cluster.defaultRing
    val r0 = Cluster.addRing(new Ring("0"))
    val r1 = Cluster.addRing(new Ring("1"))
    assertEquals(List(rd, r0, r1), Cluster.getRings)

    Cluster.removeRing(r0)
    assertEquals(List(rd, r1), Cluster.getRings)

    try { Cluster.removeRing(Cluster.defaultRing); fail() }
    catch { case e: IllegalArgumentException => assertTrue(e.getMessage, e.getMessage.contains("can't remove default")) }
  }

  @Test
  def getNodes {
    val n0 = Cluster.addNode(new Node("0"))
    val n1 = Cluster.addNode(new Node("1"))
    val n2 = Cluster.addNode(new Node("2"))
    assertEquals(List(n0, n1, n2), Cluster.getNodes)
  }

  @Test
  def getNode {
    assertNull(Cluster.getNode("0"))
    val n0 = Cluster.addNode(new Node("0"))
    assertSame(n0, Cluster.getNode("0"))
  }

  @Test
  def addNode {
    val n0 = Cluster.addNode(new Node("0"))
    assertEquals(List(n0), Cluster.getNodes)

    try { Cluster.addNode(new Node("0")); fail() }
    catch { case e: IllegalArgumentException => assertTrue(e.getMessage, e.getMessage.contains("duplicate")) }
  }

  @Test
  def removeNode {
    val n0 = Cluster.addNode(new Node("0"))
    val n1 = Cluster.addNode(new Node("1"))
    val n2 = Cluster.addNode(new Node("2"))
    assertEquals(List(n0, n1, n2), Cluster.getNodes)

    Cluster.removeNode(n1)
    assertEquals(List(n0, n2), Cluster.getNodes)
  }

  @Test
  def reset {
    assertNull(Cluster.frameworkId)
    assertTrue(Cluster.getNodes.isEmpty)
    assertEquals(List(Cluster.defaultRing), Cluster.getRings)

    Cluster.frameworkId = "id"
    Cluster.addNode(new Node("0"))
    Cluster.addRing(new Ring("1"))

    Cluster.reset()
    assertNull(Cluster.frameworkId)
    assertTrue(Cluster.getNodes.isEmpty)
    assertEquals(List(Cluster.defaultRing), Cluster.getRings)
  }

  @Test
  def toJson_fromJson {
    Cluster.addNode(new Node("1"))
    Cluster.addNode(new Node("2"))

    Cluster.addRing(new Ring("1"))
    Cluster.addRing(new Ring("2"))

    val json: JSONObject = Cluster.toJson
    Cluster.reset()
    Cluster.fromJson(Util.parseJsonAsMap("" + json))

    assertEquals(2, Cluster.getNodes.size)
    assertEquals(3, Cluster.getRings.size)
  }
}
