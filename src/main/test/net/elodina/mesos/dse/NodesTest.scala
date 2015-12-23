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
  def getRings {
    val rd = Nodes.defaultRing
    val r0 = Nodes.addRing(new Ring("0"))
    val r1 = Nodes.addRing(new Ring("1"))
    assertEquals(List(rd, r0, r1), Nodes.getRings)
  }

  @Test
  def getRing {
    assertNull(Nodes.getRing("0"))
    val r0 = Nodes.addRing(new Ring("0"))
    assertSame(r0, Nodes.getRing("0"))
  }

  @Test
  def addRing {
    val rd = Nodes.defaultRing
    val r0 = Nodes.addRing(new Ring("0"))
    assertEquals(List(rd, r0), Nodes.getRings)

    try { Nodes.addRing(new Ring("0")); fail() }
    catch { case e: IllegalArgumentException => assertTrue(e.getMessage, e.getMessage.contains("duplicate")) }
  }

  @Test
  def removeRing {
    val rd = Nodes.defaultRing
    val r0 = Nodes.addRing(new Ring("0"))
    val r1 = Nodes.addRing(new Ring("1"))
    assertEquals(List(rd, r0, r1), Nodes.getRings)

    Nodes.removeRing(r0)
    assertEquals(List(rd, r1), Nodes.getRings)

    try { Nodes.removeRing(Nodes.defaultRing); fail() }
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
    assertEquals(List(Nodes.defaultRing), Nodes.getRings)

    Nodes.frameworkId = "id"
    Nodes.addNode(new Node("0"))
    Nodes.addRing(new Ring("1"))

    Nodes.reset()
    assertNull(Nodes.frameworkId)
    assertTrue(Nodes.getNodes.isEmpty)
    assertEquals(List(Nodes.defaultRing), Nodes.getRings)
  }

  @Test
  def toJson_fromJson {
    Nodes.addNode(new Node("1"))
    Nodes.addNode(new Node("2"))

    Nodes.addRing(new Ring("1"))
    Nodes.addRing(new Ring("2"))

    val json: JSONObject = Nodes.toJson
    Nodes.reset()
    Nodes.fromJson(Util.parseJsonAsMap("" + json))

    assertEquals(2, Nodes.getNodes.size)
    assertEquals(3, Nodes.getRings.size)
  }
}
