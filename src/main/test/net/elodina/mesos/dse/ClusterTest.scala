package net.elodina.mesos.dse

import org.junit.{Before, Test}
import org.junit.Assert._

class ClusterTest {
  var cluster: Cluster = null

  @Before
  def before {
    cluster = new Cluster()
  }

  @Test
  def getNodes {
    val n0 = cluster.addNode(new Node("0"))
    val n1 = cluster.addNode(new Node("1"))
    val n2 = cluster.addNode(new Node("2"))
    assertEquals(List(n0, n1, n2), cluster.getNodes)
  }

  @Test
  def getNode {
    assertNull(cluster.getNode("0"))
    val n0 = cluster.addNode(new Node("0"))
    assertSame(n0, cluster.getNode("0"))
  }

  @Test
  def addNode {
    val n0 = cluster.addNode(new Node("0"))
    assertEquals(List(n0), cluster.getNodes)

    try { cluster.addNode(new Node("0")); fail() }
    catch { case e: IllegalArgumentException => assertTrue(e.getMessage, e.getMessage.contains("duplicate")) }
  }

  @Test
  def removeNode {
    val n0 = cluster.addNode(new Node("0"))
    val n1 = cluster.addNode(new Node("1"))
    val n2 = cluster.addNode(new Node("2"))
    assertEquals(List(n0, n1, n2), cluster.getNodes)

    cluster.removeNode(n1)
    assertEquals(List(n0, n2), cluster.getNodes)
  }

  @Test
  def getRings {
    val rd = cluster.getDefaultRing
    val r0 = cluster.addRing(new Ring("0"))
    val r1 = cluster.addRing(new Ring("1"))
    assertEquals(List(rd, r0, r1), cluster.getRings)
  }

  @Test
  def getRing {
    assertNull(cluster.getRing("0"))
    val r0 = cluster.addRing(new Ring("0"))
    assertSame(r0, cluster.getRing("0"))
  }

  @Test
  def addRing {
    val rd = cluster.getDefaultRing
    val r0 = cluster.addRing(new Ring("0"))
    assertEquals(List(rd, r0), cluster.getRings)

    try { cluster.addRing(new Ring("0")); fail() }
    catch { case e: IllegalArgumentException => assertTrue(e.getMessage, e.getMessage.contains("duplicate")) }
  }

  @Test
  def removeRing {
    val rd = cluster.getDefaultRing
    val r0 = cluster.addRing(new Ring("0"))
    val r1 = cluster.addRing(new Ring("1"))
    assertEquals(List(rd, r0, r1), cluster.getRings)

    cluster.removeRing(r0)
    assertEquals(List(rd, r1), cluster.getRings)

    try { cluster.removeRing(cluster.getDefaultRing); fail() }
    catch { case e: IllegalArgumentException => assertTrue(e.getMessage, e.getMessage.contains("can't remove default")) }
  }

  @Test
  def clear {
    assertNull(cluster.frameworkId)
    assertTrue(cluster.getNodes.isEmpty)
    assertEquals(List(cluster.getDefaultRing), cluster.getRings)

    cluster.frameworkId = "id"
    cluster.addNode(new Node("0"))
    cluster.addRing(new Ring("1"))

    cluster.clear()
    assertNull(cluster.frameworkId)
    assertTrue(cluster.getNodes.isEmpty)
    assertEquals(List(cluster.getDefaultRing), cluster.getRings)
  }

  @Test
  def toJson_fromJson {
    val cluster: Cluster = new Cluster()
    cluster.addNode(new Node("1"))
    cluster.addNode(new Node("2"))

    cluster.addRing(new Ring("1"))
    cluster.addRing(new Ring("2"))

    val read = new Cluster(Util.parseJsonAsMap("" + cluster.toJson))
    assertClusterEquals(cluster, read)
  }

  def assertClusterEquals(expected: Cluster, actual: Cluster) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.getNodes, actual.getNodes)
    assertEquals(expected.getRings, actual.getRings)
  }

  private def checkNulls(expected: Object, actual: Object): Boolean = {
    if (expected == actual) return true
    if (expected == null) throw new AssertionError("actual != null")
    if (actual == null) throw new AssertionError("actual == null")
    false
  }
}
