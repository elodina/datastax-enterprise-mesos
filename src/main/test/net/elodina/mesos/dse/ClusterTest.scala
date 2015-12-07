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
  def getNodes_addNode_removeNode {
    assertEquals(List(), cluster.getNodes)

    val n0 = cluster.addNode(new Node("0"))
    val n1 = cluster.addNode(new Node("1"))
    val n2 = cluster.addNode(new Node("2"))
    assertEquals(List(n0, n1, n2), cluster.getNodes)

    cluster.removeNode(n1)
    assertEquals(List(n0, n2), cluster.getNodes)
  }

  @Test
  def getNode {
    assertNull(cluster.getNode("0"))

    val n0 = cluster.addNode(new Node("0"))
    assertSame(n0, cluster.getNode("0"))
  }

  @Test
  def getRings_addRing_removeRing {
    assertEquals(List(), cluster.getRings)

    val r0 = cluster.addRing(new Ring("0"))
    val r1 = cluster.addRing(new Ring("1"))
    val r2 = cluster.addRing(new Ring("2"))
    assertEquals(List(r0, r1, r2), cluster.getRings)

    cluster.removeRing(r1)
    assertEquals(List(r0, r2), cluster.getRings)
  }

  @Test
  def getRing {
    assertNull(cluster.getRing("0"))

    val r0 = cluster.addRing(new Ring("0"))
    assertSame(r0, cluster.getRing("0"))
  }

  @Test
  def clear {
    assertNull(cluster.frameworkId)
    assertTrue(cluster.getNodes.isEmpty)
    assertTrue(cluster.getRings.isEmpty)

    cluster.frameworkId = "id"
    cluster.addNode(new Node("0"))
    cluster.addRing(new Ring("1"))

    cluster.clear()
    assertNull(cluster.frameworkId)
    assertTrue(cluster.getNodes.isEmpty)
    assertTrue(cluster.getRings.isEmpty)
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
