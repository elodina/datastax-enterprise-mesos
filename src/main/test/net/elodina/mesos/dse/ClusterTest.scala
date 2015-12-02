package net.elodina.mesos.dse

import org.junit.Test
import org.junit.Assert._

class ClusterTest {
  @Test
  def toJson_fromJson {
    val cluster: Cluster = new Cluster()
    cluster.addNode(new Node("1"))
    cluster.addNode(new Node("2"))

    cluster.addRing(new Ring("1"))
    cluster.addRing(new Ring("2"))

    val read = new Cluster(Util.parseJson("" + cluster.toJson))
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
