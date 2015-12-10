package net.elodina.mesos.dse

import org.junit.{Before, Test}
import org.junit.Assert._

class RingTest {
  @Before
  def before {
    Cluster.reset()
  }

  @Test
  def availSeeds {
    val ring: Ring = Cluster.defaultRing
    val n0: Node = Cluster.addNode(new Node("0"))
    Cluster.addNode(new Node("1"))
    val n2: Node = Cluster.addNode(new Node("2"))

    assertEquals(List(), ring.availSeeds)

    n0.seed = true
    n0.runtime = new Node.Runtime(hostname = "n0")
    assertEquals(List("n0"), ring.availSeeds)

    n2.seed = true
    n2.runtime = new Node.Runtime(hostname = "n2")
    assertEquals(List("n0", "n2"), ring.availSeeds)
  }

  @Test
  def toJSON_fromJSON {
    val ring: Ring = new Ring("1")
    var read = new Ring(ring.toJson.obj)
    assertRingEquals(ring, read)

    ring.name = "name"
    ring.storagePort = 0
    ring.jmxPort = 1
    ring.nativePort = 2
    ring.rpcPort = 3

    read = new Ring(ring.toJson.obj)
    assertRingEquals(ring, read)
  }

  private def assertRingEquals(expected: Ring, actual: Ring) {
    if (checkNulls(expected, actual)) return
    assertEquals(expected.id, actual.id)
    assertEquals(expected.name, actual.name)

    assertEquals(expected.storagePort, actual.storagePort)
    assertEquals(expected.jmxPort, actual.jmxPort)
    assertEquals(expected.nativePort, actual.nativePort)
    assertEquals(expected.rpcPort, actual.rpcPort)
  }

  private def checkNulls(expected: Object, actual: Object): Boolean = {
    if (expected == actual) return true
    if (expected == null) throw new AssertionError("actual != null")
    if (actual == null) throw new AssertionError("actual == null")
    false
  }
}
