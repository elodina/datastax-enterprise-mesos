package net.elodina.mesos.dse

import org.junit.{Before, Test}
import org.junit.Assert._
import scala.util.parsing.json.JSONObject

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
    ring.name = "name"

    val json: JSONObject = ring.toJson
    val read = new Ring(json.obj)

    assertEquals(ring.id, read.id)
    assertEquals(ring.name, read.name)
  }
}
