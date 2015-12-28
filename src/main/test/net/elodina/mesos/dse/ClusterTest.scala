package net.elodina.mesos.dse

import org.junit.{Before, Test}
import org.junit.Assert._
import net.elodina.mesos.dse.Util.{BindAddress, Range}

class ClusterTest extends MesosTestCase {
  @Before
  override def before {
    super.before
    Nodes.reset()
  }

  @Test
  def getNodes {
    val n0 = Nodes.addNode(new Node("0"))
    val n1 = Nodes.addNode(new Node("1"))

    val c0 = Nodes.addCluster(new Cluster("c0"))
    val n2 = Nodes.addNode(new Node("2"))
    val n3 = Nodes.addNode(new Node("3"))

    n2.cluster = c0
    n3.cluster = c0

    assertEquals(List(n0, n1), Nodes.defaultCluster.getNodes)
    assertEquals(List(n2, n3), c0.getNodes)
  }

  @Test
  def availSeeds {
    val cluster: Cluster = Nodes.defaultCluster
    val n0: Node = Nodes.addNode(new Node("0"))
    Nodes.addNode(new Node("1"))
    val n2: Node = Nodes.addNode(new Node("2"))

    assertEquals(List(), cluster.availSeeds)

    n0.seed = true
    n0.runtime = new Node.Runtime(hostname = "n0")
    n0.runtime.address = "n0"
    assertEquals(List("n0"), cluster.availSeeds)

    n2.seed = true
    n2.runtime = new Node.Runtime(hostname = "n2")
    n2.runtime.address = "n2"
    assertEquals(List("n0", "n2"), cluster.availSeeds)
  }

  @Test
  def toJSON_fromJSON {
    val cluster: Cluster = new Cluster("1")
    var read = new Cluster(Util.parseJsonAsMap("" + cluster.toJson))
    assertClusterEquals(cluster, read)

    cluster.bindAddress = new BindAddress("192.168.3.*, if:eth1")
    cluster.ports ++= Map(Node.Port.STORAGE -> new Range("100..110"), Node.Port.JMX -> new Range("200..210"))
    read = new Cluster(Util.parseJsonAsMap("" + cluster.toJson))
    assertClusterEquals(cluster, read)
  }

  private def assertClusterEquals(expected: Cluster, actual: Cluster) {
    if (checkNulls(expected, actual)) return
    assertEquals(expected.id, actual.id)
    assertEquals(expected.bindAddress, actual.bindAddress)
    assertEquals(expected.ports, actual.ports)
  }

  private def checkNulls(expected: Object, actual: Object): Boolean = {
    if (expected == actual) return true
    if (expected == null) throw new AssertionError("actual != null")
    if (actual == null) throw new AssertionError("actual == null")
    false
  }
}
