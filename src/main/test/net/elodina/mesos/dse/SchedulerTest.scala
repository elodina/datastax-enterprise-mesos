package net.elodina.mesos.dse

import org.junit.Test
import org.junit.Assert._

class SchedulerTest extends MesosTestCase {
  @Test
  def acceptOffer {
    val node = Cluster.addNode(new Node("0"))
    node.cpu = 0.5
    node.mem = 400

    assertEquals("no nodes to start", Scheduler.acceptOffer(offer()))

    node.state = Node.State.STARTING
    node.runtime = new Node.Runtime()
    assertEquals("node 0 is starting", Scheduler.acceptOffer(offer()))

    node.runtime = null
    assertEquals("node 0: cpus < 0.5", Scheduler.acceptOffer(offer(resources = "cpus:0.4")))

    assertEquals(null, Scheduler.acceptOffer(offer(resources = "cpus:0.5;mem:400;ports:0..10")))
    assertEquals(1, schedulerDriver.launchedTasks.size())
    assertEquals(Node.State.STARTING, node.state)
    assertNotNull(node.runtime)
  }

  @Test
  def acceptOffer_seeds_first {
    for (i <- 0 until 10) {
      val node = Cluster.addNode(new Node("" + i))
      node.state = Node.State.STARTING
    }
    Cluster.getNode("5").seed = true

    assertEquals(null, Scheduler.acceptOffer(offer(resources = s"cpus:1;mem:1000;ports:0..10")))
    assertEquals(1, schedulerDriver.launchedTasks.size())
    assertNotNull(Cluster.getNode("5").runtime)
  }
}
