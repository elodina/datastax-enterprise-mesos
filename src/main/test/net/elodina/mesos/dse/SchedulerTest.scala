package net.elodina.mesos.dse

import org.junit.Test
import org.junit.Assert._
import java.util.Date
import Scheduler.Reconciler

class SchedulerTest extends MesosTestCase {
  @Test
  def acceptOffer {
    val node = Cluster.addNode(new Node("0"))
    node.cpu = 0.5
    node.mem = 400

    node.state = Node.State.RECONCILING
    assertEquals("reconciling", Scheduler.acceptOffer(offer()))

    node.state = Node.State.IDLE
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

  @Test
  def Reconciler_reconcileTasksIfRequired {
    Reconciler.reconcileTime = null
    val node0 = Cluster.addNode(new Node("0"))

    val node1 = Cluster.addNode(new Node("1"))
    node1.state = Node.State.RUNNING
    node1.runtime = new Node.Runtime(taskId = "1")

    val node2 = Cluster.addNode(new Node("2"))
    node2.state = Node.State.STARTING
    node2.runtime = new Node.Runtime(taskId = "2")

    Reconciler.reconcileTasksIfRequired(force = true, now = new Date(0))
    assertEquals(1, Reconciler.reconciles)
    assertEquals(new Date(0), Reconciler.reconcileTime)

    assertNull(node0.runtime)
    assertEquals(Node.State.RECONCILING, node1.state)
    assertEquals(Node.State.RECONCILING, node2.state)

    for (i <- 2 until Reconciler.RECONCILE_MAX_TRIES + 1) {
      Reconciler.reconcileTasksIfRequired(now = new Date(Reconciler.RECONCILE_DELAY.toMillis * i))
      assertEquals(i, Reconciler.reconciles)
      assertEquals(Node.State.RECONCILING, node1.state)
    }
    assertEquals(0, schedulerDriver.killedTasks.size())

    // last reconcile should stop broker
    Reconciler.reconcileTasksIfRequired(now = new Date(Reconciler.RECONCILE_DELAY.toMillis * (Reconciler.RECONCILE_MAX_TRIES + 1)))
    assertNull(node1.runtime)
    assertEquals(2, schedulerDriver.killedTasks.size())
  }
}
