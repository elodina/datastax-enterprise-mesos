package net.elodina.mesos.dse

import org.junit.Test
import org.junit.Assert._
import java.util.Date
import Scheduler.Reconciler
import org.apache.mesos.Protos.TaskState

class SchedulerTest extends MesosTestCase {
  @Test
  def acceptOffer {
    val node = Nodes.addNode(new Node("0"))
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
      val node = Nodes.addNode(new Node("" + i))
      node.state = Node.State.STARTING
    }
    Nodes.getNode("5").seed = true

    assertEquals(null, Scheduler.acceptOffer(offer(resources = s"cpus:1;mem:1000;ports:0..10")))
    assertEquals(1, schedulerDriver.launchedTasks.size())
    assertNotNull(Nodes.getNode("5").runtime)
  }

  @Test
  def onTaskStatus {
    val node = Nodes.addNode(new Node("0"))

    // node started
    node.runtime = new Node.Runtime(node, offer())
    node.state = Node.State.STARTING
    Scheduler.onTaskStatus(taskStatus(id = node.runtime.taskId, state = TaskState.TASK_RUNNING))
    assertEquals(Node.State.RUNNING, node.state)
    assertEquals(0, schedulerDriver.killedTasks.size())

    // node failed
    Scheduler.onTaskStatus(taskStatus(id = node.runtime.taskId, state = TaskState.TASK_LOST))
    assertEquals(Node.State.STARTING, node.state)
    assertNull(node.runtime)

    // node stopped
    node.runtime = new Node.Runtime(node, offer())
    node.state = Node.State.STOPPING
    Scheduler.onTaskStatus(taskStatus(id = node.runtime.taskId, state = TaskState.TASK_FINISHED))
    assertEquals(Node.State.IDLE, node.state)
    assertNull(node.runtime)
  }

  @Test
  def onTaskStarted {
    // unknown node
    Scheduler.onTaskStarted(null, taskStatus(state = TaskState.TASK_RUNNING))
    assertEquals(1, schedulerDriver.killedTasks.size())
    schedulerDriver.killedTasks.clear()

    val node = Nodes.addNode(new Node("0"))
    node.runtime = new Node.Runtime()

    // idle, stopping
    for (state <- List(Node.State.IDLE, Node.State.STOPPING)) {
      node.runtime = new Node.Runtime(taskId = "task")
      node.state = state

      Scheduler.onTaskStarted(node, taskStatus(id = "task", state = TaskState.TASK_RUNNING, data = "address"))
      assertEquals("" + state, 1, schedulerDriver.killedTasks.size())
      assertEquals("" + state, state, node.state)
      assertEquals("" + state, null, node.runtime.address)

      schedulerDriver.killedTasks.clear()
    }

    // starting, running, reconciling
    for (state <- List(Node.State.STARTING, Node.State.RUNNING, Node.State.RECONCILING)) {
      node.runtime = new Node.Runtime(taskId = "task")
      node.state = state

      Scheduler.onTaskStarted(node, taskStatus(id = "task", state = TaskState.TASK_RUNNING, data = "address"))
      assertEquals("" + state, 0, schedulerDriver.killedTasks.size())
      assertEquals("" + state, Node.State.RUNNING, node.state)

      if (state != Node.State.RECONCILING)
        assertEquals("" + state, "address", node.runtime.address)
    }
  }

  @Test
  def onTaskStopped {
    // unknown node
    Scheduler.onTaskStopped(null, taskStatus(state = TaskState.TASK_FAILED))

    val node = Nodes.addNode(new Node("0"))
    node.runtime = new Node.Runtime(taskId = "task")

    // idle
    Scheduler.onTaskStopped(node, taskStatus(id = "task", state = TaskState.TASK_FAILED))
    assertEquals(Node.State.IDLE, node.state)

    // stopping
    node.state = Node.State.STOPPING
    Scheduler.onTaskStopped(node, taskStatus(id = "task", state = TaskState.TASK_FAILED))
    assertEquals(Node.State.IDLE, node.state)
    assertNull(node.runtime)

    // starting, running, reconciling
    for (state <- List(Node.State.STARTING, Node.State.RUNNING, Node.State.RECONCILING)) {
      node.runtime = new Node.Runtime(taskId = "task")
      node.state = state

      Scheduler.onTaskStopped(node, taskStatus(id = "task", state = TaskState.TASK_FAILED))
      assertEquals("" + state, Node.State.STARTING, node.state)
      assertNull("" + state, node.runtime)
    }
  }

  @Test
  def Reconciler_reconcileTasksIfRequired {
    Reconciler.reconcileTime = null
    val node0 = Nodes.addNode(new Node("0"))

    val node1 = Nodes.addNode(new Node("1"))
    node1.state = Node.State.RUNNING
    node1.runtime = new Node.Runtime(taskId = "1")

    val node2 = Nodes.addNode(new Node("2"))
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
