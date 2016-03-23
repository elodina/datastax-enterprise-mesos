package net.elodina.mesos.dse

import org.junit.Test
import org.junit.Assert._
import java.util.Date
import Scheduler.Reconciler
import org.apache.mesos.Protos.TaskState
import scala.collection.JavaConversions._

class SchedulerTest extends DseMesosTestCase {
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
    assertEquals("node 0: cpus < 0.5", Scheduler.acceptOffer(offer("cpus:0.4")))

    assertEquals(null, Scheduler.acceptOffer(offer("cpus:0.5;mem:400;ports:0..10")))
    assertEquals(1, schedulerDriver.launchedTasks.size())
    assertEquals(Node.State.STARTING, node.state)
    assertNotNull(node.runtime)

    // failover delay
    Scheduler.statusUpdate(schedulerDriver, taskStatus(node.runtime.taskId, TaskState.TASK_FAILED))
    assertEquals(1, node.failover.failures)
    assertEquals("no nodes to start", Scheduler.acceptOffer(offer("cpus:1.0;mem:1024;ports:0..10")))

    assertEquals(null, Scheduler.acceptOffer(offer("cpus:1.0;mem:1024;ports:0..10"), node.failover.delayExpires))
  }

  @Test
  def acceptOffer_seeds_first {
    for (i <- 0 until 10) {
      val node = Nodes.addNode(new Node("" + i))
      node.state = Node.State.STARTING
    }
    Nodes.getNode("5").seed = true

    assertEquals(null, Scheduler.acceptOffer(offer(s"cpus:1;mem:1000;ports:0..10")))
    assertEquals(1, schedulerDriver.launchedTasks.size())
    assertNotNull(Nodes.getNode("5").runtime)
  }

  @Test
  def onTaskStatus {
    val node = Nodes.addNode(new Node("0"))

    // node started
    node.runtime = new Node.Runtime(node, offer())
    node.state = Node.State.STARTING
    Scheduler.onTaskStatus(taskStatus(node.runtime.taskId, TaskState.TASK_RUNNING))
    assertEquals(Node.State.RUNNING, node.state)
    assertEquals(0, schedulerDriver.killedTasks.size())
    assertEquals(0, node.failover.failures)

    // node failed
    Scheduler.onTaskStatus(taskStatus(node.runtime.taskId, TaskState.TASK_LOST))
    assertEquals(Node.State.STARTING, node.state)
    assertNull(node.runtime)
    assertEquals(1, node.failover.failures)

    // node stopped
    node.runtime = new Node.Runtime(node, offer())
    node.state = Node.State.STOPPING
    Scheduler.onTaskStatus(taskStatus(node.runtime.taskId, TaskState.TASK_FINISHED))
    assertEquals(Node.State.IDLE, node.state)
    assertNull(node.runtime)

    // task state lost, failed, error considered as fail
    node.failover.resetFailures()
    for(taskState <- Seq(TaskState.TASK_LOST, TaskState.TASK_FAILED, TaskState.TASK_ERROR)) {
      // try to start
      node.runtime = new Node.Runtime(node, offer())
      node.state = Node.State.STARTING

      assertDifference(node.failover.failures) {
        Scheduler.onTaskStatus(taskStatus(node.runtime.taskId, taskState))
        assertEquals(Node.State.STARTING, node.state)
      }
    }
  }

  @Test
  def onTaskStarted {
    // unknown node
    Scheduler.onTaskStarted(null, taskStatus(TaskState.TASK_RUNNING))
    assertEquals(1, schedulerDriver.killedTasks.size())
    schedulerDriver.killedTasks.clear()

    val node = Nodes.addNode(new Node("0"))
    node.runtime = new Node.Runtime()

    // idle, stopping
    for (state <- List(Node.State.IDLE, Node.State.STOPPING)) {
      node.runtime = new Node.Runtime(taskId = "task")
      node.state = state

      Scheduler.onTaskStarted(node, taskStatus("task", TaskState.TASK_RUNNING, "address"))
      assertEquals("" + state, 1, schedulerDriver.killedTasks.size())
      assertEquals("" + state, state, node.state)
      assertEquals("" + state, null, node.runtime.address)

      schedulerDriver.killedTasks.clear()
    }

    // starting, running, reconciling
    for (state <- List(Node.State.STARTING, Node.State.RUNNING, Node.State.RECONCILING)) {
      node.runtime = new Node.Runtime(taskId = "task")
      node.state = state

      node.failover.resetFailures()
      node.failover.registerFailure(new Date(1))

      Scheduler.onTaskStarted(node, taskStatus("task", TaskState.TASK_RUNNING, "address"))
      assertEquals("" + state, 0, schedulerDriver.killedTasks.size())
      assertEquals("" + state, Node.State.RUNNING, node.state)

      if (state != Node.State.RECONCILING)
        assertEquals("" + state, "address", node.runtime.address)

      assertEquals(0, node.failover.failures)
      assertNull(node.failover.failureTime)
    }
  }

  @Test
  def onTaskStopped {
    // unknown node
    Scheduler.onTaskStopped(null, taskStatus(TaskState.TASK_FAILED))

    val node = Nodes.addNode(new Node("0"))
    node.runtime = new Node.Runtime(taskId = "task")

    // idle
    Scheduler.onTaskStopped(node, taskStatus("task", TaskState.TASK_FAILED))
    assertEquals(Node.State.IDLE, node.state)

    // stopping
    node.state = Node.State.STOPPING
    Scheduler.onTaskStopped(node, taskStatus("task", TaskState.TASK_FAILED))
    assertEquals(Node.State.IDLE, node.state)
    assertNull(node.runtime)

    // starting, running, reconciling
    for (state <- List(Node.State.STARTING, Node.State.RUNNING, Node.State.RECONCILING)) {
      node.runtime = new Node.Runtime(taskId = "task")
      node.state = state

      Scheduler.onTaskStopped(node, taskStatus("task", TaskState.TASK_FAILED))
      assertEquals("" + state, Node.State.STARTING, node.state)
      assertNull("" + state, node.runtime)
    }

  }

  @Test
  def onTaskStopped_failover {
    val node = Nodes.addNode(new Node("0"))
    node.runtime = new Node.Runtime(taskId = "task")

    // register failure when on task update received task status failed, lost, error
    // when node was in state starting, running
    for(nodeState <- Seq(Node.State.STARTING, Node.State.RUNNING, Node.State.RECONCILING)) {
      var ts = 0L
      var failures = 0
      node.failover.resetFailures()
      assertEquals(failures, node.failover.failures)

      for(taskState <- Seq(TaskState.TASK_LOST, TaskState.TASK_FAILED, TaskState.TASK_ERROR)) {
        ts += 1
        // try to start
        node.runtime = new Node.Runtime(node, offer())
        node.state = nodeState
        if (node.state == Node.State.RUNNING) node.runtime.address = "slave0"

        Scheduler.onTaskStopped(node, taskStatus(node.runtime.taskId, taskState), new Date(ts))
        // when ever failure occurs and max tries not exceeded node state changed to starting
        assertEquals(Node.State.STARTING, node.state)

        failures += 1

        assertEquals(failures, node.failover.failures)
        assertEquals(new Date(ts), node.failover.failureTime)
      }
    }

    // when node was in status stopping
    // reset failures when node bas been stopped (task finished or killed)
    for(taskState <- Seq(TaskState.TASK_LOST, TaskState.TASK_FAILED, TaskState.TASK_ERROR, TaskState.TASK_FINISHED, TaskState.TASK_KILLED)) {
      node.runtime = new Node.Runtime(node, offer())
      node.state = Node.State.STOPPING

      Scheduler.onTaskStopped(node, taskStatus(node.runtime.taskId, taskState))
      assert(node.idle)

      assertEquals(0, node.failover.failures)
      assertNull(node.failover.failureTime)
    }

    // stop node when exceeded max tries
    for(nodeState <- Seq(Node.State.STARTING, Node.State.RUNNING, Node.State.RECONCILING)) {
      for(taskState <- Seq(TaskState.TASK_LOST, TaskState.TASK_FAILED, TaskState.TASK_ERROR)) {
        node.failover.resetFailures()
        node.failover.registerFailure(new Date(1))
        node.failover.registerFailure(new Date(2))
        node.failover.maxTries = 3

        node.runtime = new Node.Runtime(node, offer())
        node.state = nodeState

        Scheduler.onTaskStopped(node, taskStatus(node.runtime.taskId, taskState), new Date(5))
        // when ever failure occurs and max tries exceeded node state changed to idle
        assert(node.idle)

        assertTrue(node.failover.isMaxTriesExceeded)
        assertEquals(3, node.failover.failures)
        assertEquals(new Date(5), node.failover.failureTime)
      }
    }
  }

  @Test
  def stopNode: Unit = {
    import net.elodina.mesos.dse.Node.State._
    Nodes.reset()
    // no node, nothing to do
    Scheduler.stopNode("")

    val node0 = Nodes.addNode(new Node("0"))
    // node exists but no runtime, thus have to assign state IDLE
    Scheduler.stopNode("0")
    assertEquals(Node.State.IDLE, node0.state)

    // for instance node is starting but offers has insufficient resource, perhaps missing ports
    node0.state = STARTING
    Scheduler.stopNode(node0.id)
    // kill task sent only when node has runtime
    assertEquals(0, schedulerDriver.killedTasks.size())
    assertEquals(IDLE, node0.state)

    // node has sufficient resources to start
    node0.state = STARTING
    Scheduler.resourceOffers(schedulerDriver, List(offer("cpus:2.0;mem:20480;ports:0..65000")))
    assertNotNull(node0.runtime)
    // confirm start by executor
    Scheduler.onTaskStarted(node0, taskStatus(node0.runtime.taskId, TaskState.TASK_RUNNING))
    assertEquals(Node.State.RUNNING, node0.state)

    // illegal state exception thrown when scheduler becomes disconnected from the master
    Scheduler.disconnected(schedulerDriver)
    try {
      Scheduler.stopNode(node0.id)
      fail()
    } catch {
      case e: IllegalStateException => assertEquals("scheduler disconnected from the master", e.getMessage)
    }
    Scheduler.reregistered(schedulerDriver, master())

    Scheduler.stopNode(node0.id)
    assertEquals(STOPPING, node0.state)
    // trigger sending kill task
    assertEquals(1, schedulerDriver.killedTasks.size())
    // resourceOffers doesn't trigger set runtime to null
    assertNotNull(node0.runtime)
    // ability to sent kill task on each call to stopNode
    assertDifference(schedulerDriver.killedTasks.size()) {
      Scheduler.stopNode(node0.id)
    }

    Scheduler.onTaskStopped(node0, taskStatus(node0.runtime.taskId, TaskState.TASK_FINISHED))
    assertEquals(Node.State.IDLE, node0.state)
    assertNull(node0.runtime)
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
