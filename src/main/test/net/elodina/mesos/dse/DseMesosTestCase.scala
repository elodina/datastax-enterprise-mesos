package net.elodina.mesos.dse

import net.elodina.mesos.test.MesosTestCase
import org.junit.{After, Before}
import org.apache.log4j.BasicConfigurator
import java.io.File
import java.nio.file.Files
import net.elodina.mesos.util.Net
import org.junit.Assert._
import net.elodina.mesos.dse.Node.{Runtime, Failover, Stickiness, Reservation}
import scala.concurrent.duration.Duration

class DseMesosTestCase extends MesosTestCase {
  @Before
  def before {
    BasicConfigurator.configure()
    Scheduler.initLogging()

    val storageFile: File = Files.createTempFile(classOf[DseMesosTestCase].getSimpleName, null).toFile
    storageFile.delete()
    Nodes.storage = new FileStorage(storageFile)
    Nodes.reset()

    Config.api = "http://localhost:" + Net.findAvailPort
    Config.dse = new File("dse.tar.gz")
    Config.cassandra = new File("cassandra.tar.gz")
    Config.jar = new File("dse-mesos.jar")

    Scheduler.registered(schedulerDriver, frameworkId(), master())
  }

  @After
  def after {
    Scheduler.disconnected(schedulerDriver)
    BasicConfigurator.resetConfiguration()

    Config.api = null
    Config.dse = null
    Config.cassandra = null
    Config.jar = null

    Nodes.storage.asInstanceOf[FileStorage].file.delete()
    Nodes.storage = Nodes.newStorage(Config.storage)
  }

  def assertNodeEquals(expected: Node, actual: Node) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.id, actual.id)
    assertEquals(expected.state, actual.state)
    assertEquals(expected.cluster, actual.cluster)
    assertStickinessEquals(expected.stickiness, actual.stickiness)
    assertFailoverEquals(expected.failover, actual.failover)
    assertRuntimeEquals(expected.runtime, actual.runtime)

    assertEquals(expected.cpu, actual.cpu, 0.001)
    assertEquals(expected.mem, actual.mem)
    assertEquals(expected.seed, actual.seed)

    assertEquals(expected.jvmOptions, actual.jvmOptions)

    assertEquals(expected.rack, actual.rack)
    assertEquals(expected.dc, actual.dc)

    assertEquals(expected.constraints, actual.constraints)
    assertEquals(expected.seedConstraints, actual.seedConstraints)

    assertEquals(expected.dataFileDirs, actual.dataFileDirs)
    assertEquals(expected.commitLogDir, actual.commitLogDir)
    assertEquals(expected.savedCachesDir, actual.savedCachesDir)
  }

  def assertReservationEquals(expected: Reservation, actual: Reservation) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.cpus, actual.cpus, 0.001)
    assertEquals(expected.mem, actual.mem)
    assertEquals(expected.ports, actual.ports)
    assertEquals(expected.ignoredPorts, actual.ignoredPorts)
  }

  def assertStickinessEquals(expected: Stickiness, actual: Stickiness) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.period, actual.period)
    assertEquals(expected.hostname, actual.hostname)
    assertEquals(expected.stopTime, actual.stopTime)
  }

  def assertFailoverEquals(expected: Failover, actual: Failover) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.delay, actual.delay)
    assertEquals(expected.maxDelay, actual.maxDelay)
    assertEquals(expected.maxTries, actual.maxTries)

    assertEquals(expected.failures, actual.failures)
    assertEquals(expected.failureTime, actual.failureTime)
  }

  def assertRuntimeEquals(expected: Runtime, actual: Runtime) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.taskId, actual.taskId)
    assertEquals(expected.executorId, actual.executorId)

    assertEquals(expected.slaveId, actual.slaveId)
    assertEquals(expected.hostname, actual.hostname)
    assertEquals(expected.address, actual.address)

    assertEquals(expected.seeds, actual.seeds)
    assertReservationEquals(expected.reservation, actual.reservation)
    assertEquals(expected.attributes, actual.attributes)
  }

  private def checkNulls(expected: Object, actual: Object): Boolean = {
    if (expected eq actual) return true
    if (expected == null) throw new AssertionError("actual != null")
    if (actual == null) throw new AssertionError("actual == null")
    false
  }

  def assertDifference[T: Numeric](expr: => T, difference: T = 1, message: String = "has to change")(action: => Unit): Unit = {
    import scala.math.Numeric.Implicits._
    val before = expr
    action
    val after = expr
    assertEquals(message, difference, after - before)
  }

  def assertNoDifference[T: Numeric](expr: => T, message: String = "don't change")(action: => Unit): Unit = {
    import scala.math.Numeric.Implicits._
    val before = expr
    action
    val after = expr
    assertEquals(message, 0, after - before)
  }

  def delay(duration: String = "100ms")(f: => Unit) = new Thread {
    override def run(): Unit = {
      Thread.sleep(Duration(duration).toMillis)
      f
    }
  }.start()
}
