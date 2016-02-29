/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.elodina.mesos.dse

import net.elodina.mesos.dse.Node.{Failover, Runtime, Stickiness, Reservation}
import org.apache.mesos.Protos.Resource.DiskInfo.Persistence
import org.apache.mesos.Protos.Resource.{DiskInfo, ReservationInfo}
import org.apache.mesos.Protos.Volume.Mode
import org.apache.mesos.Protos._
import java.util.{Collections, UUID}
import org.apache.mesos.Protos.Value.Text
import org.junit.Assert._
import scala.collection.JavaConversions._
import org.apache.mesos.{ExecutorDriver, SchedulerDriver}
import java.util
import org.junit.{Ignore, After, Before}
import org.apache.log4j.BasicConfigurator
import com.google.protobuf.ByteString
import java.io.{PrintStream, ByteArrayOutputStream, File}
import java.nio.file.Files
import scala.concurrent.duration.Duration

@Ignore
class MesosTestCase {
  var schedulerDriver: TestSchedulerDriver = null
  var executorDriver: TestExecutorDriver = null

  @Before
  def before {
    BasicConfigurator.configure()
    Scheduler.initLogging()

    val storageFile: File = Files.createTempFile(classOf[MesosTestCase].getSimpleName, null).toFile
    storageFile.delete()
    Nodes.storage = new FileStorage(storageFile)
    Nodes.reset()

    schedulerDriver = new TestSchedulerDriver()
    Scheduler.registered(schedulerDriver, frameworkId(), master())

    executorDriver = new TestExecutorDriver()

    Config.api = "http://localhost:" + Util.findAvailPort
    Config.dse = new File("dse.tar.gz")
    Config.cassandra = new File("cassandra.tar.gz")
    Config.jar = new File("dse-mesos.jar")
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

  val LOCALHOST_IP: Int = 2130706433
  
  def frameworkId(id: String = "" + UUID.randomUUID()): FrameworkID = FrameworkID.newBuilder().setValue(id).build()
  def taskId(id: String = "" + UUID.randomUUID()): TaskID = TaskID.newBuilder().setValue(id).build()

  def master(
    id: String = "" + UUID.randomUUID(),
    ip: Int = LOCALHOST_IP,
    port: Int = 5050,
    hostname: String = "master"
  ): MasterInfo = {
    MasterInfo.newBuilder()
    .setId(id)
    .setIp(ip)
    .setPort(port)
    .setHostname(hostname)
    .setVersion("0.23.0")
    .build()
  }

  def offer(
    id: String = "" + UUID.randomUUID(),
    frameworkId: String = "" + UUID.randomUUID(),
    slaveId: String = "" + UUID.randomUUID(),
    hostname: String = "host",
    resources: String = null,
    attributes: String = null
  ): Offer = {
    val builder = Offer.newBuilder()
      .setId(OfferID.newBuilder().setValue(id))
      .setFrameworkId(FrameworkID.newBuilder().setValue(frameworkId))
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId))

    builder.setHostname(hostname)
    builder.addAllResources(this.resources(resources))

    if (attributes != null) {
      val map = Util.parseMap(attributes)
      for ((k, v) <- map) {
        val attribute = Attribute.newBuilder()
          .setType(Value.Type.TEXT)
          .setName(k)
          .setText(Text.newBuilder().setValue(v))
          .build
        builder.addAttributes(attribute)
      }
    }

    builder.build()
  }

  // parses range definition: 1000..1100,1102,2000..3000
  def ranges(s: String): util.List[Value.Range] = {
    if (s.isEmpty) return Collections.emptyList()
    s.split(",").toList
      .map(s => new Util.Range(s.trim))
      .map(r => Value.Range.newBuilder().setBegin(r.start).setEnd(r.end).build())
  }

  // parses resources definition like: cpus:0.5; cpus(kafka):0.3; mem:128; ports(kafka):1000..2000
  // Must parse the following
  // disk:73390
  // disk(*):73390
  // disk(kafka):73390
  // cpu(kafka, principal):0.01
  // disk(kafka, principal)[test_volume:fake_path]:100)
  def resources(s: String): util.List[Resource] = {
    val resources = new util.ArrayList[Resource]()
    if (s == null) return resources

    for (r <- s.split(";").map(_.trim).filter(!_.isEmpty)) {
      val colonIdx = r.lastIndexOf(":")
      if (colonIdx == -1) throw new IllegalArgumentException("invalid resource: " + r)
      var key = r.substring(0, colonIdx)

      var role = "*"
      var principal: String = null
      var volumeId: String = null
      var volumePath: String = null

      // role & principal
      val roleStart = key.indexOf("(")
      if (roleStart != -1) {
        val roleEnd = key.indexOf(")")
        if (roleEnd == -1) throw new IllegalArgumentException(s)

        role = key.substring(roleStart + 1, roleEnd)
        
        val principalIdx = role.indexOf(",")
        if (principalIdx != -1) {
          principal = role.substring(principalIdx + 1)
          role = role.substring(0, principalIdx)
        }

        key = key.substring(0, roleStart) + key.substring(roleEnd + 1)
      }

      // volume
      val volumeStart = key.indexOf("[")
      if (volumeStart != -1) {
        val volumeEnd = key.indexOf("]")
        if (volumeEnd == -1) throw new IllegalArgumentException(s)

        val volume = key.substring(volumeStart + 1, volumeEnd)
        val colonIdx = volume.indexOf(":")

        volumeId = volume.substring(0, colonIdx)
        volumePath = volume.substring(colonIdx + 1)

        key = key.substring(0, volumeStart) + key.substring(volumeEnd + 1)
      }

      // name & value
      val name = key
      val value = r.substring(colonIdx + 1)

      val builder = Resource.newBuilder()
        .setName(name)
        .setRole(role)
      
      if (principal != null)
        builder.setReservation(ReservationInfo.newBuilder.setPrincipal(principal))

      if (volumeId != null)
        builder.setDisk(DiskInfo.newBuilder
          .setPersistence(Persistence.newBuilder.setId(volumeId))
          .setVolume(Volume.newBuilder.setContainerPath(volumePath).setMode(Mode.RW))
        )

      if (key == "cpus" || key == "mem" || key == "disk")
        builder.setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder.setValue(java.lang.Double.parseDouble(value)))
      else if (key == "ports")
        builder.setType(Value.Type.RANGES).setRanges(Value.Ranges.newBuilder.addAllRange(ranges(value)))
      else throw new IllegalArgumentException("Unsupported resource type: " + key)

      resources.add(builder.build())
    }

    resources
  }

  def task(
    id: String = "" + UUID.randomUUID(),
    name: String = "Task",
    slaveId: String = "" + UUID.randomUUID(),
    data: String = Util.formatMap(Collections.singletonMap("node", new Node().toJson(expanded = true)))
  ): TaskInfo = {
    val builder = TaskInfo.newBuilder()
    .setName(id)
    .setTaskId(TaskID.newBuilder().setValue(id))
    .setSlaveId(SlaveID.newBuilder().setValue(slaveId))

    if (data != null) builder.setData(ByteString.copyFromUtf8(data))

    builder.build()
  }

  def taskStatus(
    id: String = "" + UUID.randomUUID(),
    state: TaskState,
    data: String = null
  ): TaskStatus = {
    val builder = TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue(id))
      .setState(state)

    if (data != null)
      builder.setData(ByteString.copyFromUtf8(data))

    builder.build
  }

  class TestSchedulerDriver extends SchedulerDriver {
    var status: Status = Status.DRIVER_RUNNING

    val declinedOffers: util.List[String] = new util.ArrayList[String]()
    val acceptedOffers: util.List[String] = new util.ArrayList[String]()
    
    val launchedTasks: util.List[TaskInfo] = new util.ArrayList[TaskInfo]()
    val killedTasks: util.List[String] = new util.ArrayList[String]()
    val reconciledTasks: util.List[String] = new util.ArrayList[String]()

    def declineOffer(id: OfferID): Status = {
      declinedOffers.add(id.getValue)
      status
    }

    def declineOffer(id: OfferID, filters: Filters): Status = {
      declinedOffers.add(id.getValue)
      status
    }

    def launchTasks(offerId: OfferID, tasks: util.Collection[TaskInfo]): Status = {
      acceptedOffers.add(offerId.getValue)
      launchedTasks.addAll(tasks)
      status
    }

    def launchTasks(offerId: OfferID, tasks: util.Collection[TaskInfo], filters: Filters): Status = {
      acceptedOffers.add(offerId.getValue)
      launchedTasks.addAll(tasks)
      status
    }

    def launchTasks(offerIds: util.Collection[OfferID], tasks: util.Collection[TaskInfo]): Status = {
      for (offerId <- offerIds) acceptedOffers.add(offerId.getValue)
      launchedTasks.addAll(tasks)
      status
    }

    def launchTasks(offerIds: util.Collection[OfferID], tasks: util.Collection[TaskInfo], filters: Filters): Status = {
      for (offerId <- offerIds) acceptedOffers.add(offerId.getValue)
      launchedTasks.addAll(tasks)
      status
    }

    def stop(): Status = throw new UnsupportedOperationException

    def stop(failover: Boolean): Status = throw new UnsupportedOperationException

    def killTask(id: TaskID): Status = {
      killedTasks.add(id.getValue)
      status
    }

    def requestResources(requests: util.Collection[Request]): Status = throw new UnsupportedOperationException

    def sendFrameworkMessage(executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]): Status = throw new UnsupportedOperationException

    def join(): Status = throw new UnsupportedOperationException

    def reconcileTasks(statuses: util.Collection[TaskStatus]): Status = {
      reconciledTasks.addAll(statuses.map(_.getTaskId.getValue))
      status
    }

    def reviveOffers(): Status = throw new UnsupportedOperationException

    def run(): Status = throw new UnsupportedOperationException

    def abort(): Status = throw new UnsupportedOperationException

    def start(): Status = throw new UnsupportedOperationException

    def acceptOffers(offerIds: util.Collection[OfferID], operations: util.Collection[Offer.Operation], filters: Filters): Status = throw new UnsupportedOperationException

    def acknowledgeStatusUpdate(status: TaskStatus): Status = throw new UnsupportedOperationException

    def suppressOffers(): Status = throw new UnsupportedOperationException
  }

  class TestExecutorDriver extends ExecutorDriver {
    var status: Status = Status.DRIVER_RUNNING
    
    private val _statusUpdates: util.List[TaskStatus] = new util.concurrent.CopyOnWriteArrayList[TaskStatus]()
    def statusUpdates: util.List[TaskStatus] = util.Collections.unmodifiableList(_statusUpdates)

    def start(): Status = {
      status = Status.DRIVER_RUNNING
      status
    }

    def stop(): Status = {
      status = Status.DRIVER_STOPPED
      status
    }

    def abort(): Status = {
      status = Status.DRIVER_ABORTED
      status
    }

    def join(): Status = { status }

    def run(): Status = {
      status = Status.DRIVER_RUNNING
      status
    }

    def sendStatusUpdate(status: TaskStatus): Status = {
      _statusUpdates.synchronized {
        _statusUpdates.add(status)
        _statusUpdates.notify()
      }
      
      this.status
    }
    
    def waitForStatusUpdates(count: Int): Unit = {
      _statusUpdates.synchronized {
        while (_statusUpdates.size() < count)
          _statusUpdates.wait()
      }
    }

    def sendFrameworkMessage(message: Array[Byte]): Status = throw new UnsupportedOperationException
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

trait CliTestCase{
  def cli: (Array[String]) => Unit

  def assertCliError(args: Array[String], msg: String) = {
    val _ = try{ cli(args); fail(s"args ($args) has to produce error '$msg'}") }
    catch { case e: Cli.Error => assertTrue(e.getMessage.contains(msg)) }
  }

  def assertCliResponse(args: Array[String], msg: String, newLine: Boolean = true) = {
    val baos = new ByteArrayOutputStream()
    Cli.out = new PrintStream(baos, true)
    try {
      cli(args)
      assertEquals(if (newLine) msg + "\n" else msg, baos.toString)
    } finally {
      Cli.out = System.out
    }
  }

  def outputToString(cmd: => Unit) = {
    val baos = new ByteArrayOutputStream()
    val out = new PrintStream(baos, true)
    Cli.out = out
    try {
      cmd
    } finally {
      Cli.out = System.out
    }
    baos.toString
  }
}
