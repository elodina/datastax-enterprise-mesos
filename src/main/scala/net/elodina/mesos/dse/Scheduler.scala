/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.elodina.mesos.dse

import java.io.File
import java.util
import java.util.concurrent.TimeUnit

import com.google.protobuf.ByteString
import org.apache.log4j._
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, SchedulerDriver}

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.language.postfixOps

import Util.Str

object Scheduler extends org.apache.mesos.Scheduler with Constraints[Node] with Reconciliation[Node] {
  private[dse] val logger = Logger.getLogger(this.getClass)
  private var driver: SchedulerDriver = null

  override protected val reconcileDelay: Duration = Duration("20 seconds")
  override protected val reconcileMaxTries: Int = 5

  def start() {
    initLogging()
    logger.info(s"Starting scheduler:\n$Config")

    resolveDeps()
    Cluster.load()
    HttpServer.start()

    val frameworkBuilder = FrameworkInfo.newBuilder()
    frameworkBuilder.setUser(if (Config.user != null) Config.user else "")
    if (Cluster.frameworkId != null) frameworkBuilder.setId(FrameworkID.newBuilder().setValue(Cluster.frameworkId))
    frameworkBuilder.setRole(Config.frameworkRole)

    frameworkBuilder.setName(Config.frameworkName)
    frameworkBuilder.setFailoverTimeout(Config.frameworkTimeout.toUnit(TimeUnit.SECONDS))
    frameworkBuilder.setCheckpoint(true)

    var credsBuilder: Credential.Builder = null
    if (Config.principal != null && Config.secret != null) {
      frameworkBuilder.setPrincipal(Config.principal)

      credsBuilder = Credential.newBuilder()
        .setPrincipal(Config.principal)
        .setSecret(ByteString.copyFromUtf8(Config.secret))
    }

    val driver = if (credsBuilder == null) new MesosSchedulerDriver(this, frameworkBuilder.build, Config.master)
    else new MesosSchedulerDriver(this, frameworkBuilder.build, Config.master, credsBuilder.build())

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        HttpServer.stop()
      }
    })

    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1
    sys.exit(status)
  }

  override def registered(driver: SchedulerDriver, id: FrameworkID, master: MasterInfo) {
    logger.info("[registered] framework:" + Str.id(id.getValue) + " master:" + Str.master(master))
    checkMesosVersion(master, driver)

    Cluster.frameworkId = id.getValue
    Cluster.save()

    this.driver = driver
    implicitReconcile(driver)
  }

  override def offerRescinded(driver: SchedulerDriver, id: OfferID) {
    logger.info("[offerRescinded] " + Str.id(id.getValue))
  }

  override def disconnected(driver: SchedulerDriver) {
    logger.info("[disconnected]")
    this.driver = null
  }

  override def reregistered(driver: SchedulerDriver, master: MasterInfo) {
    logger.info("[reregistered] master:" + Str.master(master))

    this.driver = driver
    implicitReconcile(driver)
  }

  override def slaveLost(driver: SchedulerDriver, id: SlaveID) {
    logger.info("[slaveLost] " + Str.id(id.getValue))
  }

  override def error(driver: SchedulerDriver, message: String) {
    logger.info("[error] " + message)
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus) {
    logger.info("[statusUpdate] " + Str.taskStatus(status))
    onTaskStatus(driver, status)
  }

  override def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]) {
    logger.info("[frameworkMessage] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " data: " + new String(data))
  }

  override def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]) {
    logger.debug("[resourceOffers]\n" + Str.offers(offers))
    onResourceOffers(offers.toList)
  }

  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {
    logger.info("[executorLost] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " status:" + status)
  }

  override def nodes: Traversable[Node] = Cluster.getNodes

  private def onResourceOffers(offers: List[Offer]) {
    for (offer <- offers) {
      val declineReason = acceptOffer(offer)

      if (declineReason != null) {
        driver.declineOffer(offer.getId)
        logger.info(s"Declined offer ${Str.offer(offer)}:\n  $declineReason")
      }
    }

    explicitReconcile(driver)
    Cluster.save()
  }

  private def acceptOffer(offer: Offer): String = {
    Cluster.getNodes.filter(_.state == Node.State.Stopped).toList.sortBy(_.id.toInt) match {
      case Nil => "all nodes are running"
      case nodes =>
        if (Cluster.getNodes.exists(node => node.state == Node.State.Staging || node.state == Node.State.Starting))
          "should wait until other nodes are started"
        else {
          // Consider starting seeds first
          val filteredNodes = nodes.filter(_.seed) match {
            case Nil => nodes
            case seeds =>
              logger.info("There are seed nodes to be launched so will prefer them first.")
              seeds
          }

          val reason = filteredNodes.flatMap { node =>
            checkSeedConstraints(offer, node).orElse(checkConstraints(offer, node)).orElse(Option(node.matches(offer))) match {
              case Some(declineReason) => Some(s"node ${node.id}: $declineReason")
              case None =>
                launchTask(node, offer)
                ""
            }
          }.mkString(", ")

          if (reason.isEmpty) null else reason
        }
    }
  }

  private def checkSeedConstraints(offer: Offer, node: Node): Option[String] = {
    if (node.seed) checkConstraintsWith(offer, node, _.seedConstraints, name => nodes.filter(_.seed).flatMap(_.attribute(name)).toList)
    else None
  }

  private def launchTask(node: Node, offer: Offer) {
    node.runtime = new Node.Runtime(node, offer)
    node.state = Node.State.Staging

    val task = node.newTask()
    driver.launchTasks(util.Arrays.asList(offer.getId), util.Arrays.asList(task), Filters.newBuilder().setRefuseSeconds(1).build)

    logger.info(s"Starting node ${node.id} with task ${node.runtime.taskId} for offer ${offer.getId.getValue}")
  }

  private def onTaskStatus(driver: SchedulerDriver, status: TaskStatus) {
    val node = Cluster.getNodes.find(_.id == Node.idFromTaskId(status.getTaskId.getValue))

    status.getState match {
      case TaskState.TASK_RUNNING => onTaskStarted(node, driver, status)
      case TaskState.TASK_LOST | TaskState.TASK_FAILED | TaskState.TASK_ERROR => onTaskFailed(node, status)
      case TaskState.TASK_FINISHED | TaskState.TASK_KILLED => onTaskFinished(node, status)
      case TaskState.TASK_STAGING | TaskState.TASK_STARTING =>
      case _ => logger.warn("Got unexpected node state: " + status.getState)
    }

    Cluster.save()
  }

  private def onTaskStarted(nodeOpt: Option[Node], driver: SchedulerDriver, status: TaskStatus) {
    nodeOpt match {
      case Some(node) =>
        node.state = Node.State.Running
        node.replaceAddress = null
      case None =>
        logger.info(s"Got ${status.getState} for unknown/stopped node, killing task ${status.getTaskId.getValue}")
        driver.killTask(status.getTaskId)
    }
  }

  private def onTaskFailed(nodeOpt: Option[Node], status: TaskStatus) {
    nodeOpt match {
      case Some(node) =>
        node.state = Node.State.Stopped
        node.runtime = null
      case None => logger.info(s"Got ${status.getState} for unknown/stopped node with task id ${status.getTaskId.getValue}")
    }
  }

  private def onTaskFinished(nodeOpt: Option[Node], status: TaskStatus) {
    nodeOpt match {
      case Some(node) =>
        node.state = Node.State.Inactive
        node.runtime = null
        logger.info(s"Node ${node.id} has finished")
      case None => logger.info(s"Got ${status.getState} for unknown/stopped node with task id ${status.getTaskId.getValue}")
    }
  }

  def stopNode(id: String): Option[Node] = {
    Cluster.getNodes.find(_.id == id) match {
      case Some(node) =>
        node.state match {
          case Node.State.Staging | Node.State.Starting | Node.State.Running =>
            driver.killTask(TaskID.newBuilder().setValue(node.runtime.taskId).build)
          case _ =>
        }

        node.state = Node.State.Inactive
        Some(node)
      case None =>
        logger.warn(s"Node $id was removed, ignoring its stop call")
        None
    }
  }

  def removeNode(id: String): Option[Node] = {
    Cluster.getNodes.find(_.id == id) match {
      case Some(node) =>
        stopNode(id)

        Cluster.removeNode(node)
        Some(node)
      case None =>
        logger.warn(s"Node $id is already removed")
        None
    }
  }

  private def checkMesosVersion(master: MasterInfo, driver: SchedulerDriver): Unit = {
    val minVersion = "0.23.0"
    val version = master.getVersion

    def versionNumber(ver: String): Int = {
      val parts = ver.split('.')
      parts(0).toInt * 1000000 + parts(1).toInt * 1000 + parts(2).toInt
    }

    if (version.isEmpty || versionNumber(version) < versionNumber(minVersion)) {
      val versionStr = if (version.isEmpty) "< \"0.23.0\"" else "\"" + version + "\""
      logger.fatal(s"""Minimum supported Mesos version is "$minVersion", whereas current version is $versionStr. Stopping Scheduler""")
      driver.stop
    }
  }

  private def resolveDeps() {
    for (file <- new File(".").listFiles()) {
      if (file.getName.matches(Config.jarMask)) Config.jar = file
      if (file.getName.matches(Config.dseMask)) Config.dse = file
    }

    if (Config.jar == null) throw new IllegalStateException(Config.jarMask + " not found in current dir")
    if (Config.dse == null) throw new IllegalStateException(Config.dseMask + " not found in in current dir")
  }

  private def initLogging() {
    System.setProperty("org.eclipse.jetty.util.log.class", classOf[JettyLog4jLogger].getName)

    BasicConfigurator.resetConfiguration()

    val root = Logger.getRootLogger
    root.setLevel(Level.INFO)

    Logger.getLogger("org.I0Itec.zkclient").setLevel(Level.WARN)

    val logger = Logger.getLogger(Scheduler.getClass)
    logger.setLevel(if (Config.debug) Level.DEBUG else Level.INFO)

    val layout = new PatternLayout("%d [%t] %-5p %c %x - %m%n")

    val appender: Appender = new ConsoleAppender(layout)

    root.addAppender(appender)
  }

  class JettyLog4jLogger extends org.eclipse.jetty.util.log.Logger {
    private var logger: Logger = Logger.getLogger("Jetty")

    def this(logger: Logger) {
      this()
      this.logger = logger
    }

    def isDebugEnabled: Boolean = logger.isDebugEnabled

    def setDebugEnabled(enabled: Boolean) = logger.setLevel(if (enabled) Level.DEBUG else Level.INFO)

    def getName: String = logger.getName

    def getLogger(name: String): org.eclipse.jetty.util.log.Logger = new JettyLog4jLogger(Logger.getLogger(name))

    def info(s: String, args: AnyRef*) = logger.info(format(s, args))

    def info(s: String, t: Throwable) = logger.info(s, t)

    def info(t: Throwable) = logger.info("", t)

    def debug(s: String, args: AnyRef*) = logger.debug(format(s, args))

    def debug(s: String, t: Throwable) = logger.debug(s, t)

    def debug(t: Throwable) = logger.debug("", t)

    def warn(s: String, args: AnyRef*) = logger.warn(format(s, args))

    def warn(s: String, t: Throwable) = logger.warn(s, t)

    def warn(s: String) = logger.warn(s)

    def warn(t: Throwable) = logger.warn("", t)

    def ignore(t: Throwable) = logger.info("Ignored", t)
  }

  private def format(s: String, args: AnyRef*): String = {
    var result: String = ""
    var i: Int = 0

    for (token <- s.split("\\{\\}")) {
      result += token
      if (args.length > i) result += args(i)
      i += 1
    }

    result
  }
}
