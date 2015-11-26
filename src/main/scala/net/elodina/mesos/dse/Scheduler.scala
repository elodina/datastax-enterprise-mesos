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

import _root_.net.elodina.mesos.utils.constraints.Constraints
import _root_.net.elodina.mesos.utils.{Pretty, Reconciliation, State, TaskRuntime}
import com.google.protobuf.ByteString
import org.apache.log4j._
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, SchedulerDriver}

import scala.collection.JavaConversions._
import scala.concurrent.duration.{Duration, _}
import scala.language.postfixOps

object Scheduler extends org.apache.mesos.Scheduler with Constraints[DSETask] with Reconciliation[DSETask] {
  private val logger = Logger.getLogger(this.getClass)

  private[dse] val cluster = Cluster()
  private var driver: SchedulerDriver = null

  override protected val reconcileDelay: Duration = 20 seconds
  override protected val reconcileMaxTries: Int = 5

  def start() {
    initLogging()
    logger.info(s"Starting scheduler:\n$Config")

    resolveDeps()
    cluster.load()
    HttpServer.start()

    val frameworkBuilder = FrameworkInfo.newBuilder()
    frameworkBuilder.setUser(Config.user)
    cluster.frameworkId.foreach(id => frameworkBuilder.setId(FrameworkID.newBuilder().setValue(id)))
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
    logger.info("[registered] framework:" + Pretty.id(id.getValue) + " master:" + Pretty.master(master))
    checkMesosVersion(master, driver)

    cluster.frameworkId = Some(id.getValue)
    cluster.save()

    this.driver = driver
    implicitReconcile(driver)
  }

  override def offerRescinded(driver: SchedulerDriver, id: OfferID) {
    logger.info("[offerRescinded] " + Pretty.id(id.getValue))
  }

  override def disconnected(driver: SchedulerDriver) {
    logger.info("[disconnected]")

    this.driver = null
  }

  override def reregistered(driver: SchedulerDriver, master: MasterInfo) {
    logger.info("[reregistered] master:" + Pretty.master(master))

    this.driver = driver
    implicitReconcile(driver)
  }

  override def slaveLost(driver: SchedulerDriver, id: SlaveID) {
    logger.info("[slaveLost] " + Pretty.id(id.getValue))
  }

  override def error(driver: SchedulerDriver, message: String) {
    logger.info("[error] " + message)
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus) {
    logger.info("[statusUpdate] " + Pretty.taskStatus(status))

    onTaskStatus(driver, status)
  }

  override def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]) {
    logger.info("[frameworkMessage] executor:" + Pretty.id(executorId.getValue) + " slave:" + Pretty.id(slaveId.getValue) + " data: " + new String(data))
  }

  override def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]) {
    logger.debug("[resourceOffers]\n" + Pretty.offers(offers))

    onResourceOffers(offers.toList)
  }

  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {
    logger.info("[executorLost] executor:" + Pretty.id(executorId.getValue) + " slave:" + Pretty.id(slaveId.getValue) + " status:" + status)
  }

  override def tasks: Traversable[DSETask] = cluster.tasks

  private def onResourceOffers(offers: List[Offer]) {
    offers.foreach { offer =>
      acceptOffer(offer).foreach { declineReason =>
        driver.declineOffer(offer.getId)
        logger.info(s"Declined offer ${Pretty.offer(offer)}:\n  $declineReason")
      }
    }

    explicitReconcile(driver)
    cluster.save()
  }

  private def acceptOffer(offer: Offer): Option[String] = {
    cluster.tasks.filter(_.state == State.Stopped).toList.sortBy(_.id.toInt) match {
      case Nil => Some("all tasks are running")
      case tasks =>
        if (cluster.tasks.exists(task => task.state == State.Staging || task.state == State.Starting))
          Some("should wait until other tasks are started")
        else {
          // Consider starting seeds first
          val filteredTasks = tasks.filter(_.seed) match {
            case Nil => tasks
            case seeds =>
              logger.info("There are seed nodes to be launched so will prefer them first.")
              seeds
          }

          val reason = filteredTasks.flatMap { task =>
            checkSeedConstraints(offer, task).orElse(checkConstraints(offer, task)).orElse(task.matches(offer)) match {
              case Some(declineReason) => Some(s"task ${task.id}: $declineReason")
              case None =>
                launchTask(task, offer)
                None
            }
          }.mkString(", ")

          if (reason.isEmpty) None else Some(reason)
        }
    }
  }

  private def checkSeedConstraints(offer: Offer, task: DSETask): Option[String] = {
    if (task.seed) checkConstraintsWith(offer, task, _.seedConstraints, name => tasks.filter(_.seed).flatMap(_.attribute(name)).toList)
    else None
  }

  private def launchTask(task: DSETask, offer: Offer) {
    val taskInfo = task.createTaskInfo(offer)

    task.runtime = Some(new TaskRuntime(taskInfo, offer))
    task.state = State.Staging

    driver.launchTasks(util.Arrays.asList(offer.getId), util.Arrays.asList(taskInfo), Filters.newBuilder().setRefuseSeconds(1).build)
    logger.info(s"Starting task ${task.id} with taskid ${taskInfo.getTaskId.getValue} for offer ${offer.getId.getValue}")
  }

  private def onTaskStatus(driver: SchedulerDriver, status: TaskStatus) {
    val task = cluster.tasks.find(_.id == DSETask.idFromTaskId(status.getTaskId.getValue))

    status.getState match {
      case TaskState.TASK_RUNNING => onTaskStarted(task, driver, status)
      case TaskState.TASK_LOST | TaskState.TASK_FAILED | TaskState.TASK_ERROR => onTaskFailed(task, status)
      case TaskState.TASK_FINISHED | TaskState.TASK_KILLED => onTaskFinished(task, status)
      case TaskState.TASK_STAGING | TaskState.TASK_STARTING =>
      case _ => logger.warn("Got unexpected task state: " + status.getState)
    }

    cluster.save()
  }

  private def onTaskStarted(taskOpt: Option[DSETask], driver: SchedulerDriver, status: TaskStatus) {
    taskOpt match {
      case Some(task) =>
        task.state = State.Running
        task.replaceAddress = ""
      case None =>
        logger.info(s"Got ${status.getState} for unknown/stopped task, killing task ${status.getTaskId.getValue}")
        driver.killTask(status.getTaskId)
    }
  }

  private def onTaskFailed(taskOpt: Option[DSETask], status: TaskStatus) {
    taskOpt match {
      case Some(task) =>
        task.state = State.Stopped
        task.runtime = None
      case None => logger.info(s"Got ${status.getState} for unknown/stopped task with task id ${status.getTaskId.getValue}")
    }
  }

  private def onTaskFinished(taskOpt: Option[DSETask], status: TaskStatus) {
    taskOpt match {
      case Some(task) =>
        task.state = State.Inactive
        task.runtime = None
        logger.info(s"Task ${task.id} has finished")
      case None => logger.info(s"Got ${status.getState} for unknown/stopped task with task id ${status.getTaskId.getValue}")
    }
  }

  def stopTask(id: String): Option[DSETask] = {
    cluster.tasks.find(_.id == id) match {
      case Some(task) =>
        task.state match {
          case State.Staging | State.Starting | State.Running =>
            driver.killTask(TaskID.newBuilder().setValue(task.runtime.get.taskId).build)
          case _ =>
        }

        task.state = State.Inactive
        Some(task)
      case None =>
        logger.warn(s"Task $id was removed, ignoring its stop call")
        None
    }
  }

  def removeTask(id: String): Option[DSETask] = {
    cluster.tasks.find(_.id == id) match {
      case Some(task) =>
        stopTask(id)

        cluster.tasks -= task
        Some(task)
      case None =>
        logger.warn(s"Task $id is already removed")
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

  private[dse] def setSeedNodes(task: DSETask, hostname: String) {
    val seeds = cluster.tasks.collect {
      case cassandraNode: CassandraNodeTask => cassandraNode
    }.filter(c => c.seed && c.clusterName == task.clusterName)
      .filter(c => c.state == State.Staging || c.state == State.Starting || c.state == State.Running)
      .flatMap(_.attribute("hostname")).toList

    if (seeds.isEmpty) {
      if (!task.seed) {
        logger.warn(s"No seed nodes available and current not is not seed node. Forcing seed for node ${task.id}")
        task.seed = true
      }

      task.seeds = hostname
    } else task.seeds = seeds.sorted.mkString(",")
  }

  private def resolveDeps() {
    for (file <- new File(".").listFiles()) {
      if (file.getName.matches(Config.jarMask)) Config.jar = file
      if (file.getName.matches(Config.dseMask)) Config.dse = file
      if (file.getName.matches(Config.jreMask) && !file.isDirectory) Config.jre = file
    }

    if (Config.jar == null) throw new IllegalStateException(Config.jarMask + " not found in current dir")
    if (Config.dse == null) throw new IllegalStateException(Config.dseMask + " not found in in current dir")
    if (Config.jre == null) throw new IllegalStateException(Config.jreMask + " not found in in current dir")
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
