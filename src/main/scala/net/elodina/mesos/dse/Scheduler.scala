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
import java.util.{Collections, Date}

import _root_.net.elodina.mesos.utils.constraints.Constraints
import _root_.net.elodina.mesos.utils.{State, Pretty}
import org.apache.log4j
import org.apache.log4j._
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, SchedulerDriver}

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object Scheduler extends org.apache.mesos.Scheduler with Constraints[DSETask] {
  private val logger = Logger.getLogger(this.getClass)

  private[dse] val cluster = Cluster()
  private var driver: SchedulerDriver = null

  def start() {
    initLogging()
    logger.info(s"Starting scheduler:\n$Config")

    resolveDeps()
    cluster.load()
    HttpServer.start()

    val frameworkBuilder = FrameworkInfo.newBuilder()
    frameworkBuilder.setUser(Config.user)
    cluster.frameworkId.foreach(id => frameworkBuilder.setId(FrameworkID.newBuilder().setValue(id)))
    frameworkBuilder.setName(Config.frameworkName)
    frameworkBuilder.setFailoverTimeout(Config.frameworkTimeout.toUnit(TimeUnit.SECONDS))
    frameworkBuilder.setCheckpoint(true)

    val driver = new MesosSchedulerDriver(this, frameworkBuilder.build, Config.master)

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

    cluster.frameworkId = Some(id.getValue)
    cluster.save()

    this.driver = driver
//    reconcileTasks(force = true)
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
//    reconcileTasks(force = true)
  }

  override def slaveLost(driver: SchedulerDriver, id: SlaveID) {
    logger.info("[slaveLost] " + Pretty.id(id.getValue))
  }

  override def error(driver: SchedulerDriver, message: String) {
    logger.info("[error] " + message)
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus) {
    logger.info("[statusUpdate] " + Pretty.taskStatus(status))

//    onServerStatus(driver, status)
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

    //TODO reconcile tasks
    cluster.save()
  }

  private def acceptOffer(offer: Offer): Option[String] = {
    cluster.tasks.filter(_.state == State.Stopped).toList match {
      case Nil => Some("all tasks are running")
      case tasks =>
        val reason = tasks.flatMap { task =>
          checkConstraints(offer, task).orElse(task.matches(offer)) match {
            case Some(declineReason) => Some(s"task ${task.id}: $declineReason")
            case None =>
              launchTask(task, offer)
              None
          }
        }.mkString(", ")

        if (reason.isEmpty) None else Some(reason)
    }
  }

  private def launchTask(task: DSETask, offer: Offer) {

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
    //TODO configure this better
    System.setProperty("org.eclipse.jetty.util.log.class", classOf[JettyLog4jLogger].getName)

    BasicConfigurator.resetConfiguration()

    val root = Logger.getRootLogger
    root.setLevel(Level.INFO)

    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
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
