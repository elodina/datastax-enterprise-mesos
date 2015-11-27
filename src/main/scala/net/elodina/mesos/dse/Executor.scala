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

import java.io.{PrintWriter, StringWriter}

import _root_.net.elodina.mesos.utils.Pretty
import org.apache.log4j._
import org.apache.mesos.Protos._
import org.apache.mesos.{ExecutorDriver, MesosExecutorDriver}
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.util.{Failure, Success}

object Executor extends org.apache.mesos.Executor {
  private val logger = Logger.getLogger(Executor.getClass)

  private var hostname: String = null
  private var node: DSENode = null
  private var agent: DatastaxAgent = null

  def main(args: Array[String]) {
    initLogging()

    val driver = new MesosExecutorDriver(Executor)
    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1

    sys.exit(status)
  }

  def registered(driver: ExecutorDriver, executor: ExecutorInfo, framework: FrameworkInfo, slave: SlaveInfo) {
    logger.info("[registered] framework:" + Pretty.framework(framework) + " slave:" + Pretty.slave(slave))

    this.hostname = slave.getHostname
  }

  def reregistered(driver: ExecutorDriver, slave: SlaveInfo) {
    logger.info("[reregistered] " + Pretty.slave(slave))

    this.hostname = slave.getHostname
  }

  def disconnected(driver: ExecutorDriver) {
    logger.info("[disconnected]")
  }

  def launchTask(driver: ExecutorDriver, taskInfo: TaskInfo) {
    logger.info("[launchTask] " + Pretty.task(taskInfo))

    val task = Json.parse(taskInfo.getData.toStringUtf8).as[DSETask]
    driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId).setState(TaskState.TASK_STARTING).build)

    new Thread {
      override def run() {
        setName(task.taskType)

        var env = Map[String, String]()
        findJreDir().foreach { env += "JAVA_HOME" -> _ }

        node = DSENode(task, driver, taskInfo, hostname, env)
        agent = DatastaxAgent(task, env)

        node.start()
        agent.start()

        Future(blocking(node.awaitConsistentState())).map {
          case true => driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId).setState(TaskState.TASK_RUNNING).build)
          case false => logger.info("Node stopped, abandon waiting for consistent state")
        }

        Future.firstCompletedOf(Seq(Future(node.await()), Future(agent.await()))).onComplete { result =>
          result match {
            case Success(exitCode) =>
              if ((exitCode == 0 || exitCode == 143) && (node.stopped || agent.stopped))
                driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId).setState(TaskState.TASK_FINISHED).build)
              else
                driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId).setState(TaskState.TASK_FAILED).setMessage(s"exitCode=$exitCode").build)

              node.stop()
              agent.stop()
            case Failure(ex) =>
              node.stop()
              agent.stop()
              sendTaskFailed(driver, taskInfo, ex)
          }

          driver.stop()
        }
      }
    }.start()
  }

  def killTask(driver: ExecutorDriver, id: TaskID) {
    logger.info("[killTask] " + id.getValue)

    node.stop()
    agent.stop()
  }

  def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]) {
    logger.info("[frameworkMessage] " + new String(data))
  }

  def shutdown(driver: ExecutorDriver) {
    logger.info("[shutdown]")

    node.stop()
    agent.stop()
  }

  def error(driver: ExecutorDriver, message: String) {
    logger.info("[error] " + message)
  }

  private def sendTaskFailed(driver: ExecutorDriver, task: TaskInfo, t: Throwable) {
    val stackTrace = new StringWriter()
    t.printStackTrace(new PrintWriter(stackTrace, true))

    driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId).setState(TaskState.TASK_FAILED)
      .setMessage("" + stackTrace).build)
  }

  private def initLogging() {
    BasicConfigurator.resetConfiguration()

    val root = Logger.getRootLogger
    root.setLevel(Level.INFO)

    val logger = Logger.getLogger(Executor.getClass.getPackage.getName)
    logger.setLevel(if (System.getProperty("debug") != null) Level.DEBUG else Level.INFO)

    val layout = new PatternLayout("%d [%t] %-5p %c %x - %m%n")
    root.addAppender(new ConsoleAppender(layout))
  }

  private[dse] def findJreDir(): Option[String] = {
    for (file <- new java.io.File(System.getProperty("user.dir")).listFiles()) {
      if (file.isDirectory && file.getName.matches(Config.jreMask))
        return Some(file.getCanonicalPath)
    }

    None
  }
}
