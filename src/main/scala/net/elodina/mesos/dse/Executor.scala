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

import java.io.{FileNotFoundException, File, PrintWriter, StringWriter}

import org.apache.log4j._
import org.apache.mesos.Protos._
import org.apache.mesos.{ExecutorDriver, MesosExecutorDriver}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.util.{Failure, Success}

import Util.Str

object Executor extends org.apache.mesos.Executor {
  private val logger = Logger.getLogger(Executor.getClass)

  private var hostname: String = null
  private var dseProcess: DSEProcess = null
  private var agentProcess: AgentProcess = null

  val dseDir = findDSEDir()

  def main(args: Array[String]) {
    initLogging()

    val driver = new MesosExecutorDriver(Executor)
    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1

    sys.exit(status)
  }

  def registered(driver: ExecutorDriver, executor: ExecutorInfo, framework: FrameworkInfo, slave: SlaveInfo) {
    logger.info("[registered] framework:" + Str.framework(framework) + " slave:" + Str.slave(slave))

    this.hostname = slave.getHostname
  }

  def reregistered(driver: ExecutorDriver, slave: SlaveInfo) {
    logger.info("[reregistered] " + Str.slave(slave))

    this.hostname = slave.getHostname
  }

  def disconnected(driver: ExecutorDriver) {
    logger.info("[disconnected]")
  }

  def launchTask(driver: ExecutorDriver, taskInfo: TaskInfo) {
    logger.info("[launchTask] " + Str.task(taskInfo))

    val json: Map[String, Any] = Util.parseJsonAsMap(taskInfo.getData.toStringUtf8)
    val node: Node = new Node(json, expanded = true)

    driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId).setState(TaskState.TASK_STARTING).build)

    new Thread {
      override def run() {
        setName("executor-processes")

        var env = Map[String, String]()
        findJreDir().foreach { env += "JAVA_HOME" -> _ }

        dseProcess = DSEProcess(node, driver, taskInfo, hostname, env)
        agentProcess = AgentProcess(node, env)

        dseProcess.start()
        agentProcess.start()

        Future(blocking(dseProcess.awaitConsistentState())).map {
          case true => driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId).setState(TaskState.TASK_RUNNING).build)
          case false => logger.info("DSEProcess stopped, abandon waiting for consistent state")
        }

        Future.firstCompletedOf(Seq(Future(dseProcess.await()), Future(agentProcess.await()))).onComplete { result =>
          result match {
            case Success(exitCode) =>
              if ((exitCode == 0 || exitCode == 143) && (dseProcess.stopped || agentProcess.stopped))
                driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId).setState(TaskState.TASK_FINISHED).build)
              else
                driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId).setState(TaskState.TASK_FAILED).setMessage(s"exitCode=$exitCode").build)

              dseProcess.stop()
              agentProcess.stop()
            case Failure(ex) =>
              dseProcess.stop()
              agentProcess.stop()
              sendTaskFailed(driver, taskInfo, ex)
          }

          driver.stop()
        }
      }
    }.start()
  }

  def killTask(driver: ExecutorDriver, id: TaskID) {
    logger.info("[killTask] " + id.getValue)

    dseProcess.stop()
    agentProcess.stop()
  }

  def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]) {
    logger.info("[frameworkMessage] " + new String(data))
  }

  def shutdown(driver: ExecutorDriver) {
    logger.info("[shutdown]")

    dseProcess.stop()
    agentProcess.stop()
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

  private[dse] def findDSEDir(): File = {
    for (file <- new File(".").listFiles()) {
      if (file.isDirectory && file.getName.matches(Config.dseDirMask) && file.getName != "dse-data")
        return file
    }

    throw new FileNotFoundException(s"${Config.dseDirMask} not found in current directory")
  }
}
