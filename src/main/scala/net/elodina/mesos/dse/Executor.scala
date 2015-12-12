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

import java.io.{File, PrintWriter, StringWriter}

import org.apache.log4j._
import org.apache.mesos.Protos._
import org.apache.mesos.{ExecutorDriver, MesosExecutorDriver}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.util.{Failure, Success}

import Util.Str

object Executor extends org.apache.mesos.Executor {
  private val logger = Logger.getLogger(Executor.getClass)
  private[dse] var dir: File = new File(".")

  private var hostname: String = null
  private var cassandraProcess: CassandraProcess = null
  private var agentProcess: AgentProcess = null

  def main(args: Array[String]) {
    initLogging()
    resolveDeps()

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
        if (Executor.jreDir != null) env += "JAVA_HOME" -> Executor.jreDir.toString

        cassandraProcess = CassandraProcess(node, driver, taskInfo, hostname, env)
        cassandraProcess.start()

        if (dseDir != null) {
          agentProcess = AgentProcess(node, env)
          agentProcess.start()
        }

        Future(blocking(cassandraProcess.awaitConsistentState())).map {
          case true => driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId).setState(TaskState.TASK_RUNNING).build)
          case false => logger.info("DSEProcess stopped, abandon waiting for consistent state")
        }

        Future.firstCompletedOf(Seq(Future(cassandraProcess.await()))).onComplete { result =>
          result match {
            case Success(exitCode) =>
              if ((exitCode == 0 || exitCode == 143) && cassandraProcess.stopped)
                driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId).setState(TaskState.TASK_FINISHED).build)
              else
                driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId).setState(TaskState.TASK_FAILED).setMessage(s"exitCode=$exitCode").build)

              stopProcesses()
            case Failure(ex) =>
              stopProcesses()
              sendTaskFailed(driver, taskInfo, ex)
          }

          driver.stop()
        }
      }
    }.start()
  }

  def killTask(driver: ExecutorDriver, id: TaskID) {
    logger.info("[killTask] " + id.getValue)
    stopProcesses()
  }

  def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]) {
    logger.info("[frameworkMessage] " + new String(data))
  }

  def shutdown(driver: ExecutorDriver) {
    logger.info("[shutdown]")
    stopProcesses()
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

  private def stopProcesses() {
    cassandraProcess.stop()
    if (agentProcess != null) agentProcess.stop()
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

  var cassandraDir: File = null
  var dseDir: File = null
  var jreDir: File = null

  def resolveDeps() {
    cassandraDir = Util.IO.findDir(dir, "apache-cassandra.*")
    dseDir = Util.IO.findDir(dir, "dse.*")
    jreDir = Util.IO.findDir(dir, "jre.*")

    if (dseDir == null && cassandraDir == null)
      throw new IllegalStateException("Either cassandra or dse dir should exist")
  }
}
