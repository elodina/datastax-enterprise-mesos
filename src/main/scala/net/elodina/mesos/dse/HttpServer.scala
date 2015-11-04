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

import java.io._
import java.util.Scanner
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import net.elodina.mesos.dse.cli._
import net.elodina.mesos.utils.constraints.Constraint
import net.elodina.mesos.utils.{State, Util}
import org.apache.log4j.Logger
import org.eclipse.jetty.server.{Server, ServerConnector}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import play.api.libs.json._

import scala.util.{Failure, Success, Try}

case class ApiResponse(success: Boolean, message: String, value: Option[Cluster])

object ApiResponse {
  implicit val format = Json.format[ApiResponse]
}

object HttpServer {
  private val logger = Logger.getLogger(HttpServer.getClass)
  private var server: Server = null

  def start(resolveDeps: Boolean = true) {
    if (server != null) throw new IllegalStateException("HttpServer already started")

    val threadPool = new QueuedThreadPool(16)
    threadPool.setName("Jetty")

    server = new Server(threadPool)
    val connector = new ServerConnector(server)
    connector.setPort(Config.httpServerPort)
    connector.setIdleTimeout(60 * 1000)

    val handler = new ServletContextHandler
    handler.addServlet(new ServletHolder(Servlet), "/")

    server.setHandler(handler)
    server.addConnector(connector)
    server.start()

    logger.info("started on port " + connector.getPort)
  }

  def stop() {
    if (server == null) throw new IllegalStateException("HttpServer not started")

    server.stop()
    server.join()
    server = null

    logger.info("HttpServer stopped")
  }

  object Servlet extends HttpServlet {
    override def doPost(request: HttpServletRequest, response: HttpServletResponse) = doGet(request, response)

    override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
      Try(handle(request, response)) match {
        case Success(_) =>
        case Failure(e) =>
          logger.warn("", e)
          response.sendError(500, "" + e)
          throw e
      }
    }

    def handle(request: HttpServletRequest, response: HttpServletResponse) {
      val uri = request.getRequestURI
      if (uri.startsWith("/health")) handleHealth(response)
      else if (uri.startsWith("/jar/")) downloadFile(Config.jar, response)
      else if (uri.startsWith("/dse/")) downloadFile(Config.dse, response)
      else if (uri.startsWith("/jre/")) downloadFile(Config.jre, response)
      else if (uri.startsWith("/api")) handleApi(request, response)
      else response.sendError(404)
    }

    def downloadFile(file: File, response: HttpServletResponse) {
      response.setContentType("application/zip")
      response.setHeader("Content-Length", "" + file.length())
      response.setHeader("Content-Disposition", "attachment; filename=\"" + file.getName + "\"")
      Util.copyAndClose(new FileInputStream(file), response.getOutputStream)
    }

    def handleApi(request: HttpServletRequest, response: HttpServletResponse) {
      response.setContentType("application/json; charset=utf-8")
      var uri: String = request.getRequestURI.substring("/api".length)
      if (uri.startsWith("/")) uri = uri.substring(1)

      if (uri == "add") handleAddTask(request, response)
      else if (uri == "update") handleUpdateTask(request, response)
      else if (uri == "start") handleStartTask(request, response)
      else if (uri == "stop") handleStopTask(request, response)
      else if (uri == "remove") handleRemoveTask(request, response)
      else if (uri == "status") handleClusterStatus(request, response)
      else response.sendError(404)
    }

    def handleAddTask(request: HttpServletRequest, response: HttpServletResponse) {
      val opts = postBody(request).as[AddOptions]

      val ids = Scheduler.cluster.expandIds(opts.id)
      val existing = ids.filter(id => Scheduler.cluster.tasks.exists(_.id == id))
      if (existing.nonEmpty) respond(ApiResponse(success = false, s"Tasks ${existing.mkString(",")} already exist", None), response)
      else {
        val tasks = ids.map { id =>
          val task = DSETask(id, opts)
          Scheduler.cluster.tasks.add(task)
          logger.info(s"Added task $task")
          task
        }

        Scheduler.cluster.save()
        respond(ApiResponse(success = true, s"Added tasks ${opts.id}", Some(Cluster(tasks))), response)
      }
    }

    def handleUpdateTask(request: HttpServletRequest, response: HttpServletResponse) {
      val opts = postBody(request).as[UpdateOptions]

      val ids = Scheduler.cluster.expandIds(opts.id)
      val missing = ids.filter(id => !Scheduler.cluster.tasks.exists(_.id == id))
      if (missing.nonEmpty) respond(ApiResponse(success = false, s"Tasks ${missing.mkString(",")} do not exist", None), response)
      else {
        val tasks = ids.flatMap { id =>
          Scheduler.cluster.tasks.find(_.id == id) match {
            case Some(task) =>
              opts.cpu.foreach(task.cpu = _)
              opts.mem.foreach(task.mem = _)
              opts.broadcast.foreach(task.broadcast = _)
              opts.constraints.foreach { constraints =>
                task.constraints.clear()
                task.constraints ++= Constraint.parse(constraints)
              }
              opts.seedConstraints.foreach { seedConstraints =>
                task.seedConstraints.clear()
                task.seedConstraints ++= Constraint.parse(seedConstraints)
              }
              opts.nodeOut.foreach(task.nodeOut = _)
              opts.clusterName.foreach(task.clusterName = _)
              opts.seed.foreach(task.seed = _)

              logger.info(s"Updated task $id")
              Some(task)
            case None =>
              logger.warn(s"Task $id was removed, ignoring its update call")
              None
          }
        }

        Scheduler.cluster.save()
        respond(ApiResponse(success = true, s"Updated tasks ${opts.id}", Some(Cluster(tasks))), response)
      }
    }

    def handleStartTask(request: HttpServletRequest, response: HttpServletResponse) {
      val opts = postBody(request).as[StartOptions]

      val ids = Scheduler.cluster.expandIds(opts.id)
      val missing = ids.filter(id => !Scheduler.cluster.tasks.exists(_.id == id))
      if (missing.nonEmpty) respond(ApiResponse(success = false, s"Tasks ${missing.mkString(",")} do not exist", None), response)
      else {
        val tasks = ids.flatMap { id =>
          Scheduler.cluster.tasks.find(_.id == id) match {
            case Some(task) =>
              if (task.state == State.Inactive) {
                task.state = State.Stopped
                logger.info(s"Starting task $id")
              } else logger.warn(s"Task $id already started")
              Some(task)
            case None =>
              logger.warn(s"Task $id was removed, ignoring its start call")
              None
          }
        }

        if (opts.timeout.toMillis > 0) {
          val ok = tasks.forall(_.waitFor(State.Running, opts.timeout))
          if (ok) respond(ApiResponse(success = true, s"Started tasks ${opts.id}", Some(Cluster(tasks))), response)
          else respond(ApiResponse(success = true, s"Start tasks ${opts.id} timed out after ${opts.timeout}", None), response)
        } else respond(ApiResponse(success = true, s"Servers ${opts.id} scheduled to start", Some(Cluster(tasks))), response)
      }
    }

    def handleStopTask(request: HttpServletRequest, response: HttpServletResponse) {
      val opts = postBody(request).as[StopOptions]

      val ids = Scheduler.cluster.expandIds(opts.id)
      val missing = ids.filter(id => !Scheduler.cluster.tasks.exists(_.id == id))
      if (missing.nonEmpty) respond(ApiResponse(success = false, s"Tasks ${missing.mkString(",")} do not exist", None), response)
      else {
        val tasks = ids.flatMap(Scheduler.stopTask)
        Scheduler.cluster.save()
        respond(ApiResponse(success = true, s"Stopped tasks ${opts.id}", Some(Cluster(tasks))), response)
      }
    }

    def handleRemoveTask(request: HttpServletRequest, response: HttpServletResponse) {
      val opts = postBody(request).as[RemoveOptions]

      val ids = Scheduler.cluster.expandIds(opts.id)
      val missing = ids.filter(id => !Scheduler.cluster.tasks.exists(_.id == id))
      if (missing.nonEmpty) respond(ApiResponse(success = false, s"Tasks ${missing.mkString(",")} do not exist", None), response)
      else {
        val tasks = ids.flatMap(Scheduler.removeTask)
        Scheduler.cluster.save()
        respond(ApiResponse(success = true, s"Removed tasks ${opts.id}", Some(Cluster(tasks))), response)
      }
    }

    def handleClusterStatus(request: HttpServletRequest, response: HttpServletResponse) {
      respond(ApiResponse(success = true, s"Retrieved current cluster status", Some(Scheduler.cluster)), response)
    }

    private def handleHealth(response: HttpServletResponse) {
      response.setContentType("text/plain; charset=utf-8")
      response.getWriter.println("ok")
    }

    private def postBody(request: HttpServletRequest): JsValue = {
      val scanner = new Scanner(request.getReader).useDelimiter("\\A")
      try {
        Json.parse(scanner.next())
      } finally {
        scanner.close()
      }
    }

    private def respond(apiResponse: ApiResponse, response: HttpServletResponse) {
      response.getWriter.println(Json.toJson(apiResponse))
    }
  }

}
