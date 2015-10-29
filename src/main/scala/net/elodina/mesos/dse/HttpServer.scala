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

import net.elodina.mesos.dse.cli.AddOptions
import net.elodina.mesos.utils.Util
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
