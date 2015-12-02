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
import org.apache.log4j.Logger
import org.eclipse.jetty.server.{Server, ServerConnector}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import play.api.libs.json._

import scala.util.{Failure, Success, Try}
import scala.util.parsing.json.JSONObject
import scala.collection.mutable

class ApiResponse {
  var success: Boolean = false
  var message: String = null
  var cluster: Cluster = null

  def this(success: Boolean, message: String, cluster: Cluster) = {
    this
    this.success = success
    this.message = message
    this.cluster = cluster
  }

  def this(json: Map[String, Any]) = {
    this
    fromJson(json)
  }

  def fromJson(json: Map[String, Any]): Unit = {
    success = json("success").asInstanceOf[Boolean]
    message = json("message").asInstanceOf[String]
    if (json.contains("cluster")) cluster = new Cluster(json("cluster").asInstanceOf[Map[String, Any]])
  }

  def toJson: JSONObject = {
    val json = new mutable.LinkedHashMap[String, Any]()

    json("success") = success
    json("message") = message
    if (cluster != null) json("cluster") = cluster.toJson

    new JSONObject(json.toMap)
  }
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

      if (uri == "add") handleAddNode(request, response)
      else if (uri == "update") handleUpdateNode(request, response)
      else if (uri == "start") handleStartNode(request, response)
      else if (uri == "stop") handleStopNode(request, response)
      else if (uri == "remove") handleRemoveNode(request, response)
      else if (uri == "status") handleClusterStatus(request, response)
      else response.sendError(404)
    }

    def handleAddNode(request: HttpServletRequest, response: HttpServletResponse) {
      val opts = postBody(request).as[AddOptions]

      val ids = Scheduler.cluster.expandIds(opts.id)
      val existing = ids.filter(id => Scheduler.cluster.getNodes.exists(_.id == id))
      if (existing.nonEmpty) respond(new ApiResponse(success = false, s"Nodes ${existing.mkString(",")} already exist", null), response)
      else {
        val nodes = ids.map { id =>
          val node = Node(id, opts)
          Scheduler.cluster.addNode(node)
          logger.info(s"Added node $node")
          node
        }

        Scheduler.cluster.save()
        respond(new ApiResponse(success = true, s"Added nodes ${opts.id}", new Cluster(nodes)), response)
      }
    }

    def handleUpdateNode(request: HttpServletRequest, response: HttpServletResponse) {
      val opts = postBody(request).as[UpdateOptions]

      val ids = Scheduler.cluster.expandIds(opts.id)
      val missing = ids.filter(id => !Scheduler.cluster.getNodes.exists(_.id == id))
      if (missing.nonEmpty) respond(new ApiResponse(success = false, s"Nodes ${missing.mkString(",")} do not exist", null), response)
      else {
        val nodes = ids.flatMap { id =>
          Scheduler.cluster.getNodes.find(_.id == id) match {
            case Some(node_) =>
              opts.cpu.foreach(node_.cpu = _)
              opts.mem.foreach(node_.mem = _)
              opts.broadcast.foreach(node_.broadcast = _)
              opts.constraints.foreach { constraints =>
                node_.constraints.clear()
                node_.constraints ++= Constraint.parse(constraints)
              }
              opts.seedConstraints.foreach { seedConstraints =>
                node_.seedConstraints.clear()
                node_.seedConstraints ++= Constraint.parse(seedConstraints)
              }
              opts.nodeOut.foreach(node_.nodeOut = _)
              opts.clusterName.foreach(node_.clusterName = _)
              opts.seed.foreach(node_.seed = _)
              opts.replaceAddress.foreach(node_.replaceAddress = _)
              opts.dataFileDirs.foreach(node_.dataFileDirs = _)
              opts.commitLogDir.foreach(node_.commitLogDir = _)
              opts.savedCachesDir.foreach(node_.savedCachesDir = _)
              opts.awaitConsistentStateBackoff.foreach(node_.awaitConsistentStateBackoff = _)

              logger.info(s"Updated node $id")
              Some(node_)
            case None =>
              logger.warn(s"Node $id was removed, ignoring its update call")
              None
          }
        }

        Scheduler.cluster.save()
        respond(new ApiResponse(success = true, s"Updated nodes ${opts.id}", new Cluster(nodes)), response)
      }
    }

    def handleStartNode(request: HttpServletRequest, response: HttpServletResponse) {
      val opts = postBody(request).as[StartOptions]

      val ids = Scheduler.cluster.expandIds(opts.id)
      val missing = ids.filter(id => !Scheduler.cluster.getNodes.exists(_.id == id))
      if (missing.nonEmpty) respond(new ApiResponse(success = false, s"Nodes ${missing.mkString(",")} do not exist", null), response)
      else {
        val nodes = ids.flatMap { id =>
          Scheduler.cluster.getNodes.find(_.id == id) match {
            case Some(node_) =>
              if (node_.state == Node.State.Inactive) {
                node_.state = Node.State.Stopped
                logger.info(s"Starting node $id")
              } else logger.warn(s"Node $id already started")
              Some(node_)
            case None =>
              logger.warn(s"Node $id was removed, ignoring its start call")
              None
          }
        }

        if (opts.timeout.toMillis > 0) {
          val ok = nodes.forall(_.waitFor(Node.State.Running, opts.timeout))
          if (ok) respond(new ApiResponse(success = true, s"Started nodes ${opts.id}", new Cluster(nodes)), response)
          else respond(new ApiResponse(success = true, s"Start nodes ${opts.id} timed out after ${opts.timeout}", null), response)
        } else respond(new ApiResponse(success = true, s"Servers ${opts.id} scheduled to start", new Cluster(nodes)), response)
      }
    }

    def handleStopNode(request: HttpServletRequest, response: HttpServletResponse) {
      val opts = postBody(request).as[StopOptions]

      val ids = Scheduler.cluster.expandIds(opts.id)
      val missing = ids.filter(id => !Scheduler.cluster.getNodes.exists(_.id == id))
      if (missing.nonEmpty) respond(new ApiResponse(success = false, s"Nodes ${missing.mkString(",")} do not exist", null), response)
      else {
        val nodes = ids.flatMap(Scheduler.stopNode)
        Scheduler.cluster.save()
        respond(new ApiResponse(success = true, s"Stopped nodes ${opts.id}", new Cluster(nodes)), response)
      }
    }

    def handleRemoveNode(request: HttpServletRequest, response: HttpServletResponse) {
      val opts = postBody(request).as[RemoveOptions]

      val ids = Scheduler.cluster.expandIds(opts.id)
      val missing = ids.filter(id => !Scheduler.cluster.getNodes.exists(_.id == id))
      if (missing.nonEmpty) respond(new ApiResponse(success = false, s"Nodes ${missing.mkString(",")} do not exist", null), response)
      else {
        val nodes = ids.flatMap(Scheduler.removeNode)
        Scheduler.cluster.save()
        respond(new ApiResponse(success = true, s"Removed nodes ${opts.id}", new Cluster(nodes)), response)
      }
    }

    def handleClusterStatus(request: HttpServletRequest, response: HttpServletResponse) {
      respond(new ApiResponse(success = true, s"Retrieved current cluster status", Scheduler.cluster), response)
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
      response.getWriter.println("" + apiResponse.toJson)
    }
  }

}
