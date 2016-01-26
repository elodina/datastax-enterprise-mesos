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

import org.apache.log4j.Logger

import scala.collection.mutable
import scala.util.parsing.json.{JSONArray, JSONObject}
import java.io.File

object Nodes {
  private val logger = Logger.getLogger(this.getClass)
  private[dse] var storage = Nodes.newStorage(Config.storage)

  var frameworkId: String = null
  private val clusters: mutable.ListBuffer[Cluster] = new mutable.ListBuffer[Cluster]
  private val nodes: mutable.ListBuffer[Node] = new mutable.ListBuffer[Node]

  reset()

  def getClusters: List[Cluster] = clusters.toList

  def getCluster(id: String): Cluster = clusters.filter(id == _.id).headOption.getOrElse(null)

  def defaultCluster: Cluster = getCluster("default")

  def addCluster(cluster: Cluster): Cluster = {
    if (getCluster(cluster.id) != null)
      throw new IllegalArgumentException(s"duplicate cluster ${cluster.id}")

    clusters += cluster
    cluster
  }

  def removeCluster(cluster: Cluster): Unit = {
    if (cluster == defaultCluster) throw new IllegalArgumentException("can't remove default cluster")
    cluster.getNodes.foreach(_.cluster = defaultCluster)
    clusters -= cluster
  }


  def getNodes: List[Node] = nodes.toList.sortBy(_.id.toInt)

  def getNode(id: String) = nodes.filter(id == _.id).headOption.getOrElse(null)

  def addNode(node: Node): Node = {
    if (getNode(node.id) != null) throw new IllegalArgumentException(s"duplicate node ${node.id}")
    nodes += node
    node
  }

  def removeNode(node: Node): Unit = { nodes -= node }


  def reset(): Unit = {
    frameworkId = null

    clusters.clear()
    val defaultCluster = new Cluster("default")
    addCluster(defaultCluster)

    nodes.clear()
  }

  def fromJson(json: Map[String, Any]): Unit = {
    if (json.contains("clusters")) {
      clusters.clear()
      for (clusterObj <- json("clusters").asInstanceOf[List[Map[String, Object]]])
        addCluster(new Cluster(clusterObj))
    }

    if (json.contains("nodes")) {
      nodes.clear()
      for (nodeJson <- json("nodes").asInstanceOf[List[Map[String, Object]]])
        addNode(new Node(nodeJson))
    }

    if (json.contains("frameworkId"))
      frameworkId = json("frameworkId").asInstanceOf[String]
  }

  def toJson: JSONObject = {
    val json = new mutable.LinkedHashMap[String, Object]()
    if (frameworkId != null) json("frameworkId") = frameworkId

    if (!clusters.isEmpty) {
      val clustersJson = clusters.map(_.toJson)
      json("clusters") = new JSONArray(clustersJson.toList)
    }

    if (!nodes.isEmpty) {
      val nodesJson = nodes.map(_.toJson())
      json("nodes") = new JSONArray(nodesJson.toList)
    }

    new JSONObject(json.toMap)
  }

  def save() = storage.save(this.toJson)

  def load() {
    val json = storage.load()
    if (json == null) {
      logger.info("No nodes state available")
      return
    }

    this.fromJson(json)
  }

  private[dse] def newStorage(storage: String): Storage = {
    storage.split(":", 2) match {
      case Array("file", fileName) => FileStorage(new File(fileName))
      case Array("zk", zk) => ZkStorage(zk)
      case _ => throw new IllegalArgumentException(s"Unsupported storage: $storage")
    }
  }
}