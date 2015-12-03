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

import net.elodina.mesos.utils
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.util.parsing.json.{JSONArray, JSONObject}
import scala.collection.mutable.ListBuffer

object Cluster {
  private val logger = Logger.getLogger(this.getClass)

  private def newStorage(storage: String): Storage = {
    storage.split(":", 2) match {
      case Array("file", fileName) => FileStorage(fileName)
      case Array("zk", zk) => ZkStorage(zk)
      case _ => throw new IllegalArgumentException(s"Unsupported storage: $storage")
    }
  }
}

class Cluster {
  private val storage = Cluster.newStorage(Config.storage)

  var frameworkId: String = null

  private val nodes: mutable.ListBuffer[Node] = new mutable.ListBuffer[Node]
  private val rings: mutable.ListBuffer[Ring] = new mutable.ListBuffer[Ring]

  def this(bootstrapNodes: List[Node] = Nil, frameworkId: String = null) {
    this
    this.frameworkId = frameworkId
    bootstrapNodes.foreach(addNode)
  }
  
  def this(json: Map[String, Any]) {
    this
    fromJson(json)
  }

  def expandIds(expr: String): List[String] = {
    if (expr == null || expr == "") throw new IllegalArgumentException("ID expression cannot be null or empty")
    else {
      expr.split(",").flatMap { part =>
        utils.Range(part) match {
          case utils.Range.* => nodes.map(_.id).toList
          case range => range.values.map(_.toString)
        }
      }.distinct.sorted.toList
    }
  }


  def getRings: List[Ring] = rings.toList

  def addRing(ring: Ring): Ring = {
    rings += ring
    ring
  }

  def removeRing(ring: Ring): Unit = { rings -= ring }


  def getNodes: List[Node] = nodes.toList

  def addNode(node: Node): Node = {
    nodes += node
    node
  }

  def removeNode(node: Node): Unit = { nodes -= node }

  def clear(): Unit = {
    nodes.clear()
    rings.clear()
  }

  def fromJson(json: Map[String, Any]): Unit = {
    if (json.contains("nodes")) {
      for (nodeJson <- json("nodes").asInstanceOf[List[Map[String, Object]]]) {
        addNode(new Node(nodeJson))
      }
    }

    if (json.contains("rings")) {
      for (ringObj <- json("rings").asInstanceOf[List[Map[String, Object]]]) {
        addRing(new Ring(ringObj))
      }
    }

    if (json.contains("frameworkId"))
      frameworkId = json("frameworkId").asInstanceOf[String]
  }

  def toJson: JSONObject = {
    val json = new mutable.LinkedHashMap[String, Object]()

    if (!nodes.isEmpty) {
      val nodesJson = new ListBuffer[JSONObject]()
      nodes.foreach(nodesJson += _.toJson)
      json("nodes") = new JSONArray(nodesJson.toList)
    }

    if (!rings.isEmpty) {
      val ringsJson = new ListBuffer[JSONObject]()
      rings.foreach(ringsJson += _.toJson)
      json("rings") = new JSONArray(ringsJson.toList)
    }

    if (frameworkId != null) json("frameworkId") = frameworkId
    new JSONObject(json.toMap)
  }

  def save() = storage.save(this)

  def load() {
    val cluster: Cluster = storage.load()
    if (cluster == null) {
      Cluster.logger.info("No cluster state available")
      return
    }

    this.fromJson(Util.parseJsonAsMap("" + cluster.toJson))
  }
}
