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
import net.elodina.mesos.utils.storage._
import org.apache.log4j.Logger
import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.collection.mutable

object Cluster {
  private val logger = Logger.getLogger(this.getClass)

  private def newStorage(storage: String): Storage[Cluster] = {
    storage.split(":", 2) match {
      case Array("file", fileName) => FileStorage(fileName)
      case Array("zk", zk) => ZkStorage(zk)
      case _ => throw new IllegalArgumentException(s"Unsupported storage: $storage")
    }
  }

  implicit val writer = new Writes[Cluster] {
    override def writes(o: Cluster): JsValue = Json.obj("frameworkid" -> o.frameworkId, "cluster" -> o.tasks.toList)
  }

  implicit val reader = ((__ \ 'cluster).read[List[DSETask]] and
  (__ \ 'frameworkid).readNullable[String])(Cluster.apply _)

  implicit object ClusterSerializer extends Encoder[Cluster] with Decoder[Cluster] {
    override def encode(value: Cluster): Array[Byte] = Json.stringify(Json.toJson(value)).getBytes("UTF-8")

    override def decode(bytes: Array[Byte]): Option[Cluster] = {
      try {
        Json.parse(bytes).validate[Cluster] match {
          case JsSuccess(cluster, _) => Some(cluster)
          case JsError(error) =>
            logger.info(s"Cannot decode cluster state: $error")
            None
        }
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          None
      }
    }
  }

}

case class Cluster(bootstrapTasks: List[DSETask] = Nil, var frameworkId: Option[String] = None) {
  private val storage = Cluster.newStorage(Config.storage)

  private[dse] val tasks = new mutable.HashSet[DSETask]()

  //add anything that was passed to constructor
  bootstrapTasks.foreach(tasks.add)

  def expandIds(expr: String): List[String] = {
    if (expr == null || expr == "") throw new IllegalArgumentException("ID expression cannot be null or empty")
    else {
      expr.split(",").flatMap { part =>
        utils.Range(part) match {
          case utils.Range.* => tasks.map(_.id).toList
          case range => range.values.map(_.toString)
        }
      }.distinct.sorted.toList
    }
  }

  def save() = storage.save(this)

  def load() {
    storage.load match {
      case Some(cluster) =>
        this.frameworkId = cluster.frameworkId
        cluster.tasks.foreach(this.tasks.add)
      case None => Cluster.logger.info("No cluster state available")
    }
  }
}
