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

import net.elodina.mesos.utils.Task
import play.api.libs.json._

object TaskTypes {
  final val CASSANDRA_NODE: String = "cassandra-node"
}

trait DSETask extends Task {
  val taskType: String
}

object DSETask {
  implicit val reader = new Reads[DSETask] {
    implicit val listReader: Reads[List[DSETask]] = Reads.list(this)

    override def reads(json: JsValue): JsResult[DSETask] = {
      (json \ "type").asOpt[String] match {
        case Some(t) => t match {
          case TaskTypes.CASSANDRA_NODE => CassandraNodeTask.reader.reads(json)
        }
        case None => play.api.libs.json.JsError("Missing type for DSETask")
      }
    }
  }

  implicit val writer = new Writes[DSETask] {
    override def writes(obj: DSETask): JsValue = {
      obj match {
        case cassandra: CassandraNodeTask => CassandraNodeTask.writer.writes(cassandra)
      }
    }
  }
}

case class CassandraNodeTask(id: String) extends DSETask {
  val taskType = TaskTypes.CASSANDRA_NODE
}

object CassandraNodeTask {
  implicit val reader = Json.reads[CassandraNodeTask]
  implicit val writer = Json.writes[CassandraNodeTask]
}
