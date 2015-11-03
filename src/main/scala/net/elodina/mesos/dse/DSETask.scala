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

import com.google.protobuf.ByteString
import net.elodina.mesos.dse.cli.AddOptions
import net.elodina.mesos.utils.constraints.{Constrained, Constraint}
import net.elodina.mesos.utils.{State, Task, TaskRuntime, Util}
import org.apache.mesos.Protos
import org.apache.mesos.Protos._
import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.postfixOps


object TaskTypes {
  final val CASSANDRA_NODE: String = "cassandra-node"
}

trait DSETask extends Task with Constrained {
  val taskType: String

  var cpu: Double = 0.5
  var mem: Long = 512
  var broadcast: String = ""
  var nodeOut: String = ""
  var agentOut: String = ""
  var clusterName: String = ""
  var seed: Boolean = false
  var seeds: String = ""
  val constraints: mutable.Map[String, List[Constraint]] = new mutable.HashMap[String, List[Constraint]]

  override def attribute(name: String): Option[String] = {
    if (name == "hostname") runtime.map(_.hostname)
    else runtime.flatMap(_.attributes.get(name))
  }

  def matches(offer: Offer): Option[String] = {
    val offerResources = offer.getResourcesList.toList.map(res => res.getName -> res).toMap

    offerResources.get("cpus") match {
      case Some(cpusResource) => if (cpusResource.getScalar.getValue < cpu) return Some(s"cpus ${cpusResource.getScalar.getValue} < $cpu")
      case None => return Some("no cpus")
    }

    offerResources.get("mem") match {
      case Some(memResource) => if (memResource.getScalar.getValue.toLong < mem) return Some(s"mem ${memResource.getScalar.getValue.toLong} < $mem")
      case None => return Some("no mem")
    }

    None
  }

  def createTaskInfo(offer: Offer): TaskInfo = {
    val taskName = s"$taskType-$id"
    val taskId = TaskID.newBuilder().setValue(s"$taskName-${System.currentTimeMillis()}").build()

    Scheduler.setSeedNodes(this, offer.getHostname)

    TaskInfo.newBuilder().setName(taskName).setTaskId(taskId).setSlaveId(offer.getSlaveId)
      .setExecutor(createExecutorInfo(taskName))
      .setData(ByteString.copyFromUtf8(Json.stringify(Json.toJson(this))))
      .addResources(Protos.Resource.newBuilder().setName("cpus").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(this.cpu)))
      .addResources(Protos.Resource.newBuilder().setName("mem").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(this.mem)))
      .build()
  }

  private def createExecutorInfo(name: String): ExecutorInfo = {
    val commandBuilder = CommandInfo.newBuilder()
    commandBuilder
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/dse/" + Config.dse.getName).setExtract(true))
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/jre/" + Config.jre.getName).setExtract(true))
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/jar/" + Config.jar.getName))
      .setValue(s"$$(find jre* -maxdepth 0 -type d)/bin/java -cp ${Config.jar.getName}${if (Config.debug) " -Ddebug" else ""} net.elodina.mesos.dse.Executor")

    ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder().setValue(s"$name-${System.currentTimeMillis()}"))
      .setCommand(commandBuilder)
      .setName(name)
      .build
  }
}

object DSETask {
  def apply(id: String, opts: AddOptions): DSETask = {
    val task = opts.taskType match {
      case TaskTypes.CASSANDRA_NODE => CassandraNodeTask(id)
      case _ => throw new IllegalArgumentException(s"Unknown task type ${opts.taskType}")
    }

    task.cpu = opts.cpu
    task.mem = opts.mem
    task.broadcast = opts.broadcast
    Constraint.parse(opts.constraints).foreach(task.constraints +=)
    task.nodeOut = opts.nodeOut
    task.agentOut = opts.agentOut
    task.clusterName = opts.clusterName
    task.seed = opts.seed

    task
  }

  def idFromTaskId(taskId: String): String = {
    taskId.split("-", 4) match {
      case Array(_, _, value, _) => value
      case _ => throw new IllegalArgumentException(taskId)
    }
  }

  implicit val stateFormats = new Format[State.Value] {
    override def writes(o: State.Value): JsValue = JsString(o.toString)

    override def reads(json: JsValue): JsResult[State.Value] = json.validate[String].map(State.withName)
  }

  implicit val taskRuntimeFormats = Json.format[TaskRuntime]

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

  import DSETask._

  implicit val reader = (
    (__ \ 'id).read[String] and
    (__ \ 'state).read[State.Value] and
    (__ \ 'runtime).readNullable[TaskRuntime] and
    (__ \ 'cpu).read[Double] and
    (__ \ 'mem).read[Long] and
    (__ \ 'broadcast).read[String] and
    (__ \ 'nodeOut).read[String] and
    (__ \ 'agentOut).read[String] and
    (__ \ 'clusterName).read[String] and
    (__ \ 'seed).read[Boolean] and
    (__ \ 'seeds).read[String] and
    (__ \ 'constraints).read[String].map(Constraint.parse))((id, state, runtime, cpu, mem, broadcast,
      nodeOut, agentOut, clusterName, seed, seeds, constraints) => {

    val task = CassandraNodeTask(id)
    task.state = state
    task.runtime = runtime
    task.cpu = cpu
    task.mem = mem
    task.broadcast = broadcast
    task.nodeOut = nodeOut
    task.agentOut = agentOut
    task.clusterName = clusterName
    task.seed = seed
    task.seeds = seeds
    constraints.foreach(task.constraints +=)

    task
  })

  implicit val writer = new Writes[CassandraNodeTask] {
    override def writes(o: CassandraNodeTask): JsValue = {
      Json.obj(
        "type" -> o.taskType,
        "id" -> o.id,
        "state" -> Json.toJson(o.state),
        "runtime" -> Json.toJson(o.runtime),
        "cpu" -> o.cpu,
        "mem" -> o.mem,
        "broadcast" -> o.broadcast,
        "nodeOut" -> o.nodeOut,
        "agentOut" -> o.agentOut,
        "clusterName" -> o.clusterName,
        "seed" -> o.seed,
        "seeds" -> o.seeds,
        "constraints" -> Util.formatConstraints(o.constraints)
      )
    }
  }
}
