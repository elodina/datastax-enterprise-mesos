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

import java.util.UUID

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
  var logStdout: String = ""
  var logStderr: String = ""
  var agentStdout: String = ""
  var agentStderr: String = ""
  var clusterName: String = ""
  var seed: Boolean = false
  val constraints: mutable.Map[String, List[Constraint]] = new mutable.HashMap[String, List[Constraint]]

  override def attribute(name: String): Option[String] = {
    if (name == "hostname") task.map(_.hostname)
    else task.flatMap(_.attributes.get(name))
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

  def createMesosTask(offer: Offer): TaskInfo = {
    val taskName = s"$taskType-$id"
    val taskId = TaskID.newBuilder().setValue(s"$taskName-${UUID.randomUUID()}").build()

    TaskInfo.newBuilder().setName(taskName).setTaskId(taskId).setSlaveId(offer.getSlaveId)
      .setExecutor(createExecutor(taskName))
      .setData(ByteString.copyFromUtf8(Json.stringify(Json.toJson(this))))
      .addResources(Protos.Resource.newBuilder().setName("cpus").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(this.cpu)))
      .addResources(Protos.Resource.newBuilder().setName("mem").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(this.mem)))
      .build()
  }

  private def createExecutor(name: String): ExecutorInfo = {
    val commandBuilder = CommandInfo.newBuilder()
    commandBuilder
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/dse/" + Config.dse.getName).setExtract(true))
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/jre/" + Config.jre.getName).setExtract(true))
      .addUris(CommandInfo.URI.newBuilder().setValue(s"${Config.api}/jar/" + Config.jar.getName))
      .setValue(s"jre/bin/java -cp ${Config.jar.getName}${if (Config.debug) " -Ddebug" else ""} net.elodina.mesos.dse.Executor")

    ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder().setValue(id))
      .setCommand(commandBuilder)
      .setName(name)
      .build
  }
}

object DSETask {
  def apply(id: String, opts: AddOptions): DSETask = {
    val task = opts.taskType match {
      case TaskTypes.CASSANDRA_NODE => CassandraNodeTask(opts.id)
      case _ => throw new IllegalArgumentException(s"Unknown task type ${opts.taskType}")
    }

    task.cpu = opts.cpu
    task.mem = opts.mem
    task.broadcast = opts.broadcast
    Constraint.parse(opts.constraints).foreach(task.constraints +=)
    task.logStdout = opts.logStdout
    task.logStderr = opts.logStderr
    task.agentStdout = opts.agentStdout
    task.agentStderr = opts.agentStderr
    task.clusterName = opts.clusterName
    task.seed = opts.seed

    task
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
    (__ \ 'logStdout).read[String] and
    (__ \ 'logStderr).read[String] and
    (__ \ 'agentStdout).read[String] and
    (__ \ 'agentStderr).read[String] and
    (__ \ 'clusterName).read[String] and
    (__ \ 'seed).read[Boolean] and
    (__ \ 'constraints).read[String].map(Constraint.parse))((id, state, runtime, cpu, mem, broadcast,
      logStdout, logStderr, agentStdout, agentStderr, clusterName, seed, constraints) => {

    val task = CassandraNodeTask(id)
    task.state = state
    task.task = runtime
    task.cpu = cpu
    task.mem = mem
    task.broadcast = broadcast
    task.logStdout = logStdout
    task.logStderr = logStderr
    task.agentStdout = agentStdout
    task.agentStderr = agentStderr
    task.clusterName = clusterName
    task.seed = seed
    constraints.foreach(task.constraints +=)

    task
  })

  implicit val writer = new Writes[CassandraNodeTask] {
    override def writes(o: CassandraNodeTask): JsValue = {
      Json.obj(
        "type" -> o.taskType,
        "id" -> o.id,
        "state" -> Json.toJson(o.state),
        "runtime" -> Json.toJson(o.task),
        "cpu" -> o.cpu,
        "mem" -> o.mem,
        "broadcast" -> o.broadcast,
        "logStdout" -> o.logStdout,
        "logStderr" -> o.logStderr,
        "agentStdout" -> o.agentStdout,
        "agentStderr" -> o.agentStderr,
        "clusterName" -> o.clusterName,
        "seed" -> o.seed,
        "constraints" -> Util.formatConstraints(o.constraints)
      )
    }
  }
}
