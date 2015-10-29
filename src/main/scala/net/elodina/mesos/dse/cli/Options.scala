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

package net.elodina.mesos.dse.cli

import play.api.libs.json.Json

import scala.concurrent.duration.{Duration, _}
import scala.language.postfixOps

sealed trait Options

object NoOptions extends Options

case class SchedulerOptions(api: String = "", master: String = "", user: String = "", frameworkRole: String = "*",
                            frameworkName: String = "datastax-enterprise", frameworkTimeout: Duration = 30 days,
                            storage: String = "file:datastax.json", debug: Boolean = false) extends Options

case class AddOptions(taskType: String = "", id: String = "", api: String = "", cpu: Double = 0.5, mem: Long = 512,
                      broadcast: String = "", constraints: String = "", logStdout: String = "cassandra-node.log",
                      logStderr: String = "cassandra-node.err", agentStdout: String = "datastax-agent.log",
                      agentStderr: String = "datastax-agent.err", clusterName: String = "Test Cluster",
                      seed: Boolean = false) extends Options

object AddOptions {
  implicit val formats = Json.format[AddOptions]
}
