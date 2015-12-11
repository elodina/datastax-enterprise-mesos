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

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConversions._

import org.apache.log4j.Logger
import java.util

case class AgentProcess(node: Node, env: Map[String, String] = Map.empty) {
  private val logger = Logger.getLogger(this.getClass)

  private val started = new AtomicBoolean(false)
  private[dse] var stopped: Boolean = false

  private var process: Process = null

  def start() {
    if (started.getAndSet(true)) throw new IllegalStateException("Datastax Agent already started")
    logger.info("Starting Datastax Agent")
    process = startProcess()
  }

  private def startProcess(): Process = {
    val cmd = util.Arrays.asList("" + new File(Executor.dseDir, "datastax-agent/bin/datastax-agent"), "-f")

    val out: File = new File("agent.log")
    val builder: ProcessBuilder = new ProcessBuilder(cmd)
      .redirectOutput(out)
      .redirectError(out)

    builder.environment().putAll(env)
    builder.start()
  }

  def await(): Int = {
    process.waitFor()
  }

  def stop() {
    this.synchronized {
      if (!stopped) {
        logger.info("Stopping Datastax Agent")

        stopped = true
        process.destroy()
      }
    }
  }
}
