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
import java.net.URI

import scala.concurrent.duration._
import scala.language.postfixOps

object Config {
  final val API_ENV = "DM_API"

  var debug: Boolean = false

  var master: String = null
  var user: String = null
  var principal: String = null
  var secret: String = null

  var api: String = null

  var storage: String = "file:dse-mesos.json"

  var frameworkName: String = "dse"
  var frameworkRole: String = "*"
  var frameworkTimeout: Duration = Duration("30 days")

  var jar: File = null
  var dse: File = null
  var cassandra: File = null
  var jre: File = null

  var namespace = "default"
  var cassandraKeyspace: String = null
  var cassandraTable: String = null

  def httpServerPort: Int = {
    val port = new URI(api).getPort
    if (port == -1) 80 else port
  }

  override def toString: String = {
    s"""
       |Debug:             $debug
       |Master:            $master
       |User:              ${if (user != null) user else "<auto>"}
       |Api:               $api
       |Storage:           $storage
       |Framework Name:    $frameworkName
       |Framework Role:    $frameworkRole
       |Framework Timeout: $frameworkTimeout
       |JRE:               ${if (jre != null) jre else "<pre-installed>"}
    """.stripMargin
  }

  def resolveDeps() {
    val jarMask = "dse-mesos.*jar"
    val dseMask = "dse.*gz"
    val cassandraMask = "apache-cassandra.*gz"

    for (file <- new File(".").listFiles()) {
      if (file.getName.matches(jarMask)) jar = file
      if (file.getName.matches(cassandraMask)) cassandra = file
      if (file.getName.matches(dseMask)) dse = file
    }

    if (jar == null) throw new IllegalStateException(jarMask + " not found in current dir")
    if (dse == null && cassandra == null)
      throw new IllegalStateException(s"Either $cassandraMask or $dseMask should be present in current dir")
  }
}

