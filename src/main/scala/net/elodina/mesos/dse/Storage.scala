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
import java.nio.file.{Files, Paths}

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer

import scala.io.Source
import scala.util.parsing.json.JSONObject

trait Storage {
  def save(json: JSONObject)
  def load(): Map[String, Any]
}

case class FileStorage(file: String) extends Storage {
  override def save(json: JSONObject) {
    Files.write(Paths.get(file), json.toString().getBytes("utf-8"))
  }

  override def load(): Map[String, Any] = {
    if (!new File(file).exists()) null
    else {
      val source = Source.fromFile(file, "utf-8")
      try {
        Util.parseJsonAsMap(source.mkString)
      } finally {
        source.close()
      }
    }
  }
}

case class ZkStorage[T](zk: String) extends Storage {
  val (zkConnect, path) = zk.span(_ != '/')
  createChrootIfRequired()

  private def createChrootIfRequired() {
    if (path != "") {
      val client = zkClient
      try {
        client.createPersistent(path, true)
      }
      finally {
        client.close()
      }
    }
  }

  private def zkClient: ZkClient = new ZkClient(zkConnect, 30000, 30000, new BytesPushThroughSerializer)

  override def save(json: JSONObject) {
    val client = zkClient
    val encoded = json.toString().getBytes("utf-8")
    try {
      client.createPersistent(path, encoded)
    } catch {
      case e: ZkNodeExistsException => client.writeData(path, encoded)
    }
    finally {
      client.close()
    }
  }

  override def load(): Map[String, Any] = {
    val client = zkClient
    try {
      val bytes: Array[Byte] = client.readData(path, true).asInstanceOf[Array[Byte]]
      if (bytes == null) return null

      Util.parseJsonAsMap(new String(bytes, "utf-8"))
    } finally {
      client.close()
    }
  }
}

