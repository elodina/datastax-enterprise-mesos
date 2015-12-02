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

import java.io.{IOException, InputStream, OutputStream}
import java.util

import org.apache.mesos.Protos
import org.apache.mesos.Protos._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.parsing.json.JSON

object Util {
  def parseList(s: String, entrySep: Char = ',', valueSep: Char = '=', nullValues: Boolean = true): List[(String, String)] = {
    def splitEscaped(s: String, sep: Char, unescape: Boolean = false): Array[String] = {
      val parts = new util.ArrayList[String]()

      var escaped = false
      var part = ""
      for (c <- s.toCharArray) {
        if (c == '\\' && !escaped) escaped = true
        else if (c == sep && !escaped) {
          parts.add(part)
          part = ""
        } else {
          if (escaped && !unescape) part += "\\"
          part += c
          escaped = false
        }
      }

      if (escaped) throw new IllegalArgumentException("open escaping")
      if (part != "") parts.add(part)

      parts.toArray(Array[String]())
    }

    val result = new mutable.ListBuffer[(String, String)]()
    if (s == null) return result.toList

    for (entry <- splitEscaped(s, entrySep)) {
      if (entry.trim.isEmpty) throw new IllegalArgumentException(s)

      val pair = splitEscaped(entry, valueSep, unescape = true)
      val key: String = pair(0).trim
      val value: String = if (pair.length > 1) pair(1).trim else null

      if (value == null && !nullValues) throw new IllegalArgumentException(s)
      result += key -> value
    }

    result.toList
  }

  def parseMap(s: String, entrySep: Char = ',', valueSep: Char = '=', nullValues: Boolean = true): Map[String, String] = parseList(s, entrySep, valueSep, nullValues).toMap

  def formatList(list: List[(String, _ <: Any)], entrySep: Char = ',', valueSep: Char = '='): String = {
    def escape(s: String): String = {
      var result = ""

      for (c <- s.toCharArray) {
        if (c == entrySep || c == valueSep || c == '\\') result += "\\"
        result += c
      }

      result
    }

    var s = ""
    list.foreach { tuple =>
      if (!s.isEmpty) s += entrySep
      s += escape(tuple._1)
      if (tuple._2 != null) s += valueSep + escape("" + tuple._2)
    }

    s
  }

  def formatMap(map: collection.Map[String, _ <: Any], entrySep: Char = ',', valueSep: Char = '='): String = formatList(map.toList, entrySep, valueSep)

  def formatConstraints(constraints: scala.collection.Map[String, List[Constraint]]): String = formatList(constraints.toList.flatMap { case (name, values) =>
    values.map(name -> _)
  })

  private val jsonLock = new Object

  def parseJson(json: String): Map[String, Object] = {
    jsonLock synchronized {
      val node: Map[String, Object] = JSON.parseFull(json).getOrElse(null).asInstanceOf[Map[String, Object]]
      if (node == null) throw new IllegalArgumentException("Failed to parse json: " + json)
      node
    }
  }

  def copyAndClose(in: InputStream, out: OutputStream): Unit = {
    val buffer = new Array[Byte](128 * 1024)
    var actuallyRead = 0

    try {
      while (actuallyRead != -1) {
        actuallyRead = in.read(buffer)
        if (actuallyRead != -1) out.write(buffer, 0, actuallyRead)
      }
    } finally {
      try {
        in.close()
      }
      catch {
        case ignore: IOException =>
      }

      try {
        out.close()
      }
      catch {
        case ignore: IOException =>
      }
    }
  }

  def getScalarResources(offer: Offer, name: String): Double = {
    offer.getResourcesList.foldLeft(0.0) { (all, current) =>
      if (current.getName == name) all + current.getScalar.getValue
      else all
    }
  }

  def getRangeResources(offer: Offer, name: String): List[Protos.Value.Range] = {
    offer.getResourcesList.foldLeft[List[Protos.Value.Range]](List()) { case (all, current) =>
      if (current.getName == name) all ++ current.getRanges.getRangeList
      else all
    }
  }
}
