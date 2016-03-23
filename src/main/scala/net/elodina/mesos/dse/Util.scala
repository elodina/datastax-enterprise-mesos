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

import java.io._
import java.util

import org.apache.mesos.Protos
import org.apache.mesos.Protos._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.parsing.json.JSON
import scala.collection.mutable.ListBuffer
import java.util.regex.{Matcher, Pattern}
import java.net._
import net.elodina.mesos.util.Net

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

  def formatConstraints(constraints: scala.collection.Map[String, List[Constraint]]): String = formatList(constraints.toList.flatMap { case (name, values) =>
    values.map(name -> _)
  })

  def parseJsonAsMap(json: String): Map[String, Any] = {
    parseJson(json).asInstanceOf[Map[String, Any]]
  }

  def parseJsonAsList(json: String): List[Any] = {
    parseJson(json).asInstanceOf[List[Any]]
  }

  private val jsonLock = new Object
  def parseJson(json: String): Any = {
    jsonLock synchronized {
      val node: Any = JSON.parseFull(json).getOrElse(null)
      if (node == null) throw new IllegalArgumentException("Failed to parse json: " + json)
      node
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

  class Size(s: String) {
    private var _value: Long = 0
    private var _unit: String = ""
    private var _bytes: Long = 0
    private var _bytesPerUnit: Long = 1L

    import Size._

    parse()
    private def parse() {
      if (s.isEmpty) throw new IllegalArgumentException(s)

      var unitIdx = s.length
      if (s.last.isLetter) {
        unitIdx -= 1
        _unit = s.substring(unitIdx)
      }

      try { _value = java.lang.Long.valueOf(s.substring(0, unitIdx)) }
      catch { case e: IllegalArgumentException => throw new IllegalArgumentException(s) }

      _bytesPerUnit = _unit match {
        case ""        => 1L
        case "k" | "K" => Kilobyte
        case "m" | "M" => Megabyte
        case "g" | "G" => Gigabyte
        case "t" | "T" => Terabyte
        case _ => throw new IllegalArgumentException(s)
      }

      _bytes = _value * _bytesPerUnit
    }

    def value: Long = _value
    def unit: String = _unit
    def bytes: Long = _bytes
    def bytesPerUnit: Long = _bytesPerUnit

    def normalize: Size = unit match {
      case "t" | "T" => this
      case _ if value % 1024 == 0 =>
        unit match {
          case ""        => new Size((value / 1024L) + "K").normalize
          case "k" | "K" => new Size((value / 1024L) + "M").normalize
          case "m" | "M" => new Size((value / 1024L) + "G").normalize
          case "g" | "G" => new Size((value / 1024L) + "T")
        }
      case _ => this
    }

    override def hashCode(): Int = _bytes.##

    override def toString: String = "" + _value + _unit

    def canEqual(other: Any): Boolean = other.isInstanceOf[Size]

    override def equals(other: Any): Boolean = other match {
      case that: Size => (that canEqual this) && _bytes == that._bytes
      case _ => false
    }

    def toUnit(to: String = ""): Size = {
      val dstUnit = if (to == null) "" else to
      val toPerUnit = new Size("0" + dstUnit).bytesPerUnit
      if (bytesPerUnit < toPerUnit) {
        new Size((value / (toPerUnit / bytesPerUnit)) + dstUnit)
      } else if (bytesPerUnit > toPerUnit) {
        new Size((value * (bytesPerUnit / toPerUnit)) + dstUnit)
      } else this
    }

    def toK: Size = toUnit("K")
    def toM: Size = toUnit("M")
    def toG: Size = toUnit("G")
    def toT: Size = toUnit("T")
    def toB: Size = toUnit("")
  }

  object Size {
    val Kilobyte = 1024L
    val Megabyte = Kilobyte * 1024L
    val Gigabyte = Megabyte * 1024L
    val Terabyte = Gigabyte * 1024L

    def apply(s: String): Size = new Size(s)
  }

  class BindAddress(s: String) {
    private val _values: ListBuffer[Value] = new ListBuffer[Value]()

    parse
    def parse {
      for (part <- s.split(",")) {
        _values += new Value(part.trim)
      }
    }

    def values: List[Value] = _values.toList

    def resolve(port: Int = -1): String = {
      for (value <- values) {
        val address: String = value.resolve()
        if (address != null && (port == -1 || Net.isPortAvail(address, port)))
          return address
      }

      null
    }

    override def hashCode(): Int = values.hashCode()

    override def equals(o: scala.Any): Boolean = {
      if (!o.isInstanceOf[BindAddress]) return false
      val address = o.asInstanceOf[BindAddress]
      values == address.values
    }

    override def toString: String = values.mkString(",")

    class Value(s: String) {
      private var _source: String = null
      private var _value: String = null

      def source: String = _source
      def value: String = _value

      parse
      def parse {
        val idx = s.indexOf(":")
        if (idx != -1) {
          _source = s.substring(0, idx)
          _value = s.substring(idx + 1)
        } else
          _value = s

        if (source != null && source != "if")
          throw new IllegalArgumentException(s)
      }

      def resolve(): String = {
        _source match {
          case null => resolveAddress(_value)
          case "if" => resolveInterfaceAddress(_value)
          case _ => throw new UnsupportedOperationException(s"Invalid source $s")
        }
      }

      def resolveAddress(addressOrMask: String): String = {
        if (!addressOrMask.endsWith("*")) return addressOrMask
        val prefix = addressOrMask.substring(0, addressOrMask.length - 1)

        for (ni <- NetworkInterface.getNetworkInterfaces) {
          val address = ni.getInetAddresses.find(_.getHostAddress.startsWith(prefix)).getOrElse(null)
          if (address != null) return address.getHostAddress
        }

        null
      }

      def resolveInterfaceAddress(name: String): String = {
        val ni = NetworkInterface.getNetworkInterfaces.find(_.getName == name).getOrElse(null)
        if (ni == null) return null

        val addresses: util.Enumeration[InetAddress] = ni.getInetAddresses
        val address = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(null)
        if (address != null) return address.getHostAddress

        null
      }

      override def hashCode(): Int = 31 * _source.hashCode + _value.hashCode

      override def equals(o: scala.Any): Boolean = {
        if (!o.isInstanceOf[Value]) return false
        val address = o.asInstanceOf[Value]
        _source == address._source && _value == address._value
      }

      override def toString: String = s
    }
  }

  object IO {
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

    def delete(file: File): Unit = {
      if (file.isDirectory) {
        val files: Array[File] = file.listFiles()
        for (file <- files) delete(file)
      }

      file.delete()
    }

    def findFile(dir: File, mask: String): File = findFile0(dir, mask)
    def findDir(dir: File, mask: String): File = findFile0(dir, mask, isDir = true)

    private[dse] def findFile0(dir: File, mask: String, isDir: Boolean = false): File = {
      for (file <- dir.listFiles())
        if (file.getName.matches(mask) && (isDir && file.isDirectory || !isDir && file.isFile))
          return file

      null
    }

    def readFile(file: File): String = {
      val buffer = new ByteArrayOutputStream()
      copyAndClose(new FileInputStream(file), buffer)
      buffer.toString("utf-8")
    }

    def writeFile(file: File, content: String): Unit = {
      copyAndClose(new ByteArrayInputStream(content.getBytes("utf-8")), new FileOutputStream(file))
    }

    def replaceInFile(file: File, replacements: Map[String, String], ignoreMisses: Boolean = false) {
      var content = readFile(file)

      for ((regex, value) <- replacements) {
        val matcher: Matcher = Pattern.compile(regex).matcher(content)
        if (!ignoreMisses && !matcher.find()) throw new IllegalStateException(s"regex $regex not found in file $file")

        content = matcher.replaceAll(value)
      }

      writeFile(file, content)
    }
  }

  // to allow transformations Map[String, Any] => Map[String, Any] and be able to serialize to JSON
  // without adding to much boilerplate such as wrapping manually map into JSONObject, list into JSONArray
  import scala.util.parsing.json.{JSONObject, JSONArray}
  import scala.util.parsing.json.JSONFormat.{ValueFormatter, quoteString}
  val jsonFormatter: ValueFormatter = (x : Any) => x match {
    case s: String => "\"" + quoteString(s) + "\""
    case jo: JSONObject => jo.toString(jsonFormatter)
    case ja: JSONArray => ja.toString(jsonFormatter)
    case m: Map[_, _] => new JSONObject(m.asInstanceOf[Map[String, Any]]).toString(jsonFormatter)
    case l: List[_] => new JSONArray(l).toString(jsonFormatter)
    case o => o.toString
  }
}
