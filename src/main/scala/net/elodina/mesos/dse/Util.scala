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
import java.util.Date
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer
import java.util.regex.{Matcher, Pattern}
import java.net._

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

  def findAvailPort: Int = {
    var s: ServerSocket = null
    var port: Int = -1

    try {
      s = new ServerSocket(0)
      port = s.getLocalPort
    } finally { if (s != null) s.close(); }

    port
  }

  private def portAvail(address: String, port: Int): Boolean = {
    var socket: ServerSocket = null
    try {
      socket = new ServerSocket()
      socket.bind(new InetSocketAddress(address, port))
    } catch {
      case e: IOException => return false
    } finally {
      if (socket != null)
        try { socket.close() }
        catch { case e: IOException => }
    }

    true
  }

  class Range(s: String) {
    private var _start: Int = -1
    private var _end: Int = -1

    def this(start: Int, end: Int) = this(start + ".." + end)
    def this(start: Int) = this("" + start)

    parse()
    private def parse() {
      val idx = s.indexOf("..")

      if (idx == -1) {
        _start = Integer.parseInt(s)
        _end = _start
        return
      }

      _start = Integer.parseInt(s.substring(0, idx))
      _end = Integer.parseInt(s.substring(idx + 2))
      if (_start > _end) throw new IllegalArgumentException("start > end")
    }

    def start: Int = _start
    def end : Int = _end

    def overlap(r: Range): Range = {
      var x: Range = this
      var y: Range = r
      if (x.start > y.start) {
        val t = x
        x = y
        y = t
      }
      assert(x.start <= y.start)

      if (y.start > x.end) return null
      assert(y.start <= x.end)

      val start = y.start
      val end = Math.min(x.end, y.end)
      new Range(start, end)
    }

    def contains(p: Int): Boolean = start <= p && p <= end

    def split(p: Int): List[Range] = {
      if (!contains(p)) throw new IllegalArgumentException("point not in range")

      val result = new ListBuffer[Range]
      if (start < p) result += new Range(start, p - 1)
      if (p < end) result += new Range(p + 1, end)

      result.toList
    }

    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[Range]) return false
      val range = obj.asInstanceOf[Range]
      start == range.start && end == range.end
    }

    override def hashCode(): Int = 31 * start + end

    override def toString: String = if (start == end) "" + start else start + ".." + end
  }

  class Period(s: String) {
    private var _value: Long = 0
    private var _unit: String = null
    private var _ms: Long = 0

    parse()
    private def parse() {
      if (s.isEmpty) throw new IllegalArgumentException(s)

      var unitIdx = s.length - 1
      if (s.endsWith("ms")) unitIdx -= 1
      if (s == "0") unitIdx = 1

      try { _value = java.lang.Long.valueOf(s.substring(0, unitIdx)) }
      catch { case e: IllegalArgumentException => throw new IllegalArgumentException(s) }

      _unit = s.substring(unitIdx)
      if (s == "0") _unit = "ms"

      _ms = value
      if (_unit == "ms") _ms *= 1
      else if (_unit == "s") _ms *= 1000
      else if (_unit == "m") _ms *= 60 * 1000
      else if (_unit == "h") _ms *= 60 * 60 * 1000
      else if (_unit == "d") _ms *= 24 * 60 * 60 * 1000
      else throw new IllegalArgumentException(s)
    }

    def value: Long = _value
    def unit: String = _unit
    def ms: Long = _ms

    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[Period]) return false
      obj.asInstanceOf[Period]._ms == _ms
    }

    override def hashCode: Int = _ms.asInstanceOf[Int]
    override def toString: String = _value + _unit
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
        if (address != null && (port == -1 || portAvail(address, port)))
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

  object Str {
    def dateTime(date: Date): String = {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ssX").format(date)
    }

    def framework(framework: FrameworkInfo): String = {
      var s = ""

      s += id(framework.getId.getValue)
      s += " name: " + framework.getName
      s += " hostname: " + framework.getHostname
      s += " failover_timeout: " + framework.getFailoverTimeout

      s
    }

    def master(master: MasterInfo): String = {
      var s = ""

      s += id(master.getId)
      s += " pid:" + master.getPid
      s += " hostname:" + master.getHostname

      s
    }

    def slave(slave: SlaveInfo): String = {
      var s = ""

      s += id(slave.getId.getValue)
      s += " hostname:" + slave.getHostname
      s += " port:" + slave.getPort
      s += " " + resources(slave.getResourcesList)

      s
    }

    def offer(offer: Offer): String = {
      var s = ""

      s += offer.getHostname + id(offer.getId.getValue)
      s += " " + resources(offer.getResourcesList)
      if (offer.getAttributesCount > 0) s += " " + attributes(offer.getAttributesList)

      s
    }

    def offers(offers: Iterable[Offer]): String = {
      var s = ""

      for (offer <- offers)
        s += (if (s.isEmpty) "" else "\n") + Str.offer(offer)

      s
    }

    def task(task: TaskInfo): String = {
      var s = ""

      s += task.getTaskId.getValue
      s += " slave:" + id(task.getSlaveId.getValue)

      s += " " + resources(task.getResourcesList)
      s += " data:" + new String(task.getData.toByteArray)

      s
    }

    def resources(resources: util.List[Protos.Resource]): String = {
      var s = ""

      val order: util.List[String] = "cpus mem disk ports".split(" ").toList
      for (resource <- resources.sortBy(r => order.indexOf(r.getName))) {
        if (!s.isEmpty) s += "; "

        s += resource.getName
        if (resource.getRole != "*") {
          s += "(" + resource.getRole

          if (resource.hasReservation && resource.getReservation.hasPrincipal)
            s += ", " + resource.getReservation.getPrincipal

          s += ")"
        }

        if (resource.hasDisk)
          s += "[" + resource.getDisk.getPersistence.getId + ":" + resource.getDisk.getVolume.getContainerPath + "]"

        s += ":"

        if (resource.hasScalar)
          s += "%.2f".format(resource.getScalar.getValue)

        if (resource.hasRanges)
          for (range <- resource.getRanges.getRangeList)
            s += "[" + range.getBegin + ".." + range.getEnd + "]"
      }

      s
    }

    def attributes(attributes: util.List[Protos.Attribute]): String = {
      var s = ""

      for (attr <- attributes) {
        if (!s.isEmpty) s += ";"
        s += attr.getName + ":"

        if (attr.hasText) s += attr.getText.getValue
        if (attr.hasScalar) s +=  "%.2f".format(attr.getScalar.getValue)
      }

      s
    }

    def taskStatus(status: TaskStatus): String = {
      var s = ""
      s += status.getTaskId.getValue
      s += " " + status.getState.name()

      s += " slave:" + id(status.getSlaveId.getValue)

      if (status.getState != TaskState.TASK_RUNNING)
        s += " reason:" + status.getReason.name()

      if (status.getMessage != null && status.getMessage != "")
        s += " message:" + status.getMessage

      if (status.getData.size > 0)
        s += " data: " + status.getData.toStringUtf8

      s
    }

    def id(id: String): String = "#" + suffix(id, 5)

    def suffix(s: String, maxLen: Int): String = {
      if (s.length <= maxLen) return s
      s.substring(s.length - maxLen)
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
