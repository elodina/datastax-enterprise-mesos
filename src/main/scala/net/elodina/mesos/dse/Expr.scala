package net.elodina.mesos.dse

import java.util
import scala.collection.JavaConversions._
import java.io.PrintStream
import java.lang.Comparable
import net.elodina.mesos.util.Strings

object Expr {
  def expandNodes(_expr: String, sortByAttrs: Boolean = false): List[String] = {
    var expr: String = _expr
    var attributes: util.Map[String, String] = null

    if (expr.endsWith("]")) {
      val filterIdx = expr.lastIndexOf("[")
      if (filterIdx == -1) throw new IllegalArgumentException("Invalid expr " + expr)

      attributes = Strings.parseMap(expr.substring(filterIdx + 1, expr.length - 1), true)
      expr = expr.substring(0, filterIdx)
    }

    var ids: util.List[String] = new util.ArrayList[String]()

    for (_part <- expr.split(",")) {
      val part = _part.trim()

      if (part.equals("*"))
        for (node <- Nodes.getNodes) ids.add(node.id)
      else if (part.contains("..")) {
        val idx = part.indexOf("..")

        var start: Integer = null
        var end: Integer = null
        try {
          start = Integer.parseInt(part.substring(0, idx))
          end = Integer.parseInt(part.substring(idx + 2, part.length))
        } catch {
          case e: NumberFormatException => throw new IllegalArgumentException("Invalid expr " + expr)
        }

        for (id <- start.toInt until end + 1)
          ids.add("" + id)
      } else {
        var id: Integer = null
        try { id = Integer.parseInt(part) }
        catch { case e: NumberFormatException => throw new IllegalArgumentException("Invalid expr " + expr) }
        ids.add("" + id)
      }
    }

    ids = new util.ArrayList[String](ids.distinct.sorted.toList)

    if (attributes != null)
      filterAndSortNodesByAttrs(ids, attributes, sortByAttrs)

    ids.toList
  }

  private def filterAndSortNodesByAttrs(ids: util.Collection[String], attributes: util.Map[String, String], sortByAttrs: Boolean): Unit = {
    def nodeAttr(node: Node, name: String): String = {
      if (node == null || node.runtime == null) return null

      val runtime: Node.Runtime = node.runtime
      if (name != "hostname") runtime.attributes.getOrElse(name, null) else runtime.hostname
    }

    def nodeMatches(node: Node): Boolean = {
      if (node == null) return false

      for (e <- attributes.entrySet()) {
        val expected = e.getValue
        val actual = nodeAttr(node, e.getKey)
        if (actual == null) return false

        if (expected != null) {
          if (expected.endsWith("*") && !actual.startsWith(expected.substring(0, expected.length - 1)))
            return false

          if (!expected.endsWith("*") && actual != expected)
            return false
        }
      }

      true
    }

     def filterNodes(): Unit = {
      val iterator = ids.iterator()
      while (iterator.hasNext) {
        val id = iterator.next()
        val node = Nodes.getNode(id)

        if (!nodeMatches(node))
          iterator.remove()
      }
    }

    def sortNodes(): Unit = {
      class Value(node: Node) extends Comparable[Value] {
        def compareTo(v: Value): Int = toString.compareTo(v.toString)

        override def hashCode(): Int = toString.hashCode

        override def equals(obj: scala.Any): Boolean = obj.isInstanceOf[Value] && toString == obj.toString

        override def toString: String = {
          val values = new util.LinkedHashMap[String, String]()

          for (k <- attributes.keySet()) {
            val value: String = nodeAttr(node, k)
            values.put(k, value)
          }

          Strings.formatMap(values)
        }
      }

      val values = new util.HashMap[Value, util.List[String]]()
      for (id <- ids) {
        val node: Node = Nodes.getNode(id)

        if (node != null) {
          val value = new Value(node)
          if (!values.containsKey(value)) values.put(value, new util.ArrayList[String]())
          values.get(value).add(id)
        }
      }

      val t = new util.ArrayList[String]()
      while (!values.isEmpty) {
        for (value <- new util.ArrayList[Value](values.keySet()).sorted) {
          val ids = values.get(value)

          val id: String = ids.remove(0)
          t.add(id)

          if (ids.isEmpty) values.remove(value)
        }
      }

      ids.clear()
      ids.addAll(t)
    }

    filterNodes()
    if (sortByAttrs) sortNodes()
  }

  def printNodeExprExamples(out: PrintStream): Unit = {
    out.println("node-expr examples:")
    out.println("  0      - node 0")
    out.println("  0,1    - nodes 0,1")
    out.println("  0..2   - nodes 0,1,2")
    out.println("  0,1..2 - nodes 0,1,2")
    out.println("  *      - any node")
    out.println("attribute filtering:")
    out.println("  *[rack=r1]           - any node having rack=r1")
    out.println("  *[hostname=slave*]   - any node on host with name starting with 'slave'")
    out.println("  0..4[rack=r1,dc=dc1] - any node having rack=r1 and dc=dc1")
  }
}
