package net.elodina.mesos.dse

import org.junit.Test
import org.junit.Assert._
import java.util

class ExprTest extends MesosTestCase {
  @Test
  def expandNodes {
    val cluster = Scheduler.cluster

    for (i <- 0 until 5)
    cluster.addNode(new Node("" + i))

    try {
      assertEquals(util.Arrays.asList(), Expr.expandNodes(cluster, ""))
      fail()
    } catch { case e: IllegalArgumentException => }

    assertEquals(List("0"), Expr.expandNodes(cluster, "0"))
    assertEquals(List("0", "2", "4"), Expr.expandNodes(cluster, "0,2,4"))

    assertEquals(List("1", "2", "3"), Expr.expandNodes(cluster, "1..3"))
    assertEquals(List("0", "1", "3", "4"), Expr.expandNodes(cluster, "0..1,3..4"))

    assertEquals(List("0", "1", "2", "3", "4"), Expr.expandNodes(cluster, "*"))

    // duplicates
    assertEquals(List("0", "1", "2", "3", "4"), Expr.expandNodes(cluster, "0..3,2..4"))

    // sorting
    assertEquals(List("2", "3", "4"), Expr.expandNodes(cluster, "4,3,2"))

    // not-existent nodes
    assertEquals(List("5", "6", "7"), Expr.expandNodes(cluster, "5,6,7"))
  }

  @Test
  def expandNodes_attributes {
    val cluster = Scheduler.cluster
    val b0 = cluster.addNode(new Node("0"))
    val b1 = cluster.addNode(new Node("1"))
    val b2 = cluster.addNode(new Node("2"))
    cluster.addNode(new Node("3"))

    b0.runtime = new Node.Runtime(hostname = "master", attributes = Util.parseMap("a=1"))
    b1.runtime = new Node.Runtime(hostname = "slave0", attributes = Util.parseMap("a=2,b=2"))
    b2.runtime = new Node.Runtime(hostname = "slave1", attributes = Util.parseMap("b=2"))

    // exact match
    assertEquals(List("0", "1", "2", "3"), Expr.expandNodes(cluster, "*"))
    assertEquals(List("0"), Expr.expandNodes(cluster, "*[a=1]"))
    assertEquals(List("1", "2"), Expr.expandNodes(cluster, "*[b=2]"))

    // attribute present
    assertEquals(List("0", "1"), Expr.expandNodes(cluster, "*[a]"))
    assertEquals(List("1", "2"), Expr.expandNodes(cluster, "*[b]"))

    // hostname
    assertEquals(List("0"), Expr.expandNodes(cluster, "*[hostname=master]"))
    assertEquals(List("1", "2"), Expr.expandNodes(cluster, "*[hostname=slave*]"))

    // not existent node
    assertEquals(List(), Expr.expandNodes(cluster, "5[a]"))
    assertEquals(List(), Expr.expandNodes(cluster, "5[]"))
  }

  @Test
  def expandNodes_sortByAttrs {
    val cluster = Scheduler.cluster
    val b0 = cluster.addNode(new Node("0"))
    val b1 = cluster.addNode(new Node("1"))
    val b2 = cluster.addNode(new Node("2"))
    val b3 = cluster.addNode(new Node("3"))
    val b4 = cluster.addNode(new Node("4"))
    val b5 = cluster.addNode(new Node("5"))

    b0.runtime = new Node.Runtime(attributes = Util.parseMap("r=2,a=1"))
    b1.runtime = new Node.Runtime(attributes = Util.parseMap("r=0,a=1"))
    b2.runtime = new Node.Runtime(attributes = Util.parseMap("r=1,a=1"))
    b3.runtime = new Node.Runtime(attributes = Util.parseMap("r=1,a=2"))
    b4.runtime = new Node.Runtime(attributes = Util.parseMap("r=0,a=2"))
    b5.runtime = new Node.Runtime(attributes = Util.parseMap("r=0,a=2"))

    assertEquals(List("0", "1", "2", "3", "4", "5"), Expr.expandNodes(cluster, "*", sortByAttrs = true))
    assertEquals(List("1", "2", "0", "4", "3", "5"), Expr.expandNodes(cluster, "*[r]", sortByAttrs = true))
    assertEquals(List("1", "4", "2", "3", "0", "5"), Expr.expandNodes(cluster, "*[r,a]", sortByAttrs = true))

    assertEquals(List("1", "2", "0"), Expr.expandNodes(cluster, "*[r=*,a=1]", sortByAttrs = true))
    assertEquals(List("4", "3", "5"), Expr.expandNodes(cluster, "*[r,a=2]", sortByAttrs = true))
  }
}
