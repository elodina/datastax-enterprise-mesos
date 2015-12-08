package net.elodina.mesos.dse

import org.junit.Test
import org.junit.Assert._
import java.util

class ExprTest extends MesosTestCase {
  @Test
  def expandNodes {
    for (i <- 0 until 5)
    Cluster.addNode(new Node("" + i))

    try {
      assertEquals(util.Arrays.asList(), Expr.expandNodes(""))
      fail()
    } catch { case e: IllegalArgumentException => }

    assertEquals(List("0"), Expr.expandNodes("0"))
    assertEquals(List("0", "2", "4"), Expr.expandNodes("0,2,4"))

    assertEquals(List("1", "2", "3"), Expr.expandNodes("1..3"))
    assertEquals(List("0", "1", "3", "4"), Expr.expandNodes("0..1,3..4"))

    assertEquals(List("0", "1", "2", "3", "4"), Expr.expandNodes("*"))

    // duplicates
    assertEquals(List("0", "1", "2", "3", "4"), Expr.expandNodes("0..3,2..4"))

    // sorting
    assertEquals(List("2", "3", "4"), Expr.expandNodes("4,3,2"))

    // not-existent nodes
    assertEquals(List("5", "6", "7"), Expr.expandNodes("5,6,7"))
  }

  @Test
  def expandNodes_attributes {
    val n0 = Cluster.addNode(new Node("0"))
    val n1 = Cluster.addNode(new Node("1"))
    val n2 = Cluster.addNode(new Node("2"))
    Cluster.addNode(new Node("3"))

    n0.runtime = new Node.Runtime(hostname = "master", attributes = Util.parseMap("a=1"))
    n1.runtime = new Node.Runtime(hostname = "slave0", attributes = Util.parseMap("a=2,b=2"))
    n2.runtime = new Node.Runtime(hostname = "slave1", attributes = Util.parseMap("b=2"))

    // exact match
    assertEquals(List("0", "1", "2", "3"), Expr.expandNodes("*"))
    assertEquals(List("0"), Expr.expandNodes("*[a=1]"))
    assertEquals(List("1", "2"), Expr.expandNodes("*[b=2]"))

    // attribute present
    assertEquals(List("0", "1"), Expr.expandNodes("*[a]"))
    assertEquals(List("1", "2"), Expr.expandNodes("*[b]"))

    // hostname
    assertEquals(List("0"), Expr.expandNodes("*[hostname=master]"))
    assertEquals(List("1", "2"), Expr.expandNodes("*[hostname=slave*]"))

    // not existent node
    assertEquals(List(), Expr.expandNodes("5[a]"))
    assertEquals(List(), Expr.expandNodes("5[]"))
  }

  @Test
  def expandNodes_sortByAttrs {
    val n0 = Cluster.addNode(new Node("0"))
    val n1 = Cluster.addNode(new Node("1"))
    val n2 = Cluster.addNode(new Node("2"))
    val n3 = Cluster.addNode(new Node("3"))
    val n4 = Cluster.addNode(new Node("4"))
    val n5 = Cluster.addNode(new Node("5"))

    n0.runtime = new Node.Runtime(attributes = Util.parseMap("r=2,a=1"))
    n1.runtime = new Node.Runtime(attributes = Util.parseMap("r=0,a=1"))
    n2.runtime = new Node.Runtime(attributes = Util.parseMap("r=1,a=1"))
    n3.runtime = new Node.Runtime(attributes = Util.parseMap("r=1,a=2"))
    n4.runtime = new Node.Runtime(attributes = Util.parseMap("r=0,a=2"))
    n5.runtime = new Node.Runtime(attributes = Util.parseMap("r=0,a=2"))

    assertEquals(List("0", "1", "2", "3", "4", "5"), Expr.expandNodes("*", sortByAttrs = true))
    assertEquals(List("1", "2", "0", "4", "3", "5"), Expr.expandNodes("*[r]", sortByAttrs = true))
    assertEquals(List("1", "4", "2", "3", "0", "5"), Expr.expandNodes("*[r,a]", sortByAttrs = true))

    assertEquals(List("1", "2", "0"), Expr.expandNodes("*[r=*,a=1]", sortByAttrs = true))
    assertEquals(List("4", "3", "5"), Expr.expandNodes("*[r,a=2]", sortByAttrs = true))
  }
}
