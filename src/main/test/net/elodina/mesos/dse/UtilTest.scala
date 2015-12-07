package net.elodina.mesos.dse

import Util.Range
import org.junit.Test
import org.junit.Assert._

class UtilTest {
  // Range
  @Test
  def Range_init {
    new Range("30")
    new Range("30..31")
    new Range(30)
    new Range(30, 31)

    // empty
    try { new Range(""); fail() }
    catch { case e: IllegalArgumentException => }

    // non int
    try { new Range("abc"); fail() }
    catch { case e: IllegalArgumentException => }

    // non int first
    try { new Range("abc..30"); fail() }
    catch { case e: IllegalArgumentException => }

    // non int second
    try { new Range("30..abc"); fail() }
    catch { case e: IllegalArgumentException => }

    // inverted range
    try { new Range("10..0"); fail() }
    catch { case e: IllegalArgumentException => }
  }

  @Test
  def Range_start_end {
    assertEquals(0, new Range("0").start)
    assertEquals(0, new Range("0..10").start)
    assertEquals(10, new Range("0..10").end)
  }

  @Test
  def Range_overlap {
    // no overlap
    assertNull(new Range(0, 10).overlap(new Range(20, 30)))
    assertNull(new Range(20, 30).overlap(new Range(0, 10)))
    assertNull(new Range(0).overlap(new Range(1)))

    // partial
    assertEquals(new Range(5, 10), new Range(0, 10).overlap(new Range(5, 15)))
    assertEquals(new Range(5, 10), new Range(5, 15).overlap(new Range(0, 10)))

    // includes
    assertEquals(new Range(2, 3), new Range(0, 10).overlap(new Range(2, 3)))
    assertEquals(new Range(2, 3), new Range(2, 3).overlap(new Range(0, 10)))
    assertEquals(new Range(5), new Range(0, 10).overlap(new Range(5)))

    // last point
    assertEquals(new Range(0), new Range(0, 10).overlap(new Range(0)))
    assertEquals(new Range(10), new Range(0, 10).overlap(new Range(10)))
    assertEquals(new Range(0), new Range(0).overlap(new Range(0)))
  }

  @Test
  def Range_toString {
    assertEquals("0", "" + new Range("0"))
    assertEquals("0..10", "" + new Range("0..10"))
    assertEquals("0", "" + new Range("0..0"))
  }
}
