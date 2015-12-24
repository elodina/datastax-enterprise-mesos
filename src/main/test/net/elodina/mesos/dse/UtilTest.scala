package net.elodina.mesos.dse

import Util.{Range, Period, BindAddress, IO}
import org.junit.Test
import org.junit.Assert._
import java.io.File
import java.nio.file.Files

class UtilTest {
  // Period
  @Test
  def Period_init() {
    new Period("1m")

    // empty
    try {
      new Period("")
      fail()
    } catch { case e: IllegalArgumentException => }

    // zero without units
    new Period("0")

    // no units
    try {
      new Period("1")
      fail()
    } catch { case e: IllegalArgumentException => }

    // no value
    try {
      new Period("ms")
      fail()
    } catch { case e: IllegalArgumentException => }

    // wrong unit
    try {
      new Period("1k")
      fail()
    } catch { case e: IllegalArgumentException => }

    // non-integer value
    try {
      new Period("0.5m")
      fail()
    } catch { case e: IllegalArgumentException => }

    // invalid value
    try {
      new Period("Xh")
      fail()
    } catch { case e: IllegalArgumentException => }
  }

  @Test
  def Period_ms {
    assertEquals(0, new Period("0").ms)
    assertEquals(1, new Period("1ms").ms)
    assertEquals(10, new Period("10ms").ms)

    val s: Int = 1000
    assertEquals(s, new Period("1s").ms)
    assertEquals(10 * s, new Period("10s").ms)

    val m: Int = 60 * s
    assertEquals(m, new Period("1m").ms)
    assertEquals(10 * m, new Period("10m").ms)

    val h: Int = 60 * m
    assertEquals(h, new Period("1h").ms)
    assertEquals(10 * h, new Period("10h").ms)

    val d: Int = 24 * h
    assertEquals(d, new Period("1d").ms)
    assertEquals(10 * d, new Period("10d").ms)
  }

  @Test
  def Period_value {
    assertEquals(0, new Period("0").value)
    assertEquals(10, new Period("10ms").value)
    assertEquals(50, new Period("50h").value)
    assertEquals(20, new Period("20d").value)
  }

  @Test
  def Period_unit {
    assertEquals("ms", new Period("0").unit)
    assertEquals("ms", new Period("10ms").unit)
    assertEquals("h", new Period("50h").unit)
    assertEquals("d", new Period("20d").unit)
  }

  @Test
  def Period_toString {
    assertEquals("10ms", "" + new Period("10ms"))
    assertEquals("5h", "" + new Period("5h"))
  }

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
  def Range_contains {
    assertTrue(new Range(0).contains(0))
    assertTrue(new Range(0,1).contains(0))
    assertTrue(new Range(0,1).contains(1))

    val range = new Range(100, 200)
    assertTrue(range.contains(100))
    assertTrue(range.contains(150))
    assertTrue(range.contains(200))

    assertFalse(range.contains(99))
    assertFalse(range.contains(201))
  }

  @Test
  def Range_split {
    assertEquals(List(), new Range(0).split(0))

    assertEquals(List(new Range(1)), new Range(0, 1).split(0))
    assertEquals(List(new Range(0)), new Range(0, 1).split(1))

    assertEquals(List(new Range(0), new Range(2)), new Range(0, 2).split(1))
    assertEquals(List(new Range(100, 149), new Range(151, 200)), new Range(100, 200).split(150))

    try { new Range(100, 200).split(10); fail() }
    catch { case e: IllegalArgumentException => }

    try { new Range(100, 200).split(210); fail() }
    catch { case e: IllegalArgumentException => }
  }

  @Test
  def Range_toString {
    assertEquals("0", "" + new Range("0"))
    assertEquals("0..10", "" + new Range("0..10"))
    assertEquals("0", "" + new Range("0..0"))
  }

  // BindAddress
  @Test
  def BindAddress_init {
    new BindAddress("broker0")
    new BindAddress("192.168.*")
    new BindAddress("if:eth1")

    // unknown source
    try { new BindAddress("unknown:value"); fail() }
    catch { case e: IllegalArgumentException => }
  }

  @Test
  def BindAddress_resolve {
    // address without mask
    assertEquals("host", new BindAddress("host").resolve())

    // address with mask
    assertEquals("127.0.0.1", new BindAddress("127.0.0.*").resolve())

    // unresolvable
    try { new BindAddress("255.255.*").resolve(); fail() }
    catch { case e: IllegalStateException => }
  }

  @Test
  def IO_findDir {
    val dir: File = Files.createTempDirectory(classOf[UtilTest].getSimpleName).toFile

    try {
      assertNull(IO.findDir(dir, "mask.*"))

      val matchedDir: File = new File(dir, "mask-123")
      matchedDir.mkdir()
      assertEquals(matchedDir, IO.findDir(dir, "mask.*"))
    } finally {
      IO.delete(dir)
    }
  }

  @Test
  def IO_replaceInFile {
    val file: File = Files.createTempFile(classOf[UtilTest].getSimpleName, null).toFile

    IO.writeFile(file, "a=1\nb=2\nc=3")
    IO.replaceInFile(file, Map("a=*." -> "a=4", "b=*." -> "b=5"))
    assertEquals("a=4\nb=5\nc=3", IO.readFile(file))

    // error on miss
    IO.writeFile(file, "a=1\nb=2")
    try { IO.replaceInFile(file, Map("a=*." -> "a=3", "c=*." -> "c=4")) }
    catch { case e: IllegalStateException => assertTrue(e.getMessage, e.getMessage.contains("not found in file")) }

    // ignore misses
    IO.writeFile(file, "a=1\nb=2")
    IO.replaceInFile(file, Map("a=*." -> "a=3", "c=*." -> "c=4"), ignoreMisses = true)
    assertEquals("a=3\nb=2", IO.readFile(file))
  }
}
