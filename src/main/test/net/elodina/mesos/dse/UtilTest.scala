package net.elodina.mesos.dse

import java.util.Arrays

import org.junit.Test
import org.junit.Assert._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.nio.file.Files
import java.net.{InetSocketAddress, ServerSocket}

import scala.collection.mutable.LinkedHashMap

import Util._


class UtilTest {
  @Test
  def parseMapTest() = {
    var map = parseMap("a=1,b=2")

    assertEquals(2, map.size)
    assertEquals(Some("1"), map.get("a"))
    assertEquals(Some("2"), map.get("b"))

    // missing pair
    try { map = parseMap("a=1,,b=2"); fail() }
    catch { case e: IllegalArgumentException => }

    // null value
    map = parseMap("a=1,b,c=3")
    assertEquals(3, map.size)
    assertEquals(Some(null), map.get("b"))

    try { parseMap("a=1,b,c=3", nullValues = false) }
    catch { case e: IllegalArgumentException => }

    // escaping
    map = parseMap("a=\\,,b=\\=,c=\\\\")
    assertEquals(3, map.size)
    assertEquals(Some(","), map.get("a"))
    assertEquals(Some("="), map.get("b"))
    assertEquals(Some("\\"), map.get("c"))

    // open escaping
    try { parseMap("a=\\"); fail() }
    catch { case e: IllegalArgumentException => }

    // null
    assertTrue(parseMap(null).isEmpty)
  }

  @Test
  def formatMapTest() = {
    val map = new LinkedHashMap[String, String]()
    map.put("a", "1")
    map.put("b", "2")
    assertEquals("a=1,b=2", formatMap(map))

    // null value
    map.put("b", null)
    assertEquals("a=1,b", formatMap(map))

    // escaping
    map.put("a", ",")
    map.put("b", "=")
    map.put("c", "\\")
    assertEquals("a=\\,,b=\\=,c=\\\\", formatMap(map))
  }

  @Test
  def parseJsonTest() = {
    val node = parseJson("{\"a\":\"1\", \"b\":\"2\"}").asInstanceOf[Map[String, Object]]
    assertEquals(2, node.size)
    assertEquals("1", node("a").asInstanceOf[String])
    assertEquals("2", node("b").asInstanceOf[String])
  }

  @Test
  def copyAndCloseTest() = {
    val data = new Array[Byte](16 * 1024)
    for (i <- data.indices) data(i) = i.toByte

    var inClosed = false
    var outClosed = false

    val in = new ByteArrayInputStream(data) {
      override def close(): Unit = super.close(); inClosed = true
    }
    val out = new ByteArrayOutputStream() {
      override def close(): Unit = super.close(); outClosed = true
    }

    IO.copyAndClose(in, out)
    assertTrue(Arrays.equals(data, out.toByteArray))
    assertTrue(inClosed)
    assertTrue(outClosed)
  }

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
    new BindAddress("if:eth1,if:eth2,192.168.*")

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

    // unknown ip
    assertEquals(null, new BindAddress("255.255.*").resolve())

    // unknown if
    assertEquals(null, new BindAddress("if:unknown").resolve())
  }

  @Test
  def BindAddress_resolve_checkPort {
    val port = findAvailPort

    // port avail
    val address: BindAddress = new BindAddress("127.*")
    assertEquals("127.0.0.1", address.resolve(port))

    // port unavail
    var socket: ServerSocket = null
    try {
      socket = new ServerSocket()
      socket.bind(new InetSocketAddress("127.0.0.1", port))
      assertEquals(null, address.resolve(port))
    } finally {
      if (socket != null) socket.close()
    }
  }

  @Test
  def IO_findFile0 {
    val dir: File = Files.createTempDirectory(classOf[UtilTest].getSimpleName).toFile

    try {
      assertNull(IO.findDir(dir, "mask.*"))

      val matchedFile: File = new File(dir, "mask-123")
      matchedFile.createNewFile()

      assertNull(IO.findFile0(dir, "mask.*", isDir = true))
      assertEquals(matchedFile, IO.findFile0(dir, "mask.*"))
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
