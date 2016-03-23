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

  @Test
  def Size_init() {
    new Size("0")
    "kKmMgGtT".split("").foreach(unit => new Size("1" + unit))

    // empty
    try {
      new Size("")
      fail()
    } catch { case e: IllegalArgumentException => }

    // zero without units
    new Size("0")

    // no units, default: bytes
    new Size("1")

    // no value
    try {
      new Size("m")
      fail()
    } catch { case e: IllegalArgumentException => }

    // wrong unit
    try {
      new Size("1v")
      fail()
    } catch { case e: IllegalArgumentException => }

    // non-integer value
    try {
      new Size("0.5m")
      fail()
    } catch { case e: IllegalArgumentException => }

    // invalid value
    try {
      new Size("Xh")
      fail()
    } catch { case e: IllegalArgumentException => }
  }

  @Test
  def Size_common {
    assertEquals(0, new Size("0").bytes)
    assertEquals(1, new Size("1").bytes)

    Seq("kK", "mM", "gG", "tT").zipWithIndex.foreach { case (group, index) =>
      val bytesPerUnit = Math.pow(1024, index + 1).toLong
      val value = scala.util.Random.nextInt(1024)
      group.split("").filter(!_.isEmpty).foreach { unit =>
        val s = "" + value + unit
        val b = value * bytesPerUnit
        assertEquals(s"$s should be $b bytes", b, new Size(s).bytes)
        assertEquals(s"$s has value $value", value, new Size(s).value)
        assertEquals(s"$s has value $unit", if (unit.isEmpty) null else unit, new Size(s).unit)
        assertEquals(s"$s to string", s, "" + new Size(s))
      }
    }

    assertEquals("1", "" + new Size("1"))
  }

  @Test
  def Size_toUnit: Unit = {
    assertEquals(new Size("1k"), new Size("1024").toUnit("K"))
    assertEquals(new Size("1024k"), new Size("1048576").toUnit("K"))
    assertEquals(new Size("1m"), new Size("1024K").toUnit("M"))
    assertEquals(new Size("1g"), new Size("1024M").toUnit("G"))
    assertEquals(new Size("1t"), new Size("1024G").toUnit("T"))

    assertEquals(new Size("1024m"), new Size("1G").toUnit("M"))
    assertEquals(new Size("1024k"), new Size("1M").toUnit("K"))
    assertEquals(new Size("2048k"), new Size("2M").toUnit("K"))
    assertEquals(new Size("2048"), new Size("2k").toUnit(""))

    assertEquals(new Size("2048"), new Size("2k").toUnit(null))

    val b2048 = new Size("2048")
    assertSame(b2048, b2048.toB)
    val m2048 = new Size("2048M")
    assertSame(m2048, m2048.toM)
    val m1024 = new Size("1024m")
    assertSame(m1024, m1024.toM)

    assertEquals("0M", "" + new Size("0").toM)
    assertEquals("1M", "" + new Size("1572864").toM)
    assertEquals("1M", "" + new Size("1024K").toM)
    assertEquals("1M", "" + new Size("1054K").toM)
    assertEquals("1024M", "" + new Size("1G").toM)
    assertEquals("1048576M", "" + new Size("1T").toM)

    assertEquals(new Size("3072m"), new Size("3G").toM)
    assertEquals(new Size("1024k"), new Size("1M").toK)
    assertEquals(new Size("2048k"), new Size("2M").toK)
    assertEquals(new Size("2048"), new Size("2k").toB)
  }

  @Test
  def Size_normalize: Unit = {
    assertEquals("" + new Size("1K"), "" + new Size("1024").normalize)
    assertEquals("" + new Size("1M"), "" + new Size("1048576").normalize)
    assertEquals("" + new Size("1G"), "" + new Size("1073741824").normalize)
    assertEquals("" + new Size("1T"), "" + new Size("1099511627776").normalize)

    assertEquals("" + new Size("5K"), "" + new Size("5120").normalize)
    assertEquals("" + new Size("3M"), "" + new Size("3145728").normalize)
    assertEquals("" + new Size("7G"), "" + new Size("7516192768").normalize)
    assertEquals("" + new Size("4T"), "" + new Size("4398046511104").normalize)

    assertEquals("" + new Size("1M"), "" + new Size("1024K").normalize)
    assertEquals("" + new Size("1G"), "" + new Size("1048576K").normalize)
    assertEquals("" + new Size("1T"), "" + new Size("1073741824K").normalize)

    assertEquals("" + new Size("1G"), "" + new Size("1024M").normalize)
    assertEquals("" + new Size("1T"), "" + new Size("1048576M").normalize)

    assertEquals("" + new Size("1T"), "" + new Size("1024G").normalize)
  }
}
