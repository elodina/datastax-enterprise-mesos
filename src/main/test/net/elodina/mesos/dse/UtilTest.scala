package net.elodina.mesos.dse

import Util.{Range, IO}
import org.junit.Test
import org.junit.Assert._
import java.io.File
import java.nio.file.Files

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
  }
}
