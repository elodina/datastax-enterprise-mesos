package net.elodina.mesos.dse

import org.junit.{Test, After, Before}
import org.junit.Assert._
import java.nio.file.Files
import java.io.File

class ExecutorTest extends MesosTestCase {
  @Before
  override def before {
    super.before
    val dir: File = Files.createTempDirectory(classOf[ExecutorTest].getSimpleName).toFile
    dir.mkdir()
    Executor.dir = dir
  }

  @After
  override def after {
    super.after
    Util.IO.delete(Executor.dir)
    Executor.dir = new File(".")
  }

  @Test
  def jreDir {
    assertNull(Executor.jreDir)
    assertEquals(Executor._jreDir, None)

    Executor._jreDir = null
    val dir: File = new File(Executor.dir, "jre-1.7.0")
    dir.mkdirs()

    assertEquals(dir, Executor.jreDir)
    assertEquals(Some(dir), Executor._jreDir)
  }

 @Test
  def cassandraDir {
    assertNull(Executor.cassandraDir)
    assertEquals(Executor._cassandraDir, None)

    Executor._cassandraDir = null
    val dir: File = new File(Executor.dir, "apache-cassandra-3.0.1")
    dir.mkdirs()

    assertEquals(dir, Executor.cassandraDir)
    assertEquals(Some(dir), Executor._cassandraDir)
  }

  @Test
  def dseDir {
    assertNull(Executor.dseDir)
    assertEquals(Executor._dseDir, None)

    Executor._dseDir = null
    val dir: File = new File(Executor.dir, "dse-4.8.2")
    dir.mkdirs()

    assertEquals(dir, Executor.dseDir)
    assertEquals(Some(dir), Executor._dseDir)
  }
}
