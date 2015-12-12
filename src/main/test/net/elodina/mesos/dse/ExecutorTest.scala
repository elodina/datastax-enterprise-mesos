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
  def resolveDeps {
    try { Executor.resolveDeps(); fail() }
    catch { case e: IllegalStateException => assertTrue(e.getMessage, e.getMessage.contains("cassandra or dse dir should exist")) }
    assertNull(Executor.cassandraDir)
    assertNull(Executor.dseDir)
    assertNull(Executor.jreDir)

    val cassandraDir: File = new File(Executor.dir, "apache-cassandra-3.0.1")
    cassandraDir.mkdirs()

    val dseDir: File = new File(Executor.dir, "dse-4.8.2")
    dseDir.mkdirs()

    val jreDir: File = new File(Executor.dir, "jre-1.7.0")
    jreDir.mkdirs()

    Executor.resolveDeps()
    assertEquals(cassandraDir, Executor.cassandraDir)
    assertEquals(dseDir, Executor.dseDir)
    assertEquals(jreDir, Executor.jreDir)
  }
}
