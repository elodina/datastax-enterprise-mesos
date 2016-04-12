package net.elodina.mesos.dse

import org.junit.{Test, After, Before}
import org.junit.Assert._
import java.nio.file.Files
import java.io.File
import net.elodina.mesos.util.IO

class ExecutorTest extends DseMesosTestCase {
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
    IO.delete(Executor.dir)
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

  @Test
  def cassandra_mem_options: Unit = {
    val node = Nodes.addNode(new Node("0"))
    node.mem = 2048 // out of 16G
    node.cpu = 2.0
    val cp = new CassandraProcess(node, task("id", "name", "slave", ""), "localhost")

    val dseDir: File = new File(Executor.dir, "dse-4.8.0")
    dseDir.mkdirs()

    Executor.resolveDeps()

    val confDir: File = Executor.cassandraConfDir
    confDir.mkdirs()

    node.runtime = new Node.Runtime(reservation = new Node.Reservation(ports = Map(Node.Port.JMX -> 5001)))

    val cassandraEnvSh: File = new File(confDir, "cassandra-env.sh")
    cassandraEnvSh.createNewFile()

    def resetCassandraEnvSh =
      IO.writeFile(cassandraEnvSh,
        """
          |#MAX_HEAP_SIZE="4G"
          |#HEAP_NEWSIZE="800M"
          |JMX_PORT=3001
        """.stripMargin.replaceFirst(" +$", ""))

    def test(jvmOptions: String, expectedMaxHeap: String, expectedYoungGen: String) = {
      resetCassandraEnvSh

      node.cassandraJvmOptions = jvmOptions
      cp.editCassandraEnvSh(cassandraEnvSh)

      assertEquals(
        s"""
          |MAX_HEAP_SIZE=$expectedMaxHeap
          |HEAP_NEWSIZE=$expectedYoungGen
          |JMX_PORT=${node.runtime.reservation.ports(Node.Port.JMX)}
        """.stripMargin.replaceFirst(" +$", ""), IO.readFile(cassandraEnvSh))
    }

    // -XmxN
    test("-Xmx1280M", "1280M", "200M")
    // -XmnN
    test("-Xmn128M", "1024M", "128M")
    // -XmxN -XmnN
    test("-Xmx2048M -Xmn200M", "2048M", "200M")
  }
}
