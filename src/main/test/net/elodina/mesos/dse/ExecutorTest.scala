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

  @Test
  def xmxAndXmn: Unit = {
    val node = Nodes.addNode(new Node("0"))
    node.mem = 2048 // out of 16G
    node.cpu = 2.0
    val cp = new CassandraProcess(node, task(data = ""), "localhost")

    val dseDir: File = new File(Executor.dir, "dse-4.8.0")
    dseDir.mkdirs()

    Executor.resolveDeps()

    val confDir: File = Executor.cassandraConfDir
    confDir.mkdirs()

    node.runtime = new Node.Runtime(reservation = new Node.Reservation(ports = Map(Node.Port.JMX -> 5001)))

    val cassandraEnvSh: File = new File(confDir, "cassandra-env.sh")
    cassandraEnvSh.createNewFile()

    def resetCassandraEnvSh =
      Util.IO.writeFile(cassandraEnvSh,
        """
          |#MAX_HEAP_SIZE="4G"
          |#HEAP_NEWSIZE="800M"
          |JMX_PORT=3001
        """.stripMargin.replaceFirst(" +$", ""))

    import Math._
    // max(min(1/2 ram, 1024MB), min(1/4 ram, 8GB))
    def maxHeapSize(mem: Double): Double = max(min(1.0/2.0 * mem, 1024.0), min(1.0/4.0 * mem, 8 * 1024.0))
    // min(max_sensible_per_modern_cpu_core * num_cores, 1/4 * heap size)
    def newHeapSize(heapSizeMb: Double, cpu: Double): Double = min(100.0 * cpu, 1/4.0 * heapSizeMb)

    def check(jvmOptions: String, expectedMaxHeapSize: String, expectedYoungGenHeapSize: String) = {
      resetCassandraEnvSh

      node.cassandraJvmOptions = jvmOptions
      cp.editCassandraEnvSh(cassandraEnvSh)

      assertEquals(
        s"""
          |MAX_HEAP_SIZE=${expectedMaxHeapSize}
          |HEAP_NEWSIZE=${expectedYoungGenHeapSize}
          |JMX_PORT=${node.runtime.reservation.ports(Node.Port.JMX)}
        """.stripMargin.replaceFirst(" +$", ""), Util.IO.readFile(cassandraEnvSh))
    }

    check(null, "" + maxHeapSize(node.mem).toInt + "M", "" + newHeapSize(maxHeapSize(node.mem), node.cpu).toInt + "M")
    // -XmxN
    check("-Xmx1280M", "1280M", "" + newHeapSize(1280, node.cpu).toInt + "M")
    // -XmnN
    check("-Xmn128M", "" + maxHeapSize(node.mem).toInt + "M", "128M")
    // -XmxN -XmnN
    check("-Xmx2048M -Xmn200M", "2048M", "200M")
  }
}
