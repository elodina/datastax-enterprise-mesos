package net.elodina.mesos.dse

import java.util.Date

import net.elodina.mesos.dse.Node.{Failover, Reservation, Stickiness}
import net.elodina.mesos.dse.Util.BindAddress
import net.elodina.mesos.util.Period
import org.cassandraunit.CassandraCQLUnit
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.junit.Assert._
import org.junit.{Ignore, Before, Rule, Test}

import scala.collection.JavaConverters._

object CassandraStorageTest {
  //these should match cql dataset file
  private val CassandraKeyspace = "dse_mesos"
  private val CassandraTable = "dse_mesos_framework"

  private def createCluster(id: String, bindAddress: String, ports: Map[Node.Port.Value, Util.Range],
                            jmxRemote: Boolean, jmxUser: String, jmxPassword: String): Cluster = {
    val cluster = new Cluster(id)
    cluster.bindAddress = new BindAddress(bindAddress)
    cluster.ports = cluster.ports ++= ports
    cluster.jmxRemote = jmxRemote
    cluster.jmxUser = jmxUser
    cluster.jmxPassword = jmxPassword

    cluster
  }

  private def createStickiness(period: String, hostname: String, stopTime: Date): Stickiness = {
    val stickiness = new Stickiness(new Period(period))
    stickiness.hostname = hostname
    stickiness.stopTime = stopTime

    stickiness
  }

  private def createNode(id: String, state: Node.State.Value, cluster: Cluster, stickiness: Stickiness,
                         runtime: Node.Runtime, cpu: Double, mem: Long, seed: Boolean,
                         jvmOptions: String, rack: String, dc: String, constraints: Map[String, List[Constraint]],
                         seedConstraints: Map[String, List[Constraint]], dataFileDirs: String, commitLogDir: String,
                         savedCachesDir: String, cassandraDotYaml: Map[String, String], addressDotYaml: Map[String, String],
                         cassandraJvmOptions: String, modified: Boolean, failover: Node.Failover): Node = {
    val node = new Node(id)
    node.state = state
    node.cluster = cluster
    node.stickiness = stickiness
    node.runtime = runtime

    node.cpu = cpu
    node.mem = mem

    node.seed = seed
    node.jvmOptions = jvmOptions

    node.rack = rack
    node.dc = dc

    node.constraints = node.constraints ++= constraints
    node.seedConstraints = node.seedConstraints ++= seedConstraints

    node.dataFileDirs = dataFileDirs
    node.commitLogDir = commitLogDir
    node.savedCachesDir = savedCachesDir

    node.cassandraDotYaml = node.cassandraDotYaml ++= cassandraDotYaml
    node.addressDotYaml = node.addressDotYaml ++= addressDotYaml
    node.cassandraJvmOptions = cassandraJvmOptions

    node.modified = modified

    node.failover = failover

    node
  }

  private def assertClusterEquals(expected: Cluster, actual: Cluster): Unit = {
    assertEquals(expected.id, actual.id)
    assertEquals(expected.bindAddress.values, actual.bindAddress.values)
    assertEquals(expected.ports, actual.ports)
    assertEquals(expected.jmxRemote, actual.jmxRemote)
    assertEquals(expected.jmxUser, actual.jmxUser)
    assertEquals(expected.jmxPassword, actual.jmxPassword)
  }

  private def assertNodeEquals(expected: Node, actual: Node): Unit = {
    assertEquals(expected.id, actual.id)
    assertEquals(expected.state, actual.state)
    assertEquals(expected.cluster.id, actual.cluster.id)

    if (expected.stickiness == null || actual.stickiness == null) {
      assertTrue(expected.stickiness == null && actual.stickiness == null)
    } else {
      assertEquals(expected.stickiness.period, actual.stickiness.period)
      assertEquals(expected.stickiness.hostname, actual.stickiness.hostname)
      assertEquals(expected.stickiness.stopTime, actual.stickiness.stopTime)
    }

    if (expected.runtime == null || actual.runtime == null) {
      assertTrue(expected.runtime == null && actual.runtime == null)
    } else {
      assertEquals(expected.runtime.taskId, actual.runtime.taskId)
      assertEquals(expected.runtime.executorId, actual.runtime.executorId)
      assertEquals(expected.runtime.slaveId, actual.runtime.slaveId)
      assertEquals(expected.runtime.hostname, actual.runtime.hostname)
      assertEquals(expected.runtime.address, actual.runtime.address)
      assertEquals(expected.runtime.seeds, actual.runtime.seeds)
      assertEquals(expected.runtime.attributes, actual.runtime.attributes)
      assertEquals(expected.runtime.reservation.cpus, actual.runtime.reservation.cpus, 0.0d)
      assertEquals(expected.runtime.reservation.mem, actual.runtime.reservation.mem)
      assertEquals(expected.runtime.reservation.ports, actual.runtime.reservation.ports)
      assertEquals(expected.runtime.reservation.ignoredPorts, actual.runtime.reservation.ignoredPorts)
    }

    assertEquals(expected.cpu, actual.cpu, 0.0d)
    assertEquals(expected.mem, actual.mem)
    assertEquals(expected.seed, actual.seed)
    assertEquals(expected.jvmOptions, actual.jvmOptions)
    assertEquals(expected.rack, actual.rack)
    assertEquals(expected.dc, actual.dc)
    assertEquals(expected.constraints, actual.constraints)
    assertEquals(expected.seedConstraints, actual.seedConstraints)
    assertEquals(expected.dataFileDirs, actual.dataFileDirs)
    assertEquals(expected.commitLogDir, actual.commitLogDir)
    assertEquals(expected.savedCachesDir, actual.savedCachesDir)
    assertEquals(expected.cassandraDotYaml, actual.cassandraDotYaml)
    assertEquals(expected.addressDotYaml, actual.addressDotYaml)
    assertEquals(expected.cassandraJvmOptions, actual.cassandraJvmOptions)
    assertEquals(expected.modified, actual.modified)

    assertEquals(expected.failover.delay, actual.failover.delay)
    assertEquals(expected.failover.maxDelay, actual.failover.maxDelay)
    assertEquals(expected.failover.maxTries, actual.failover.maxTries)
  }
}

@Ignore
class CassandraStorageTest {

  import CassandraStorageTest._

  private val _cassandraCQLUnit = new CassandraCQLUnit(new FileCQLDataSet("vagrant/cassandra_schema.cql", CassandraKeyspace))

  @Rule
  //  Hack to let JUnit run scala val rule
  def cassandraCQLUnit = _cassandraCQLUnit

  private def reset(): Unit = {
    Nodes.reset()
    Config.namespace = "default_namespace"
    Nodes.namespace = Config.namespace
    Nodes.frameworkId = "framework_id_uuid"
  }

  @Before def before(): Unit = {
    val cassandraStorage = new CassandraStorage(9142, Seq("127.0.0.1"), CassandraKeyspace, CassandraTable)
    Nodes.storage = cassandraStorage
    reset()
  }

  @Test
  def testDefaultCluster(): Unit = {
    Nodes.save()

    val result = cassandraCQLUnit.session.execute(s"select * from $CassandraTable").all().asScala
    assertEquals("Default cluster should have no nodes", 1, result.size)
    assertEquals("Default cluster id is `default`", "default", result.head.getString(CassandraStorage.ClusterId))
  }

  @Test
  def testEmptyCluster(): Unit = {
    val cluster = createCluster("cluster_1", "192.168.*", Map(Node.Port.AGENT -> new Util.Range("5005")), true, "user", "pass")
    Nodes.addCluster(cluster)
    Nodes.save()

    val count = cassandraCQLUnit.session.execute(s"select count(*) from $CassandraTable").one().getLong(0)
    assertEquals("One table per empty cluster", 2, count)

    val query =
      s"""
         |select ${CassandraStorage.NrOfNodes} from $CassandraTable
          |where namespace='${Nodes.namespace}' and framework_id='${Nodes.frameworkId}' and cluster_id='${cluster.id}' and node_id='undefined'
    """.stripMargin
    println(query)
    val cluster1Nodes = cassandraCQLUnit.session.execute(query).one().getInt(0)
    assertEquals("Cluster1 has no nodes", 0, cluster1Nodes)
  }

  @Test
  def testTwoClusters(): Unit = {
    val cluster1 = createCluster("cluster_1", "192.168.*", Map(Node.Port.AGENT -> new Util.Range("5005"), Node.Port.CQL -> null), true, "user", "pass")
    val node1_1 = createNode("1", Node.State.RUNNING, cluster1, createStickiness("30m", "slave0", new Date()), null, 1.5, 2056,
      true, "", null, null, Map.empty, Map.empty, ".", ".", ".", Map.empty, Map.empty, "", false, new Failover())
    Nodes.addCluster(cluster1)
    Nodes.addNode(node1_1)

    val cluster2 = createCluster("cluster_2", "", Map.empty, false, null, null)

    val node1_2 = {
      val reservation = new Reservation(1.0, 256, Map(Node.Port.CQL -> 5001), List(Node.Port.AGENT))
      val runtime = new Node.Runtime("task_1_2", "executor_1_2", "slave0", "hostname.1.server", List("192.168.0.0"), reservation, Map("a1" -> "b1", "a2" -> "b2"))
      val failover = new Failover(new Period("100s"), new Period("50s"), 5)
      createNode("2", Node.State.RECONCILING, cluster2, createStickiness("1h", null, null), runtime, 0.5,
        1024, true, "opt1", "r1", "dc1", Map("hostname" -> List(Constraint("like:master*"))), Map("hostname" -> List(Constraint("like:slave*"))),
        "/tmp/datafile", "/tmp/comitlogdir", "/opt/savedCaches", Map("num_tokens" -> "1"), Map("stomp_interface" -> "10.1.2.3"), "copt1", false,
        failover)
    }
    Nodes.addCluster(cluster2)
    Nodes.addNode(node1_2)

    val node2_2 = {
      val reservation = new Reservation(1.0, 256, Map(Node.Port.CQL -> 5001), List(Node.Port.AGENT))
      val runtime = new Node.Runtime("task_2_2", "executor_2_2", "slave0", "hostname.1.server", List("192.168.0.0"), reservation)
      val failover = new Failover(new Period("100s"), new Period("50s"), 5)
      createNode("3", Node.State.RECONCILING, cluster2, createStickiness("2h", null, null), runtime, 0.6,
        1024, true, "opt2", "r2", "dc2", Map("hostname" -> List(Constraint("like:slave*"))), Map("hostname" -> List(Constraint("like:master*"))),
        "/tmp/datafile2", "/tmp/comitlogdir2", "/opt/savedCaches2", Map("num_tokens" -> "2"), Map("stomp_interface" -> "10.3.2.1"), "copt2", false,
        new Failover(new Period("20m"), new Period("10s"), null))
    }
    Nodes.addNode(node2_2)

    Nodes.save()

    reset()
    assertTrue("reset must empty nodes", Nodes.getNodes.isEmpty)
    Nodes.load()

    assertEquals("storage should load default and two added clusters", 3, Nodes.clusters.size)
    assertEquals(3, Nodes.nodes.size)
    assertEquals(Set(Nodes.defaultCluster, cluster1, cluster2), Nodes.clusters.toSet)
    assertClusterEquals(cluster1, Nodes.getCluster(cluster1.id))
    assertClusterEquals(cluster2, Nodes.getCluster(cluster2.id))

    assertNodeEquals(node1_1, Nodes.getNode(node1_1.id))
    assertNodeEquals(node1_2, Nodes.getNode(node1_2.id))
    assertNodeEquals(node2_2, Nodes.getNode(node2_2.id))
  }
}
