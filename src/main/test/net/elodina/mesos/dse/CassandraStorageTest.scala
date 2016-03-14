package net.elodina.mesos.dse

import java.util.Date

import net.elodina.mesos.dse.Node.{Reservation, Failover, Stickiness}
import net.elodina.mesos.dse.Util.{BindAddress, Period}
import org.cassandraunit.CassandraCQLUnit
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.junit.Assert._
import org.junit.{Before, Rule, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable

object CassandraStorageTest {
  //these should match cql dataset file
  private val CassandraKeyspace = "dse_mesos"
  private val CassandraTable = "dse_mesos_framework"

  private def createCluster(id: String, bindAddress: String, ports: Map[Node.Port.Value, Util.Range],
                            jmxRemote: Boolean, jmxUser: String, jmxPassword: String): Cluster = {
    val cluster = new Cluster(id)
    cluster.bindAddress = new BindAddress(bindAddress)
    cluster.ports = new mutable.HashMap[Node.Port.Value, Util.Range]() ++= ports
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

    node.constraints = new mutable.HashMap[String, List[Constraint]] ++= constraints
    node.seedConstraints = new mutable.HashMap[String, List[Constraint]] ++= seedConstraints

    node.dataFileDirs = dataFileDirs
    node.commitLogDir = commitLogDir
    node.savedCachesDir = savedCachesDir

    node.cassandraDotYaml = new mutable.HashMap[String, String] ++= cassandraDotYaml
    node.addressDotYaml = new mutable.HashMap[String, String] ++= addressDotYaml
    node.cassandraJvmOptions = cassandraJvmOptions

    node.modified = modified

    node.failover = failover

    node
  }

  private def clusterEquals(cluster: Cluster, other: Cluster): Boolean = {
    cluster.id == other.id &&
    cluster.bindAddress.values == other.bindAddress.values &&
    cluster.ports == other.ports &&
    cluster.jmxRemote == other.jmxRemote &&
    cluster.jmxUser == other.jmxUser &&
    cluster.jmxPassword == other.jmxPassword
  }

  private def nodeEquals(node:Node, other:Node): Boolean = {
    node.id == other.id &&
    node.state == other.state &&
    node.cluster.id == other.cluster.id &&
    node.stickiness == other.stickiness &&
    node.runtime == other.runtime &&
    node.cpu == other.cpu &&
    node.mem == other.mem &&
    node.seed == other.seed &&
    node.jvmOptions == other.jvmOptions &&
    node.rack == other.rack &&
    node.dc == other.dc &&
    node.constraints == other.constraints &&
    node.seedConstraints == other.seedConstraints &&
    node.dataFileDirs == other.dataFileDirs &&
    node.commitLogDir == other.commitLogDir &
    node.savedCachesDir == other.savedCachesDir &&
    node.cassandraDotYaml == other.cassandraDotYaml &&
    node.addressDotYaml == other.addressDotYaml &&
    node.cassandraJvmOptions == other.cassandraJvmOptions &&
    node.modified == other.modified &&
    node.failover == other.failover
  }
}

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
      val runtime = new Node.Runtime("task_1_2", "executor_1_2", "slave0", "hostname.1.server", List("192.168.0.0"), reservation)
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
    assertEquals(Set(cluster1, cluster2), Nodes.clusters.toSet)
  }

}
