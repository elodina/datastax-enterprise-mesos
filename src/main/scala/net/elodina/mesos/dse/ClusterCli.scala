package net.elodina.mesos.dse

import java.io.IOException
import joptsimple.{OptionException, OptionSet, OptionParser}
import scala.collection.mutable
import Cli.{out, printLine, handleGenericOptions, Error}

object ClusterCli {
  def handle(_args: Array[String], help: Boolean = false): Unit = {
    var args = _args

    if (help) {
      handleHelp(args)
      return
    }

    if (args.length == 0)
      throw new Error("command required")

    val cmd: String = args(0)
    args = args.slice(1, args.length)

    var arg: String = null
    if (args.length > 0 && !args(0).startsWith("-")) {
      arg = args(0)
      args = args.slice(1, args.length)
    }

    if (arg == null && cmd != "list")
      throw new Error("argument required")

    cmd match {
      case "list" => handleList()
      case "add" | "update" => handleAddUpdate(cmd, arg, args)
      case "remove" => handleRemove(arg)
      case _ => throw new Error("unsupported cluster command " + cmd)
    }
  }

  def handleHelp(args: Array[String]): Unit = {
    val cmd = if (args != null && args.length > 0) args(0) else null

    cmd match {
      case null =>
        printLine("Cluster management commands\nUsage: cluster <cmd>\n")
        printCmds()

        printLine()
        printLine("Run `help cluster <cmd>` to see details of specific command")
      case "list" => handleList(help = true)
      case "add" | "update" => handleAddUpdate(cmd, null, null, help = true)
      case "remove" => handleRemove(null, help = true)
      case _ => throw new Error(s"unsupported cluster command $cmd")
    }
  }

  def handleList(help: Boolean = false): Unit = {
    if (help) {
      printLine("List clusters\nUsage: cluster list\n")
      handleGenericOptions(null, help = true)
      return
    }

    var json: List[Any] = null
    try { json = Cli.sendRequest("/cluster/list", Map()).asInstanceOf[List[Any]] }
    catch { case e: IOException => throw new Error("" + e) }
    val clusters = json.map(j => new Cluster(j.asInstanceOf[Map[String, Any]]))

    val title: String = if (clusters.isEmpty) "no clusters" else "cluster" + (if (clusters.size > 1) "s" else "") + ":"
    printLine(title)

    for (cluster <- clusters) {
      printCluster(cluster, 1)
      printLine()
    }
  }

  def handleAddUpdate(cmd: String, id: String, args: Array[String], help: Boolean = false): Unit = {
    val parser = new OptionParser()
    parser.accepts("bind-address", "Bind address mask (192.168.50.*, if:eth1). Default - auto.").withRequiredArg().ofType(classOf[String])

    parser.accepts("jmx-remote", "Remote JMX connections enabled.").withRequiredArg().ofType(classOf[java.lang.Boolean])
    parser.accepts("jmx-user", "JMX user. Default - none.").withRequiredArg().ofType(classOf[String])
    parser.accepts("jmx-password", "JMX password. Default - none.").withRequiredArg().ofType(classOf[String])

    parser.accepts("ip-per-container-enabled", "All nodes will be started within mesos container and have IP assigned.").withRequiredArg().ofType(classOf[java.lang.Boolean])

    parser.accepts("storage-port", "Inter-node port.").withRequiredArg().ofType(classOf[String])
    parser.accepts("jmx-port", "JMX monitoring port.").withRequiredArg().ofType(classOf[String])
    parser.accepts("cql-port", "CQL port.").withRequiredArg().ofType(classOf[String])
    parser.accepts("thrift-port", "Thrift port.").withRequiredArg().ofType(classOf[String])
    parser.accepts("agent-port", "DataStax agent port.").withRequiredArg().ofType(classOf[String])

    if (help) {
      printLine(s"${cmd.capitalize} cluster \nUsage: cluster $cmd <id> [options]\n")
      parser.printHelpOn(out)

      printLine()
      handleGenericOptions(args, help = true)
      return
    }

    var options: OptionSet = null
    try { options = parser.parse(args: _*) }
    catch {
      case e: OptionException =>
        parser.printHelpOn(out)
        printLine()
        throw new Cli.Error(e.getMessage)
    }

    val bindAddress = options.valueOf("bind-address").asInstanceOf[String]

    val jmxRemote = options.valueOf("jmx-remote").asInstanceOf[java.lang.Boolean]
    val jmxUser = options.valueOf("jmx-user").asInstanceOf[String]
    val jmxPassword = options.valueOf("jmx-password").asInstanceOf[String]

    val ipPerContainerEnabled = options.valueOf("ip-per-container-enabled").asInstanceOf[java.lang.Boolean]

    val storagePort = options.valueOf("storage-port").asInstanceOf[String]
    val jmxPort = options.valueOf("jmx-port").asInstanceOf[String]
    val cqlPort = options.valueOf("cql-port").asInstanceOf[String]
    val thriftPort = options.valueOf("thrift-port").asInstanceOf[String]
    val agentPort = options.valueOf("agent-port").asInstanceOf[String]

    val params = mutable.HashMap("cluster" -> id)

    if (bindAddress != null) params("bindAddress") = bindAddress

    if (jmxRemote != null) params("jmxRemote") = "" + jmxRemote
    if (jmxUser != null) params("jmxUser") = jmxUser
    if (jmxPassword != null) params("jmxPassword") = jmxPassword

    if (ipPerContainerEnabled != null) params("ipPerContainerEnabled") = "" + ipPerContainerEnabled

    if (storagePort != null) params("storagePort") = storagePort
    if (jmxPort != null) params("jmxPort") = jmxPort
    if (cqlPort != null) params("cqlPort") = cqlPort
    if (thriftPort != null) params("thriftPort") = thriftPort
    if (agentPort != null) params("agentPort") = agentPort

    var json: Map[String, Any] = null
    try { json = Cli.sendRequest(s"/cluster/$cmd", params.toMap).asInstanceOf[Map[String, Any]] }
    catch { case e: IOException => throw new Error("" + e) }
    val cluster: Cluster = new Cluster(json)

    var title = "cluster"
    title += " " + (if (cmd == "add") "added" else "updated") + ":"
    printLine(title)

    printCluster(cluster, 1)
    printLine()
  }

  def handleRemove(id: String, help: Boolean = false): Unit = {
    if (help) {
      printLine(s"Remove cluster \nUsage: cluster remove <id>\n")
      handleGenericOptions(null, help = true)
      return
    }

    try { Cli.sendRequest(s"/cluster/remove", Map("cluster" -> id)) }
    catch { case e: IOException => throw new Error("" + e) }

    printLine("cluster removed")
  }

  def printCmds(): Unit = {
    printLine("Commands:")
    printLine("list       - list clusters", 1)
    printLine("add        - add cluster", 1)
    printLine("update     - update cluster", 1)
    printLine("remove     - remove cluster", 1)
  }

  private[dse] def printCluster(cluster: Cluster, indent: Int): Unit = {
    printLine("id: " + cluster.id, indent)
    printLine("bind-address: " + (if (cluster.bindAddress != null) cluster.bindAddress else "<auto>"), indent)
    printLine(s"jmx: ${jmxSettings(cluster)}", indent)
    printLine(s"ip per container: ${cluster.ipPerContainerEnabled}", indent)
    printLine(s"ports: ${clusterPorts(cluster)}", indent)
  }

  private def clusterPorts(cluster: Cluster): String = {
    var s = ""

    for (port <- Node.Port.values) {
      if (!s.isEmpty) s += ", "
      val range = cluster.ports(port)
      s += port + ":" + (if (range != null) range else "<auto>")
    }

    s
  }

  private def jmxSettings(cluster: Cluster): String = {
    var s = ""

    s += s"remote:${cluster.jmxRemote}"
    s += s", user:" + (if (cluster.jmxUser != null) cluster.jmxUser else "<none>")
    s += s", password:" + (if (cluster.jmxPassword != null) "***" else "<none>")

    s
  }
}
