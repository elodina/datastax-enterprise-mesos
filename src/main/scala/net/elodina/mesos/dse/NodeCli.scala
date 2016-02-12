package net.elodina.mesos.dse

import java.io.IOException
import joptsimple.{OptionException, OptionSet, OptionParser}
import scala.collection.mutable
import Cli.{out, printLine, handleGenericOptions, Error}

object NodeCli {
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
      case "start" | "stop" => handleStartStop(cmd, arg, args)
      case "restart" => handleRestart(arg, args)
      case _ => throw new Error("unsupported node command " + cmd)
    }
  }

  def handleHelp(args: Array[String]): Unit = {
    val cmd = if (args != null && args.length > 0) args(0) else null

    cmd match {
      case null =>
        printLine("Node management commands\nUsage: node <cmd>\n")
        printCmds()

        printLine()
        printLine("Run `help node <cmd>` to see details of specific command")
      case "list" =>
        handleList(help = true)
      case "add" | "update" =>
        handleAddUpdate(cmd, null, args, help = true)
      case "remove" =>
        handleRemove(null, help = true)
      case "start" | "stop" =>
        handleStartStop(cmd, null, null, help = true)
      case "restart" =>
        handleRestart(null, null, help = true)
      case _ =>
        throw new Error(s"unsupported node command $cmd")
    }
  }

  def handleList(help: Boolean = false): Unit = {
    if (help) {
      printLine("List nodes\nUsage: node list\n")
      handleGenericOptions(null, help = true)
      return
    }

    var nodesJson: List[Any] = null
    try { nodesJson = Cli.sendRequest("/node/list", Map()).asInstanceOf[List[Any]] }
    catch { case e: IOException => throw new Error("" + e) }
    val nodes = nodesJson.map(n => new Node(n.asInstanceOf[Map[String, Any]], expanded = true))

    val title: String = if (nodes.isEmpty) "no nodes" else "node" + (if (nodes.size > 1) "s" else "") + ":"
    printLine(title)

    for (node <- nodes) {
      printNode(node, 1)
      printLine()
    }
  }

  def handleAddUpdate(cmd: String, expr: String, args: Array[String], help: Boolean = false): Unit = {
    val parser = new OptionParser()

    parser.accepts("cluster", "Cluster to which node belongs to.").withRequiredArg().ofType(classOf[String])

    parser.accepts("cpu", "CPU amount (0.5, 1, 2).").withRequiredArg().ofType(classOf[java.lang.Double])
    parser.accepts("mem", "Mem amount in Mb.").withRequiredArg().ofType(classOf[java.lang.Long])
    parser.accepts("stickiness-period", "Stickiness period to preserve the same slave node (5m, 10m, 1h)").withRequiredArg().ofType(classOf[String])

    parser.accepts("rack", "Node rack.").withRequiredArg().ofType(classOf[String])
    parser.accepts("dc", "Node dc.").withRequiredArg().ofType(classOf[String])

    parser.accepts("constraints", "Constraints (hostname=like:^master$,rack=like:^1.*$).").withRequiredArg().ofType(classOf[String])
    parser.accepts("seed-constraints", "Seed node constraints. Will be evaluated only across seed nodes.").withRequiredArg().ofType(classOf[String])

    parser.accepts("seed", "Flags whether this node is a seed node.").withRequiredArg().ofType(classOf[java.lang.Boolean])
    parser.accepts("jvm-options", "JVM options for node executor.").withRequiredArg().ofType(classOf[String])

    parser.accepts("data-file-dirs", "Cassandra data file directories separated by comma. Defaults to sandbox if not set.").withRequiredArg().ofType(classOf[String])
    parser.accepts("commit-log-dir", "Cassandra commit log dir. Defaults to sandbox if not set.").withRequiredArg().ofType(classOf[String])
    parser.accepts("saved-caches-dir", "Cassandra saved caches dir. Defaults to sandbox if not set.").withRequiredArg().ofType(classOf[String])
    parser.accepts("cassandra-yaml-configs", "Comma separated key-value pairs (k1=v1,k2=v2) that override default cassandra.yaml configuration. " +
      "Default configuration file is shipped with the dse tarball. Note: These pairs are not validated by the Scheduler.").withRequiredArg.ofType(classOf[String])
    parser.accepts("address-yaml-configs", "Comma separated key-value pairs (k1=v1,k2=v2) that add or override default address.yaml configuration. " +
      "Default configuration file is created by the Executor and points to local node. The bare minimum required for OpsCenter support is the hostname of your OpsCenter. " +
      "E.g. \"stomp_interface=10.1.2.3\" . Note: These pairs are not validated by the Scheduler.").withRequiredArg.ofType(classOf[String])
    parser.accepts("cassandra-jvm-options", "A string to set JVM_OPTS environment variable. " +
      "E.g. \"-Dcassandra.replace_address=127.0.0.1 -Dcassandra.ring_delay_ms=15000\".").withRequiredArg.ofType(classOf[String])

    if (help) {
      printLine(s"${cmd.capitalize} node \nUsage: node $cmd <id> [options]\n")
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

    val cluster = options.valueOf("cluster").asInstanceOf[String]

    val cpu = options.valueOf("cpu").asInstanceOf[java.lang.Double]
    val mem = options.valueOf("mem").asInstanceOf[java.lang.Long]
    val stickinessPeriod = options.valueOf("stickiness-period").asInstanceOf[String]

    val rack = options.valueOf("rack").asInstanceOf[String]
    val dc = options.valueOf("dc").asInstanceOf[String]

    val constraints = options.valueOf("constraints").asInstanceOf[String]
    val seedConstraints = options.valueOf("seed-constraints").asInstanceOf[String]

    val seed = options.valueOf("seed").asInstanceOf[java.lang.Boolean]
    val jvmOptions = options.valueOf("jvm-options").asInstanceOf[String]

    val dataFileDirs = options.valueOf("data-file-dirs").asInstanceOf[String]
    val commitLogDir = options.valueOf("commit-log-dir").asInstanceOf[String]
    val savedCachesDir = options.valueOf("saved-caches-dir").asInstanceOf[String]
    val cassandraDotYaml = options.valueOf("cassandra-yaml-configs").asInstanceOf[String]
    val addressDotYaml = options.valueOf("address-yaml-configs").asInstanceOf[String]
    val cassandraJvmOptions = options.valueOf("cassandra-jvm-options").asInstanceOf[String]

    val params = new mutable.HashMap[String, String]()
    params("node") = expr
    if (cluster != null) params("cluster") = cluster

    if (cpu != null) params("cpu") = "" + cpu
    if (mem != null) params("mem") = "" + mem
    if (stickinessPeriod != null) params("stickinessPeriod") = stickinessPeriod

    if (rack != null) params("rack") = rack
    if (dc != null) params("dc") = dc

    if (constraints != null) params("constraints") = constraints
    if (seedConstraints != null) params("seedConstraints") = seedConstraints

    if (seed != null) params("seed") = "" + seed
    if (jvmOptions != null) params("jvmOptions") = jvmOptions

    if (dataFileDirs != null) params("dataFileDirs") = dataFileDirs
    if (commitLogDir != null) params("commitLogDir") = commitLogDir
    if (savedCachesDir != null) params("savedCachesDir") = savedCachesDir
    if (cassandraDotYaml != null) params("cassandraDotYaml") = cassandraDotYaml
    if (addressDotYaml != null) params("addressDotYaml") = addressDotYaml
    if (cassandraJvmOptions != null) params("cassandraJvmOptions") = cassandraJvmOptions

    var nodesJson: List[Any] = null
    try { nodesJson = Cli.sendRequest(s"/node/$cmd", params.toMap).asInstanceOf[List[Any]] }
    catch { case e: IOException => throw new Error("" + e) }
    val nodes = nodesJson.map(n => new Node(n.asInstanceOf[Map[String, Any]], expanded = true))

    var title = "node" + (if (nodes.length > 1) "s" else "")
    title += " " + (if (cmd == "add") "added" else "updated") + ":"
    printLine(title)

    for (node <- nodes) {
      printNode(node, 1)
      printLine()
    }
  }

  def handleRemove(expr: String, help: Boolean = false): Unit = {
    if (help) {
      printLine("Remove node \nUsage: node remove <id>\n")
      handleGenericOptions(null, help = true)
      return
    }

    try { Cli.sendRequest(s"/node/remove", Map("node" -> expr)) }
    catch { case e: IOException => throw new Error("" + e) }

    printLine("node removed")
  }

  def handleStartStop(cmd: String, expr: String, args: Array[String], help: Boolean = false): Unit = {
    val parser = new OptionParser()
    parser.accepts("timeout", "Time to wait until node starts. Should be a parsable Scala Duration value. Defaults to 2m.").withRequiredArg().ofType(classOf[String])
    if (cmd == "stop") parser.accepts("force", "forcibly stop").withOptionalArg().ofType(classOf[String])

    if (help) {
      printLine(s"${cmd.capitalize} node \nUsage: node $cmd <id> [options]\n")
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

    val timeout = options.valueOf("timeout").asInstanceOf[String]
    val force = options.has("force")

    val params = new mutable.HashMap[String, String]()
    params("node") = expr
    if (timeout != null) params("timeout") = timeout
    if (force) params("force") = null

    var json: Map[String, Any] = null
    try { json = Cli.sendRequest(s"/node/$cmd", params.toMap).asInstanceOf[Map[String, Any]] }
    catch { case e: IOException => throw new Error("" + e) }

    val status = json("status")
    val nodes = json("nodes").asInstanceOf[List[Any]]
      .map(n => new Node(n.asInstanceOf[Map[String, Any]], expanded = true))

    var title: String = if (nodes.length > 1) "nodes " else "node "
    status match {
      case "started" | "stopped" => title += s"$status:"
      case "scheduled" => title += s"$status to $cmd:"
      case "timeout" => throw new Error(s"$cmd timeout")
      case "disconnected" => throw new Error("scheduler disconnected from the master")
    }

    printLine(title)
    for (node <- nodes) {
      printNode(node, 1)
      printLine()
    }
  }

  def handleRestart(expr: String, args: Array[String], help: Boolean = false): Unit = {
    val parser = new OptionParser()
    parser.accepts("timeout", "Time to wait until node restarts. Should be a parsable Scala Duration value. Defaults to 5m.").withRequiredArg().ofType(classOf[String])

    if (help) {
      printLine(s"Restart node \nUsage: node restart <id> [options]\n")
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

    val timeout = options.valueOf("timeout").asInstanceOf[String]
    val params = new mutable.HashMap[String, String]()
    params("node") = expr
    if (timeout != null) params("timeout") = timeout

    var json: Map[String, Any] = null
    try { json = Cli.sendRequest(s"/node/restart", params.toMap).asInstanceOf[Map[String, Any]] }
    catch { case e: IOException => throw new Error("" + e) }

    val status = json("status")

    if (status == "timeout" || status == "disconnected") throw new Error(json("message").asInstanceOf[String])

    val nodes = json("nodes").asInstanceOf[List[Any]]
      .map(n => new Node(n.asInstanceOf[Map[String, Any]], expanded = true))

    var title: String = if (nodes.length > 1) "nodes " else "node "
    title += status + ":"

    printLine(title)
    for (node <- nodes) {
      printNode(node, 1)
      printLine()
    }
  }

  def printCmds(): Unit = {
    printLine("Commands:")
    printLine("list       - list nodes", 1)
    printLine("add        - add node", 1)
    printLine("update     - update node", 1)
    printLine("remove     - remove node", 1)
    printLine("start      - start node", 1)
    printLine("stop       - stop node", 1)
    printLine("restart    - restart node", 1)
  }

  private[dse] def printNode(node: Node, indent: Int = 0) {
    printLine(s"id: ${node.id}", indent)
    printLine(s"state: ${node.state}${if (node.modified) " (modified, needs restart)" else ""}", indent)

    printLine(s"topology: ${nodeTopology(node)}", indent)
    printLine(s"resources: ${nodeResources(node)}", indent)
    printLine(s"seed: ${node.seed}", indent)

    if (node.jvmOptions != null) printLine(s"jvm-options: ${node.jvmOptions}", indent)

    if (node.constraints.nonEmpty) printLine(s"constraints: ${Util.formatConstraints(node.constraints)}", indent)
    if (node.seed && node.seedConstraints.nonEmpty) printLine(s"seed constraints: ${Util.formatConstraints(node.seedConstraints)}", indent)

    if (node.dataFileDirs != null) printLine(s"data file dirs: ${node.dataFileDirs}", indent)
    if (node.commitLogDir != null) printLine(s"commit log dir: ${node.commitLogDir}", indent)
    if (node.savedCachesDir != null) printLine(s"saved caches dir: ${node.savedCachesDir}", indent)
    if (!node.cassandraDotYaml.isEmpty) printLine(s"cassandra.yaml overrides: ${Util.formatMap(node.cassandraDotYaml)}", indent)
    if (!node.addressDotYaml.isEmpty) printLine(s"address.yaml overrides: ${Util.formatMap(node.addressDotYaml)}", indent)
    if (node.cassandraJvmOptions != null) printLine(s"cassandra jvm options: ${node.cassandraJvmOptions}", indent)

    printLine(s"stickiness: ${nodeStickiness(node)}", indent)
    if (node.runtime != null) printNodeRuntime(node.runtime, indent)
  }

  private def printNodeRuntime(runtime: Node.Runtime, indent: Int = 0) {
    printLine(s"runtime:", indent)
    printLine(s"task id: ${runtime.taskId}", indent + 1)
    printLine(s"executor id: ${runtime.executorId}", indent + 1)
    printLine(s"slave id: ${runtime.slaveId}", indent + 1)
    printLine(s"host: ${nodeHost(runtime)}", indent + 1)
    printLine(s"reservation: ${nodeReservation(runtime.reservation)}", indent + 1)
    printLine(s"seeds: ${if (runtime.seeds.isEmpty) "<none>" else runtime.seeds.mkString(",")}", indent + 1)
    if (!runtime.attributes.isEmpty) printLine(s"attributes: ${Util.formatMap(runtime.attributes)}", indent + 1)
  }

  private def nodeTopology(node: Node): String = {
    var s = ""
    s += s"cluster:${node.cluster.id}"
    s += s", dc:${node.dc}"
    s += s", rack:${node.rack}"
    s
  }

  private def nodeResources(node: Node): String = {
    var s = ""
    s += s"cpu:${node.cpu}"
    s += s", mem:${node.mem}"
    s
  }

  private def nodeStickiness(node: Node): String = {
    var s = "period:" + node.stickiness.period
    if (node.stickiness.hostname != null) s += ", hostname:" + node.stickiness.hostname
    if (node.stickiness.stopTime != null) s += ", expires:" + Util.Str.dateTime(node.stickiness.expires)
    s
  }

  private def nodeHost(runtime: Node.Runtime): String = {
    var s = "name:" + runtime.hostname
    s += ", address:" + (if (runtime.address != null) runtime.address else "<pending>")
    s
  }

  private def nodeReservation(reservation: Node.Reservation): String = {
    var s = ""
    s += s"cpu:${reservation.cpus}"
    s += s", mem:${reservation.mem}"

    for (port <- Node.Port.values)
      s += s", $port-port:${reservation.ports(port)}"

    s
  }
}
