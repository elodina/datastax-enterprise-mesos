package net.elodina.mesos.dse.cli

import net.elodina.mesos.dse.cli.Cli.{out, printLine}
import net.elodina.mesos.dse.cli.Cli.CliError
import java.io.IOException
import net.elodina.mesos.dse.{Util, Node}
import joptsimple.{OptionException, OptionSet, OptionParser}
import scala.collection.mutable

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
      case _ => throw new Error("unsupported ring command " + cmd)
    }
  }

  def handleHelp(args: Array[String]): Unit = {
    val cmd = if (args != null && args.length > 0) args(0) else null

    cmd match {
      case null =>
        printLine("Node management commands\nUsage: node <command>\n")
        printCmds()

        printLine()
        printLine("Run `help node <command>` to see details of specific command")
      case "list" =>
        handleList(help = true)
      case "add" | "update" =>
        handleAddUpdate(cmd, null, args, help = true)
      case "remove" =>
        handleRemove(null, help = true)
      case _ =>
        throw new CliError(s"unsupported node command $cmd")
    }
  }

  def handleList(help: Boolean = false): Unit = {
    if (help) {
      printLine("List nodes\nUsage: node list\n")
      return
    }

    var nodesJson: List[Any] = null
    try { nodesJson = Cli.sendRequest("/node/list", Map()).asInstanceOf[List[Any]] }
    catch { case e: IOException => throw new CliError("" + e) }
    val nodes = nodesJson.map(n => new Node(n.asInstanceOf[Map[String, Any]]))

    val title: String = if (nodes.isEmpty) "no nodes" else "node" + (if (nodes.size > 1) "s" else "") + ":"
    printLine(title)

    for (node <- nodes) {
      printNode(node, 1)
      printLine()
    }
  }

  def handleAddUpdate(cmd: String, expr: String, args: Array[String], help: Boolean = false): Unit = {
    val parser = new OptionParser()

    parser.accepts("cpu", "CPU amount (0.5, 1, 2).").withRequiredArg().ofType(classOf[java.lang.Double])
    parser.accepts("mem", "Mem amount in Mb.").withRequiredArg().ofType(classOf[java.lang.Long])
    parser.accepts("broadcast", "Network interface to broadcast for nodes.").withRequiredArg().ofType(classOf[String])

    parser.accepts("constraints", "Constraints (hostname=like:^master$,rack=like:^1.*$).").withRequiredArg().ofType(classOf[String])
    parser.accepts("seed-constraints", "Seed node constraints. Will be evaluated only across seed nodes.").withRequiredArg().ofType(classOf[String])

    parser.accepts("cluster-name", "The name of the cluster.").withRequiredArg().ofType(classOf[String])
    parser.accepts("seed", "Flags whether this Datastax Node is a seed node.").withRequiredArg().ofType(classOf[java.lang.Boolean])
    parser.accepts("replace-address", "Replace address for the dead Datastax Node").withRequiredArg().ofType(classOf[String])

    parser.accepts("data-file-dirs", "Cassandra data file directories separated by comma. Defaults to sandbox if not set").withRequiredArg().ofType(classOf[String])
    parser.accepts("commit-log-dir", "Cassandra commit log dir. Defaults to sandbox if not set").withRequiredArg().ofType(classOf[String])
    parser.accepts("saved-caches-dir", "Cassandra saved caches dir. Defaults to sandbox if not set").withRequiredArg().ofType(classOf[String])

    if (help) {
      printLine(s"${cmd.capitalize} node(s) \nUsage: $cmd <id> [options]\n")
      parser.printHelpOn(out)

      printLine()
      Cli.handleGenericOptions(args, help = true)
      return
    }

    var options: OptionSet = null
    try { options = parser.parse(args: _*) }
    catch {
      case e: OptionException =>
        parser.printHelpOn(out)
        printLine()
        throw new Cli.CliError(e.getMessage)
    }

    val cpu = options.valueOf("cpu").asInstanceOf[java.lang.Double]
    val mem = options.valueOf("mem").asInstanceOf[java.lang.Long]
    val broadcast = options.valueOf("broadcast").asInstanceOf[String]

    val constraints = options.valueOf("constraints").asInstanceOf[String]
    val seedConstraints = options.valueOf("seed-constraints").asInstanceOf[String]

    val nodeOut = options.valueOf("node-out").asInstanceOf[String]
    val agentOut = options.valueOf("agent-out").asInstanceOf[String]

    val clusterName = options.valueOf("cluster-name").asInstanceOf[String]
    val seed = options.valueOf("seed").asInstanceOf[java.lang.Boolean]
    val replaceAddress = options.valueOf("replace-address").asInstanceOf[String]

    val dataFileDirs = options.valueOf("data-file-dirs").asInstanceOf[String]
    val commitLogDir = options.valueOf("commit-log-dir").asInstanceOf[String]
    val savedCachesDir = options.valueOf("saved-caches-dir").asInstanceOf[String]

    val params = new mutable.HashMap[String, String]()
    params("node") = expr
    if (cpu != null) params("cpu") = "" + cpu
    if (mem != null) params("mem") = "" + mem
    if (broadcast != null) params("broadcast") = broadcast

    if (constraints != null) params("constraints") = constraints
    if (seedConstraints != null) params("seedConstraints") = seedConstraints

    if (nodeOut != null) params("nodeOut") = nodeOut
    if (agentOut != null) params("agentOut") = agentOut

    if (clusterName != null) params("clusterName") = clusterName
    if (seed != null) params("seed") = "" + seed
    if (replaceAddress != null) params("replaceAddress") = replaceAddress

    if (dataFileDirs != null) params("dataFileDirs") = dataFileDirs
    if (commitLogDir != null) params("commitLogDir") = commitLogDir
    if (savedCachesDir != null) params("savedCachesDir") = savedCachesDir

    var nodesJson: List[Any] = null
    try { nodesJson = Cli.sendRequest(s"/node/$cmd", params.toMap).asInstanceOf[List[Any]] }
    catch { case e: IOException => throw new CliError("" + e) }
    val nodes = nodesJson.map(n => new Node(n.asInstanceOf[Map[String, Any]]))


    val addedUpdated = if (cmd == "add") "added" else "updated"
    printLine(s"nodes $addedUpdated:")

    for (node <- nodes) {
      printNode(node, 1)
      printLine()
    }
  }

  def handleRemove(expr: String, help: Boolean = false): Unit = {
    if (help) {
      printLine("Remove node(s) \nUsage: remove <id>\n")
      printLine()
      Cli.handleGenericOptions(null, help = true)
      return
    }

    try { Cli.sendRequest(s"/node/remove", Map("node" -> expr)) }
    catch { case e: IOException => throw new CliError("" + e) }

    println("nodes removed")
  }

  def printCmds(): Unit = {
    printLine("Commands:")
    printLine("list       - list nodes", 1)
    printLine("add        - add node", 1)
    printLine("update     - update node", 1)
    printLine("remove     - remove node", 1)
    printLine("start      - start node", 1)
    printLine("stop       - stop node", 1)
  }

  private def printNode(node: Node, indent: Int = 0) {
    printLine("node:", indent)
    printLine(s"id: ${node.id}", indent + 1)
    printLine(s"state: ${node.state}", indent + 1)
    printLine(s"cpu: ${node.cpu}", indent + 1)
    printLine(s"mem: ${node.mem}", indent + 1)

    if (node.broadcast != null) printLine(s"broadcast: ${node.broadcast}", indent + 1)
    if (node.clusterName != null) printLine(s"cluster name: ${node.clusterName}", indent + 1)
    printLine(s"seed: ${node.seed}", indent + 1)

    if (node.seeds != "") printLine(s"seeds: ${node.seeds}", indent + 1)
    if (node.replaceAddress != null) printLine(s"replace-address: ${node.replaceAddress}", indent + 1)
    if (node.constraints.nonEmpty) printLine(s"constraints: ${Util.formatConstraints(node.constraints)}", indent + 1)
    if (node.seed && node.seedConstraints.nonEmpty) printLine(s"seed constraints: ${Util.formatConstraints(node.seedConstraints)}", indent + 1)

    if (node.dataFileDirs != null) printLine(s"data file dirs: ${node.dataFileDirs}", indent + 1)
    if (node.commitLogDir != null) printLine(s"commit log dir: ${node.commitLogDir}", indent + 1)
    if (node.savedCachesDir != null) printLine(s"saved caches dir: ${node.savedCachesDir}", indent + 1)

    if (node.runtime != null) printNodeRuntime(node.runtime, indent + 1)
  }

  private def printNodeRuntime(runtime: Node.Runtime, indent: Int = 0) {
    printLine(s"runtime:", indent)
    printLine(s"task id: ${runtime.taskId}", indent + 1)
    printLine(s"slave id: ${runtime.slaveId}", indent + 1)
    printLine(s"executor id: ${runtime.executorId}", indent + 1)
    printLine(s"hostname: ${runtime.hostname}", indent + 1)
    printLine(s"attributes: ${Util.formatMap(runtime.attributes)}", indent + 1)
  }
}
