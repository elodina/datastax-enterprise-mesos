package net.elodina.mesos.dse.cli

import net.elodina.mesos.dse.cli.Cli._
import net.elodina.mesos.dse.cli.Cli.CliError
import java.io.IOException
import net.elodina.mesos.dse.{Util, Node}

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

    if (node.broadcast != "") printLine(s"broadcast: ${node.broadcast}", indent + 1)
    printLine(s"node out: ${node.nodeOut}", indent + 1)
    printLine(s"agent out: ${node.agentOut}", indent + 1)
    printLine(s"cluster name: ${node.clusterName}", indent + 1)
    printLine(s"seed: ${node.seed}", indent + 1)

    if (node.seeds != "") printLine(s"seeds: ${node.seeds}", indent + 1)
    if (node.replaceAddress != "") printLine(s"replace-address: ${node.replaceAddress}", indent + 1)
    if (node.constraints.nonEmpty) printLine(s"constraints: ${Util.formatConstraints(node.constraints)}", indent + 1)
    if (node.seed && node.seedConstraints.nonEmpty) printLine(s"seed constraints: ${Util.formatConstraints(node.seedConstraints)}", indent + 1)
    if (node.dataFileDirs != "") printLine(s"data file dirs: ${node.dataFileDirs}", indent + 1)
    if (node.commitLogDir != "") printLine(s"commit log dir: ${node.commitLogDir}", indent + 1)
    if (node.savedCachesDir != "") printLine(s"saved caches dir: ${node.savedCachesDir}", indent + 1)
    if (node.runtime != null) printNodeRuntime(node.runtime, indent + 1)

    printLine()
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
