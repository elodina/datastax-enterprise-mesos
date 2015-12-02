package net.elodina.mesos.dse.cli

import net.elodina.mesos.dse.cli.Cli.CliError
import net.elodina.mesos.dse.cli.Cli.printLine
import java.io.IOException
import net.elodina.mesos.dse.{Ring, Cluster}

object RingCli {
  def handle(cmd: String, _args: Array[String], help: Boolean = false): Unit = {
    var args = _args

    if (help) {
      handleHelp(cmd)
      return
    }

    var arg: String = null
    if (args.length > 0 && !args(0).startsWith("-")) {
      arg = args(0)
      args = args.slice(1, args.length)
    }

    if (arg == null && cmd != "list") {
      handleHelp(cmd); printLine()
      throw new Error("argument required")
    }

    cmd match {
      case "list" => handleList()
      case _ => throw new Error("unsupported ring command " + cmd)
    }
  }

  def handleHelp(cmd: String): Unit = {
    cmd match {
      case null =>
        printLine("Ring management commands\nUsage: ring <command>\n")
        printCmds()

        printLine()
        printLine("Run `help ring <command>` to see details of specific command")
      case "list" =>
        handleList(help = true)
      case _ =>
        throw new CliError(s"unsupported ring command $cmd")
    }
  }

  def handleList(help: Boolean = false): Unit = {
    if (help) {
      printLine("List rings\nUsage: ring list\n")
      return
    }

    var json: Map[String, Any] = null
    try { json = Cli.sendRequest("/ring/list", Map()) }
    catch { case e: IOException => throw new CliError("" + e) }
    val cluster: Cluster = new Cluster(json)

    val title: String = if (cluster.getRings.isEmpty) "no rings" else "ring" + (if (cluster.getRings.size > 1) "s" else "") + ":"
    printLine(title)

    for (ring <- cluster.getRings) {
      printRing(ring, 1)
      printLine()
    }
  }

  def printCmds(): Unit = {
    printLine("Commands:")
    printLine("list       - list rings", 1)
    printLine("add        - add ring", 1)
    printLine("update     - update ring", 1)
    printLine("remove     - remove ring", 1)
  }

  private def printRing(ring: Ring, indent: Int): Unit = {
    printLine("id: " + ring.id, indent)
  }
}
