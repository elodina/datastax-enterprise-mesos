package net.elodina.mesos.dse.cli

import net.elodina.mesos.dse.cli.Cli._
import java.io.IOException
import net.elodina.mesos.dse.Ring
import joptsimple.{OptionException, OptionSet, OptionParser}
import net.elodina.mesos.dse.cli.Cli.Error
import scala.collection.mutable

object RingCli {
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
        printLine("Ring management commands\nUsage: ring <command>\n")
        printCmds()

        printLine()
        printLine("Run `help ring <command>` to see details of specific command")
      case "list" => handleList(help = true)
      case "add" | "update" => handleAddUpdate(cmd, null, null, help = true)
      case "remove" => handleRemove(null, help = true)
      case _ => throw new Error(s"unsupported ring command $cmd")
    }
  }

  def handleList(help: Boolean = false): Unit = {
    if (help) {
      printLine("List rings\nUsage: ring list\n")
      return
    }

    var json: List[Any] = null
    try { json = Cli.sendRequest("/ring/list", Map()).asInstanceOf[List[Any]] }
    catch { case e: IOException => throw new Error("" + e) }
    val rings = json.map(j => new Ring(j.asInstanceOf[Map[String, Any]]))

    val title: String = if (rings.isEmpty) "no rings" else "ring" + (if (rings.size > 1) "s" else "") + ":"
    printLine(title)

    for (ring <- rings) {
      printRing(ring, 1)
      printLine()
    }
  }

  def handleAddUpdate(cmd: String, id: String, args: Array[String], help: Boolean = false): Unit = {
    val parser = new OptionParser()
    parser.accepts("name", "Ring name.").withRequiredArg().ofType(classOf[String])

    if (help) {
      printLine(s"${cmd.capitalize} ring \nUsage: $cmd <id> [options]\n")
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
        throw new Cli.Error(e.getMessage)
    }

    val name = options.valueOf("name").asInstanceOf[String]

    val params = mutable.HashMap("ring" -> id)
    if (name != null) params("name") = name

    var json: Map[String, Any] = null
    try { json = Cli.sendRequest(s"/ring/$cmd", params.toMap).asInstanceOf[Map[String, Any]] }
    catch { case e: IOException => throw new Error("" + e) }
    val ring: Ring = new Ring(json)

    var title = "ring"
    title += " " + (if (cmd == "add") "added" else "updated") + ":"
    printLine(title)

    printRing(ring, 1)
    printLine()
  }

  def handleRemove(id: String, help: Boolean = false): Unit = {
    if (help) {
      printLine(s"Remove ring \nUsage: remove <id>\n")
      printLine()
      Cli.handleGenericOptions(null, help = true)
      return
    }

    try { Cli.sendRequest(s"/ring/remove", Map("ring" -> id)) }
    catch { case e: IOException => throw new Error("" + e) }

    println("ring removed")
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
    if (ring.name != null) printLine("name: " + ring.name, indent)
  }
}
