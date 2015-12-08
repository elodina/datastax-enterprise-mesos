package net.elodina.mesos.dse.cli

import java.io.{IOException, PrintStream}
import java.net.{URLEncoder, HttpURLConnection, URL}

import net.elodina.mesos.dse._
import scala.io.Source
import joptsimple.{OptionException, OptionSet}

object Cli {
  private[cli] var out: PrintStream = System.out

  def main(args: Array[String]) {
    try { exec(args) }
    catch {
      case e: Error =>
        System.err.println(s"Error: ${e.message}")
        System.exit(1)
    }
  }

  def exec(_args: Array[String]) {
    var args: Array[String] = _args
    if (args.length == 0) throw new Error("command required")

    val cmd = args(0)
    args = args.slice(1, args.length)
    args = handleGenericOptions(args)

    cmd match {
      case "help" => handleHelp(args)
      case "scheduler" => SchedulerCli.handle(args)
      case "node" => NodeCli.handle(args)
      case "ring" => RingCli.handle(args)
      case _ => throw new Error(s"unsupported command $cmd")
    }
  }

  def handleHelp(args: Array[String]): Unit = {
    val cmd = if (args.length > 0) args(0) else null
    val args_ = args.slice(1, args.length)

    cmd match {
      case null =>
        printLine("Usage: <cmd> ...\n")
        printCmds()

        printLine()
        printLine("Run `help <cmd>` to see details of specific command")
      case "help" =>
        printLine("Print general or command-specific help\nUsage: help [cmd [cmd]]")
      case "scheduler" =>
        SchedulerCli.handle(args_, help = true)
      case "node" =>
        NodeCli.handle(args_, help = true)
      case "ring" =>
        RingCli.handle(args_, help = true)
      case _ =>
        throw new Error(s"unsupported command $cmd")
    }
  }

  private def printCmds(): Unit = {
    printLine("Commands:")
    printLine("help [cmd [cmd]] - print general or command-specific help", 1)
    printLine("scheduler        - start scheduler", 1)
    printLine("node             - node management commands", 1)
    printLine("ring             - ring management commands", 1)
  }

  private[cli] def sendRequest(uri: String, params: Map[String, String]): Any = {
    def queryString(params: Map[String, String]): String = {
      var s = ""
      for ((name, value) <- params) {
        if (!s.isEmpty) s += "&"
        s += URLEncoder.encode(name, "utf-8")
        if (value != null) s += "=" + URLEncoder.encode(value, "utf-8")
      }
      s
    }

    val qs: String = queryString(params)
    val url: String = Config.api + (if (Config.api.endsWith("/")) "" else "/") + "api" + uri

    val connection: HttpURLConnection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    var response: String = null
    try {
      connection.setRequestMethod("POST")
      connection.setDoOutput(true)

      val data = qs.getBytes("utf-8")
      connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
      connection.setRequestProperty("Content-Length", "" + data.length)
      connection.getOutputStream.write(data)

      try { response = Source.fromInputStream(connection.getInputStream).getLines().mkString}
      catch {
        case e: IOException =>
          if (connection.getResponseCode != 200) throw new IOException(connection.getResponseCode + " - " + connection.getResponseMessage)
          else throw e
      }
    } finally {
      connection.disconnect()
    }

    if (response.trim().isEmpty) return null

    var json: Any = null
    try { json = Util.parseJson(response)}
    catch { case e: IllegalArgumentException => throw new IOException(e) }

    json
  }

  def resolveApi(api: String) {
    if (Config.api != null) return

    if (api != null && api != "") {
      Config.api = api
      return
    }

    if (System.getenv(Config.API_ENV) != null) {
      Config.api = System.getenv(Config.API_ENV)
      return
    }

    throw Error(s"Undefined API url. Please provide either a CLI --api option or ${Config.API_ENV} env.")
  }

  private[dse] def printLine(s: AnyRef = "", indent: Int = 0) = out.println("  " * indent + s)

  private[dse] def handleGenericOptions(args: Array[String], help: Boolean = false): Array[String] = {
    val parser = new joptsimple.OptionParser()
    parser.accepts("api", s"Binding host:port for http/artifact server. Optional if ${Config.API_ENV} env is set.")
      .withOptionalArg().ofType(classOf[String])

    parser.allowsUnrecognizedOptions()

    if (help) {
      printLine("Generic Options")
      parser.printHelpOn(out)
      return args
    }

    var options: OptionSet = null
    try { options = parser.parse(args: _*) }
    catch {
      case e: OptionException =>
        parser.printHelpOn(out)
        printLine()
        throw new Error(e.getMessage)
    }

    resolveApi(options.valueOf("api").asInstanceOf[String])
    options.nonOptionArguments().toArray(new Array[String](0))
  }

  case class Error(message: String) extends RuntimeException(message)
}