package net.elodina.mesos.dse.cli

import java.io.{IOException, PrintStream}
import java.net.{URLEncoder, HttpURLConnection, URL}

import net.elodina.mesos.dse._
import net.elodina.mesos.utils
import play.api.libs.json.{Json, Writes}
import scopt.{OptionParser, Read}

import scala.concurrent.duration.Duration
import scala.io.Source
import joptsimple.{OptionException, OptionSet}

object Cli {
  private[cli] var out: PrintStream = System.out

  def main(args: Array[String]) {
    if (args.length == 0) {
      throw new CliError("command required")
    }

    val cmd = args(0)
    var _args:Array[String] = args.slice(1, args.length)
    _args = handleGenericOptions(_args)

    if (cmd == "help") { handleHelp(_args); return }
    if (cmd == "scheduler") { SchedulerCli.handle(_args); return }
    if (cmd == "node") { NodeCli.handle(_args); return }
    if (cmd == "ring") { RingCli.handle(_args); return }

    try {
      parser.parse(args, NoOptions) match {
        case None =>
          printLine("Failed to parse arguments.")
          parser.showUsage
        case Some(config) => config match {
          case NoOptions =>
            printLine("Failed to parse arguments.")
            parser.showUsage
          case startOpts: StartOptions => handleApi("/node/start", startOpts)
          case stopOpts: StopOptions => handleApi("/node/stop", stopOpts)
          case removeOpts: RemoveOptions => handleApi("/node/remove", removeOpts)
        }
      }
    } catch {
      case e: Throwable =>
        System.err.println("Error: " + e.getMessage)
        sys.exit(1)
    }
  }

  def handleHelp(args: Array[String]): Unit = {
    val cmd = if (args.length > 0) args(0) else null
    val args_ = args.slice(1, args.length)

    cmd match {
      case null =>
        printLine("Usage: <cmd>\n")
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

  def handleApi[T <: Options : Writes](url: String, data: T) {
    resolveApi(data.api)

    val response = sendJsonRequest(url, data)
    printResponse(response)
  }

  private[dse] def sendJsonRequest[T: Writes](uri: String, data: T): ApiResponse = {
    val url: String = Config.api + (if (Config.api.endsWith("/")) "" else "/") + "api" + uri

    val connection: HttpURLConnection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    var response: String = null
    try {
      connection.setRequestMethod("POST")
      connection.setDoOutput(true)

      val body = Json.stringify(Json.toJson(data)).getBytes("UTF-8")
      connection.setRequestProperty("Content-Type", "application/json; charset=utf-8")
      connection.setRequestProperty("Content-Length", "" + body.length)
      connection.getOutputStream.write(body)

      try {
        response = Source.fromInputStream(connection.getInputStream).getLines().mkString
      }
      catch {
        case e: IOException =>
          if (connection.getResponseCode != 200) throw new IOException(connection.getResponseCode + " - " + connection.getResponseMessage)
          else throw e
      }
    } finally {
      connection.disconnect()
    }

    new ApiResponse(Util.parseJsonAsMap(response))
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

    throw CliError(s"Undefined API url. Please provide either a CLI --api option or ${Config.API_ENV} env.")
  }

  private def printResponse(response: ApiResponse) {
    printLine(response.message)
    if (response.cluster != null) {
      printLine()
      printCluster(response.cluster)
    }
  }

  private def printCluster(cluster: Cluster) {
    printLine("cluster:")
    cluster.getNodes.foreach(printNode(_, 1))
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

  private[dse] def printLine(s: AnyRef = "", indent: Int = 0) = out.println("  " * indent + s)

  def reads[A](f: String => A): Read[A] = new Read[A] {
    val arity = 1
    val reads = f
  }

  implicit val durationRead: Read[Duration] = reads(Duration.apply)
  implicit val rangesRead: Read[List[utils.Range]] = reads(utils.Range.parseRanges)

  val parser = new OptionParser[Options]("dse-mesos.sh") {
    override def showUsage {
      Cli.out.println(usage)
    }

    help("help").text("Prints this usage text.")

    cmd("start").text("Starts nodes in the cluster.").action { (_, c) =>
      StartOptions()
    }.children(
      arg[List[utils.Range]]("<id>").text("ID expression to add").action { (value, opts) =>
        opts.asInstanceOf[StartOptions].copy(id = value.mkString(","))
      },

      opt[String]("api").optional().text(s"Binding host:port for http/artifact server. Optional if ${Config.API_ENV} env is set.").action { (value, opts) =>
        opts.asInstanceOf[StartOptions].copy(api = value)
      },

      opt[Duration]("timeout").optional().text("Time to wait until node starts. Should be a parsable Scala Duration value. Defaults to 2m. Optional").action { (value, config) =>
        config.asInstanceOf[StartOptions].copy(timeout = value)
      }
    )

    cmd("stop").text("Stops nodes in the cluster.").action { (_, c) =>
      StopOptions()
    }.children(
      arg[List[utils.Range]]("<id>").text("ID expression to stop").action { (value, opts) =>
        opts.asInstanceOf[StopOptions].copy(id = value.mkString(","))
      },

      opt[String]("api").optional().text(s"Binding host:port for http/artifact server. Optional if ${Config.API_ENV} env is set.").action { (value, opts) =>
        opts.asInstanceOf[StopOptions].copy(api = value)
      }
    )

    cmd("remove").text("Removes nodes in the cluster.").action { (_, c) =>
      RemoveOptions()
    }.children(
      arg[List[utils.Range]]("<id>").text("ID expression to remove").action { (value, opts) =>
        opts.asInstanceOf[RemoveOptions].copy(id = value.mkString(","))
      },

      opt[String]("api").optional().text(s"Binding host:port for http/artifact server. Optional if ${Config.API_ENV} env is set.").action { (value, opts) =>
        opts.asInstanceOf[RemoveOptions].copy(api = value)
      }
    )
  }

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
        throw new CliError(e.getMessage)
    }

    resolveApi(options.valueOf("api").asInstanceOf[String])
    options.nonOptionArguments().toArray(new Array[String](0))
  }

  case class CliError(message: String) extends RuntimeException(message)

}