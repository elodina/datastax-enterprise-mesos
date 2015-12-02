package net.elodina.mesos.dse.cli

import java.io.{File, IOException, PrintStream}
import java.net.{URLEncoder, HttpURLConnection, URL}

import net.elodina.mesos.dse._
import net.elodina.mesos.utils
import play.api.libs.json.{Json, Writes}
import scopt.{OptionParser, Read}

import scala.concurrent.duration.Duration
import scala.io.Source

object Cli {
  private[cli] var out: PrintStream = System.out

  def main(args: Array[String]) {
    if (args.length == 0) {
      throw new Error("command required")
    }

    val cmd = args(0)
    val _args:Array[String] = args.slice(1, args.length)

    if (cmd == "ring") { RingCli.handle(cmd, _args); return }

    try {
      parser.parse(args, NoOptions) match {
        case None =>
          printLine("Failed to parse arguments.")
          parser.showUsage
        case Some(config) => config match {
          case NoOptions =>
            printLine("Failed to parse arguments.")
            parser.showUsage
          case schedulerOpts: SchedulerOptions => handleScheduler(schedulerOpts)
          case addOpts: AddOptions => handleApi("/add", addOpts)
          case updateOpts: UpdateOptions => handleApi("/update", updateOpts)
          case startOpts: StartOptions => handleApi("/start", startOpts)
          case stopOpts: StopOptions => handleApi("/stop", stopOpts)
          case removeOpts: RemoveOptions => handleApi("/remove", removeOpts)
          case statusOpts: StatusOptions => handleApi("/status", statusOpts)
        }
      }
    } catch {
      case e: Throwable =>
        System.err.println("Error: " + e.getMessage)
        sys.exit(1)
    }
  }

  def handleScheduler(config: SchedulerOptions) {
    resolveApi(config.api)

    Config.master = config.master
    Config.user = config.user
    Config.principal = if (config.principal.isEmpty) null else config.principal
    Config.secret = if (config.secret.isEmpty) null else config.secret
    Config.frameworkName = config.frameworkName
    Config.frameworkRole = config.frameworkRole
    Config.frameworkTimeout = config.frameworkTimeout
    Config.storage = config.storage
    Config.debug = config.debug

    if (!config.jre.isEmpty) {
      Config.jre = new File(config.jre)
      if (!Config.jre.exists() || !Config.jre.isFile) throw new IllegalStateException("JRE file doesn't exist")
    }

    Scheduler.start()
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

    new ApiResponse(Util.parseJson(response))
  }

  private[cli] def sendRequest(uri: String, params: Map[String, String]): Map[String, Any] = {
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

    var json: Map[String, Object] = null
    try { json = Util.parseJson(response)}
    catch { case e: IllegalArgumentException => throw new IOException(e) }

    json
  }

  private def resolveApi(api: String) {
    if (Config.api != null) return

    if (api != "") {
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

    cmd("scheduler").text("Starts the Datastax Enterprise Mesos Scheduler.").action { (_, c) =>
      SchedulerOptions()
    }.children(
      opt[String]("api").optional().text(s"Binding host:port for http/artifact server. Optional if ${Config.API_ENV} env is set.").action { (value, config) =>
        config.asInstanceOf[SchedulerOptions].copy(api = value)
      },

      opt[String]("master").required().text("Mesos Master addresses.").action { (value, config) =>
        config.asInstanceOf[SchedulerOptions].copy(master = value)
      },

      opt[String]("user").optional().text("Mesos user. Defaults to current system user.").action { (value, config) =>
        config.asInstanceOf[SchedulerOptions].copy(user = value)
      },

      opt[String]("principal").optional().text("Principal (username) used to register framework.").action { (value, config) =>
        config.asInstanceOf[SchedulerOptions].copy(principal = value)
      },

      opt[String]("secret").optional().text("Secret (password) used to register framework.").action { (value, config) =>
        config.asInstanceOf[SchedulerOptions].copy(secret = value)
      },

      opt[String]("framework-name").optional().text("Framework name. Defaults to datastax-enterprise").action { (value, config) =>
        config.asInstanceOf[SchedulerOptions].copy(frameworkName = value)
      },

      opt[String]("framework-role").optional().text("Framework role. Defaults to *").action { (value, config) =>
        config.asInstanceOf[SchedulerOptions].copy(frameworkRole = value)
      },

      opt[Duration]("framework-timeout").optional().text("Framework failover timeout. Defaults to 30 days.").action { (value, config) =>
        config.asInstanceOf[SchedulerOptions].copy(frameworkTimeout = value)
      },

      opt[String]("storage").optional().text("Storage for cluster state. Examples: file:dse-mesos.json; zk:master:2181/dse-mesos.").action { (value, config) =>
        config.asInstanceOf[SchedulerOptions].copy(storage = value)
      },

      opt[Boolean]("debug").optional().text("Run in debug mode.").action { (value, config) =>
        config.asInstanceOf[SchedulerOptions].copy(debug = value)
      },

      opt[String]("jre").optional().text("Path to JRE archive.").action { (value, config) =>
	config.asInstanceOf[SchedulerOptions].copy(jre = value)
      }
    )

    cmd("add").text("Adds a node to the cluster.").action { (_, c) =>
      AddOptions()
    }.children(
      arg[List[utils.Range]]("<id>").text("ID expression to add").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(id = value.mkString(","))
      },

      opt[String]("api").optional().text(s"Binding host:port for http/artifact server. Optional if ${Config.API_ENV} env is set.").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(api = value)
      },

      opt[Double]("cpu").optional().text("CPU amount (0.5, 1, 2).").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(cpu = value)
      },

      opt[Long]("mem").optional().text("Mem amount in Mb.").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(mem = value)
      },

      opt[String]("broadcast").optional().text("Network interface to broadcast for nodes.").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(broadcast = value)
      },

      opt[String]("constraints").optional().text("Constraints (hostname=like:^master$,rack=like:^1.*$).").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(constraints = value)
      },

      opt[String]("seed-constraints").optional().text("Seed node constraints. Will be evaluated only across seed nodes.").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(seedConstraints = value)
      },

      opt[String]("node-out").optional().text("File name to redirect Datastax Node output to.").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(nodeOut = value)
      },

      opt[String]("agent-out").optional().text("File name to redirect Datastax Agent output to.").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(agentOut = value)
      },

      opt[String]("cluster-name").optional().text("The name of the cluster.").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(clusterName = value)
      },

      opt[Boolean]("seed").optional().text("Flags whether this Datastax Node is a seed node.").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(seed = value)
      },

      opt[String]("replace-address").optional().text("Replace address for the dead Datastax Node").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(replaceAddress = value)
      },

      opt[String]("data-file-dirs").optional().text("Cassandra data file directories separated by comma. Defaults to sandbox if not set").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(dataFileDirs = value)
      },

      opt[String]("commit-log-dir").optional().text("Cassandra commit log dir. Defaults to sandbox if not set").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(commitLogDir = value)
      },

      opt[String]("saved-caches-dir").optional().text("Cassandra saved caches dir. Defaults to sandbox if not set").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(savedCachesDir = value)
      },

      opt[Duration]("state-backoff").optional().text("Backoff between checks for consistent node state.").action { (value, opts) =>
        opts.asInstanceOf[AddOptions].copy(awaitConsistentStateBackoff = value)
      }
    )

    cmd("update").text("Update node configuration.").action { (_, c) =>
      UpdateOptions()
    }.children(
      arg[List[utils.Range]]("<id>").text("ID expression to update").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(id = value.mkString(","))
      },

      opt[String]("api").optional().text(s"Binding host:port for http/artifact server. Optional if ${Config.API_ENV} env is set.").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(api = value)
      },

      opt[Double]("cpu").optional().text("CPU amount (0.5, 1, 2).").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(cpu = Some(value))
      },

      opt[Long]("mem").optional().text("Mem amount in Mb.").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(mem = Some(value))
      },

      opt[String]("broadcast").optional().text("Network interface to broadcast for nodes.").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(broadcast = Some(value))
      },

      opt[String]("constraints").optional().text("Constraints (hostname=like:^master$,rack=like:^1.*$).").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(constraints = Some(value))
      },

      opt[String]("seed-constraints").optional().text("Seed node constraints. Will be evaluated only across seed nodes.").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(seedConstraints = Some(value))
      },

      opt[String]("node-out").optional().text("File name to redirect Datastax Node output to.").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(nodeOut = Some(value))
      },

      opt[String]("agent-out").optional().text("File name to redirect Datastax Agent output to.").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(agentOut = Some(value))
      },

      opt[String]("cluster-name").optional().text("The name of the cluster.").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(clusterName = Some(value))
      },

      opt[Boolean]("seed").optional().text("Flags whether this Datastax Node is a seed node.").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(seed = Some(value))
      },

      opt[String]("replace-address").optional().text("Replace address for the dead Datastax Node").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(replaceAddress = Some(value))
      },

      opt[String]("data-file-dirs").optional().text("Cassandra data file directories separated by comma. Defaults to sandbox if not set").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(dataFileDirs = Some(value))
      },

      opt[String]("commit-log-dir").optional().text("Cassandra commit log dir. Defaults to sandbox if not set").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(commitLogDir = Some(value))
      },

      opt[String]("saved-caches-dir").optional().text("Cassandra saved caches dir. Defaults to sandbox if not set").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(savedCachesDir = Some(value))
      },

      opt[Duration]("state-backoff").optional().text("Backoff between checks for consistent node state.").action { (value, opts) =>
        opts.asInstanceOf[UpdateOptions].copy(awaitConsistentStateBackoff = Some(value))
      }
    )

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

    cmd("status").text("Retrieves current cluster status.").action { (_, c) =>
      StatusOptions()
    }.children(
      opt[String]("api").optional().text(s"Binding host:port for http/artifact server. Optional if ${Config.API_ENV} env is set.").action { (value, opts) =>
        opts.asInstanceOf[StopOptions].copy(api = value)
      }
    )
  }

  case class CliError(message: String) extends RuntimeException(message)

}