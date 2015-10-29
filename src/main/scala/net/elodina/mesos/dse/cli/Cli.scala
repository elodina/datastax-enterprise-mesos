package net.elodina.mesos.dse.cli

import java.io.{IOException, PrintStream}
import java.net.{HttpURLConnection, URL}

import net.elodina.mesos.dse._
import net.elodina.mesos.utils
import play.api.libs.json.{Json, Writes}
import scopt.{OptionParser, Read}

import scala.concurrent.duration.Duration
import scala.io.Source

object Cli {
  private[cli] var out: PrintStream = System.out

  def main(args: Array[String]) {
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
          case addOpts: AddOptions => handleAdd(addOpts)
        }
      }
    } catch {
      case e: Throwable =>
        System.err.println("Error: " + e.getMessage)
        e.printStackTrace()
        sys.exit(1)
    }
  }

  def handleScheduler(config: SchedulerOptions) {
    resolveApi(config.api)

    Config.master = config.master
    Config.user = config.user
    Config.frameworkName = config.frameworkName
    Config.frameworkRole = config.frameworkRole
    Config.frameworkTimeout = config.frameworkTimeout
    Config.storage = config.storage
    Config.debug = config.debug

    Scheduler.start()
  }

  def handleAdd(config: AddOptions) {
    resolveApi(config.api)

    val response = sendRequest("/add", config)
    printResponse(response)
  }

  private[dse] def sendRequest[T: Writes](uri: String, data: T): ApiResponse = {
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

    Json.parse(response).as[ApiResponse]
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
    response.value.foreach { cluster =>
      printLine()
      printCluster(cluster)
    }
  }

  private def printCluster(cluster: Cluster) {
    printLine("cluster:")
    cluster.tasks.foreach(printTask(_, 1))
  }

  private def printTask(task: DSETask, indent: Int = 0) {
    printLine("TODO")
    printLine()
  }

  private def printLine(s: AnyRef = "", indent: Int = 0) = out.println("  " * indent + s)

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

      opt[String]("framework-name").optional().text("Framework name. Defaults to datastax-enterprise").action { (value, config) =>
        config.asInstanceOf[SchedulerOptions].copy(frameworkName = value)
      },

      opt[String]("framework-role").optional().text("Framework role. Defaults to *").action { (value, config) =>
        config.asInstanceOf[SchedulerOptions].copy(frameworkRole = value)
      },

      opt[Duration]("framework-timeout").optional().text("Framework failover timeout. Defaults to 30 days.").action { (value, config) =>
        config.asInstanceOf[SchedulerOptions].copy(frameworkTimeout = value)
      },

      opt[Duration]("storage").optional().text("Storage for cluster state. Examples: file:datastax.json; zk:master:2181/datastax-mesos.").action { (value, config) =>
        config.asInstanceOf[SchedulerOptions].copy(frameworkTimeout = value)
      },

      opt[Boolean]("debug").optional().text("Run in debug mode.").action { (value, config) =>
        config.asInstanceOf[SchedulerOptions].copy(debug = value)
      }
    )

    cmd("add").text("Adds a task to the cluster.").children(
      arg[String]("<task-type>").text("Task type to add").action { (taskType, config) =>
        taskType match {
          case TaskTypes.CASSANDRA_NODE => AddOptions(taskType = taskType, logStdout = s"$taskType.log", logStderr = s"$taskType.err")
          //other types go here
          case _ => throw new CliError(s"Unknown task type $taskType")
        }
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

        opt[String]("log-stdout").optional().text("File name to redirect Datastax Node stdout to.").action { (value, opts) =>
          opts.asInstanceOf[AddOptions].copy(logStdout = value)
        },

        opt[String]("log-stderr").optional().text("File name to redirect Datastax Node stderr to.").action { (value, opts) =>
          opts.asInstanceOf[AddOptions].copy(logStderr = value)
        },

        opt[String]("agent-stdout").optional().text("File name to redirect Datastax Agent stdout to.").action { (value, opts) =>
          opts.asInstanceOf[AddOptions].copy(agentStdout = value)
        },

        opt[String]("agent-stderr").optional().text("File name to redirect Datastax Agent stderr to.").action { (value, opts) =>
          opts.asInstanceOf[AddOptions].copy(agentStderr = value)
        },

        opt[String]("cluster-name").optional().text("The name of the cluster.").action { (value, opts) =>
          opts.asInstanceOf[AddOptions].copy(agentStderr = value)
        },

        opt[Boolean]("seed").optional().text("Flags whether this Datastax Node is a seed node.").action { (value, opts) =>
          opts.asInstanceOf[AddOptions].copy(seed = value)
        }
      )
    )
  }

  case class CliError(message: String) extends RuntimeException(message)

}