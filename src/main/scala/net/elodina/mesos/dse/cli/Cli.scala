package net.elodina.mesos.dse.cli

import java.io.PrintStream

import net.elodina.mesos.dse.{Scheduler, Config}
import scopt.{Read, OptionParser}

import scala.concurrent.duration.Duration

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
    Config.frameworkName = config.frameworkName
    Config.frameworkRole = config.frameworkRole
    Config.frameworkTimeout = config.frameworkTimeout
    Config.storage = config.storage

    Scheduler.start()
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

  private def printLine(s: AnyRef = "", indent: Int = 0) = out.println("  " * indent + s)

  def reads[A](f: String => A): Read[A] = new Read[A] {
    val arity = 1
    val reads = f
  }

  implicit val durationRead: Read[Duration] = reads(Duration.apply)

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
      }
    )
  }

  case class CliError(message: String) extends RuntimeException(message)
}