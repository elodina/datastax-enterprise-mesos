package net.elodina.mesos.dse

import org.junit.{Test, Before, After}
import org.junit.Assert._


class ClusterCliTest extends MesosTestCase with CliTestCase {
  def cli = ClusterCli.handle(_: Array[String])

  @Before
  override def before = {
    super.before
    HttpServer.start()
  }

  @After
  override def after = {
    super.after
    HttpServer.stop()
  }

  @Test
  def handle() = {
    assertCliError(Array(), "command required")

    val argumentRequiredCommands = List("add", "update", "remove")
    for(command <- argumentRequiredCommands) assertCliError(Array(command), "argument required")

    assertCliError(Array("wrong_command", "arg"), "unsupported cluster command wrong_command")
  }

}
