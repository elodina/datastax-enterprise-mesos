package net.elodina.mesos.dse

import org.junit.{Test, Before, After}
import org.junit.Assert._

class NodeCliTest extends MesosTestCase {
  def cli = NodeCli.handle(_: Array[String])

  def assertCliError(command: => Any, msg: String) = {
    val _ = try{ command }
    catch { case e: Cli.Error => assertEquals(msg, e.getMessage) }
  }

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
    assertCliError(cli(Array()), "command required")

    val argumentRequiredCommands = List("add", "update", "remove", "start", "stop")
    for(command <- argumentRequiredCommands) assertCliError(cli(Array(command)), "argument required")

    assertCliError(cli(Array("wrong_command", "arg")), "unsupported node command wrong_command")
  }

  @Test
  def handleList() = {

  }
}