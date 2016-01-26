package net.elodina.mesos.dse

import java.io.{PrintStream, PrintWriter, ByteArrayOutputStream}

import org.junit.{Test, Before, After}
import org.junit.Assert._

class NodeCliTest extends MesosTestCase {
  def cli = NodeCli.handle(_: Array[String])

  def assertCliError(args: Array[String], msg: String) = {
    val _ = try{ cli(args) }
    catch { case e: Cli.Error => assertEquals(msg, e.getMessage) }
  }

  def assertCliResponse(args: Array[String], msg: String, newLine: Boolean = true) = {
    baos.reset()
    cli(args)
    System.out.flush()
    assertEquals(if(newLine) msg + "\n" else msg, baos.toString)
  }

  val baos = new ByteArrayOutputStream()
  val out = new PrintStream(baos)
  val defaultOut = System.out

  def outputToString(cmd: => Unit) = {
    val oldOut = Cli.out
    val baos = new ByteArrayOutputStream()
    val out = new PrintStream(baos)
    Cli.out = out
    cmd
    Cli.out.flush()
    Cli.out = oldOut
    baos.toString
  }

  @Before
  override def before = {
    super.before
    HttpServer.start()
    baos.reset()
    System.setOut(out)
  }

  @After
  override def after = {
    super.after
    HttpServer.stop()
    baos.reset()
    System.setOut(defaultOut)
  }

  @Test
  def handle() = {
    assertCliError(Array(), "command required")

    val argumentRequiredCommands = List("add", "update", "remove", "start", "stop")
    for(command <- argumentRequiredCommands) assertCliError(Array(command), "argument required")

    assertCliError(Array("wrong_command", "arg"), "unsupported node command wrong_command")
  }

  @Test
  def handleList() = {
    assertCliResponse(Array("list"), "no nodes")

    val node = new Node("0")
    Nodes.addNode(node)
    Nodes.save()
    assertCliResponse(
      Array("list"),
      "node:\n" + outputToString { NodeCli.printNode(node, 1) })
  }
}