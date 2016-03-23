package net.elodina.mesos.dse

import org.junit.Assert._
import java.io.{PrintStream, ByteArrayOutputStream}

trait CliTestCase{
  def cli: (Array[String]) => Unit

  def assertCliError(args: Array[String], msg: String) = {
    val _ = try{ cli(args) }
    catch { case e: Cli.Error => assertTrue(e.getMessage.contains(msg)) }
  }

  def assertCliResponse(args: Array[String], msg: String, newLine: Boolean = true) = {
    val baos = new ByteArrayOutputStream()
    Cli.out = new PrintStream(baos, true)
    try {
      cli(args)
      assertEquals(if (newLine) msg + "\n" else msg, baos.toString)
    } finally {
      Cli.out = System.out
    }
  }

  def outputToString(cmd: => Unit) = {
    val baos = new ByteArrayOutputStream()
    val out = new PrintStream(baos, true)
    Cli.out = out
    try {
      cmd
    } finally {
      Cli.out = System.out
    }
    baos.toString
  }
}
