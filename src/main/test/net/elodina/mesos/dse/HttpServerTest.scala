package net.elodina.mesos.dse

import javax.servlet.ServletOutputStream

import org.junit.{Before, Test, AfterClass}
import org.junit.Assert._
import net.elodina.mesos.dse.HttpServer.{Servlet}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.mockito.Mockito._
import java.io._
import scala.io.Source
import java.nio.file.Files

import scala.util.parsing.json.JSONArray


class StubServletOutputStream extends ServletOutputStream {
  val baos = new ByteArrayOutputStream
  def write(i: Int) = {
    baos.write(i)
  }
  def reset() = baos.reset()
  def size() = baos.size()
  override def toString = baos.toString
}

class HttpServerTest extends MesosTestCase {
  val stringWriter = new StringWriter()
  val writer = new PrintWriter(stringWriter)
  val servletOutputStream = new StubServletOutputStream

  @Before
  override def before = {
    super.before
    stringWriter.getBuffer.setLength(0)
  }

  private def makeRequestResponse(url: String) = {
    val request = mock(classOf[HttpServletRequest])
    when(request.getRequestURI).thenReturn(url)
    val response = mock(classOf[HttpServletResponse])
    when(response.getWriter).thenReturn(writer)
    servletOutputStream.reset()
    when(response.getOutputStream).thenReturn(servletOutputStream)

    (request, response)
  }

  private def makeTempFile(filename: String) = {
    val file = new File(filename)
    val content = "content"
    val tmpWriter = new PrintWriter(file)
    tmpWriter.write(content)
    tmpWriter.flush()
    tmpWriter.close()

    (file, content)
  }
  
  @Test
  def health() = {
    // build request and response
    val (request, response) = makeRequestResponse("/health")

    // call get method
    Servlet.doGet(request, response)

    // verify response
    verify(response).setContentType("text/plain; charset=utf-8")
    writer.flush()
    assertTrue(stringWriter.toString.contains("ok"))
  }

  @Test
  def downloadFileJar() = {
    // build request and response
    val (request, response) = makeRequestResponse("/jar/")
    val (file, content) = makeTempFile("tmp_jar_file.jar")
    Config.jar = file

    // call get method
    Servlet.doGet(request, response)
    file.delete()

    // verify response
    verify(response).setContentType("application/zip")
    verify(response).setHeader("Content-Length", content.length.toString)
    verify(response).setHeader("Content-Disposition", s"""attachment; filename="${file.getName}"""")
    assertEquals(servletOutputStream.toString, content)
  }

  @Test
  def downloadFileDse() = {
    // build request and response
    val (request, response) = makeRequestResponse("/dse/")
    val (file, content) = makeTempFile("tmp_dse_file.tar.gz")
    Config.dse = file

    // call get method
    Servlet.doGet(request, response)
    file.delete()

    // verify response
    verify(response).setContentType("application/zip")
    verify(response).setHeader("Content-Length", content.length.toString)
    verify(response).setHeader("Content-Disposition", s"""attachment; filename="${file.getName}"""")
    assertEquals(servletOutputStream.toString, content)


    Config.dse = null
    Servlet.doGet(request, response)
    verify(response).sendError(404, "not found")
  }

  @Test
  def downloadFileCassandra() = {
    // build request and response
    val (request, response) = makeRequestResponse("/cassandra/")
    val (file, content) = makeTempFile("tmp_cassandra_file.tar.gz")
    Config.cassandra = file

    // call get method
    Servlet.doGet(request, response)
    file.delete()

    // verify response
    verify(response).setContentType("application/zip")
    verify(response).setHeader("Content-Length", content.length.toString)
    verify(response).setHeader("Content-Disposition", s"""attachment; filename="${file.getName}"""")
    assertEquals(servletOutputStream.toString, content)

    Config.cassandra = null
    Servlet.doGet(request, response)
    verify(response).sendError(404, "not found")
  }

  @Test
  def downloadFileJre() = {
    // build request and response
    val (request, response) = makeRequestResponse("/jre/")
    val (file, content) = makeTempFile("tmp_jre_file.tar.gz")
    Config.jre = file

    // call get method
    Servlet.doGet(request, response)
    file.delete()

    // verify response
    verify(response).setContentType("application/zip")
    verify(response).setHeader("Content-Length", content.length.toString)
    verify(response).setHeader("Content-Disposition", s"""attachment; filename="${file.getName}"""")
    assertEquals(servletOutputStream.toString, content)

    Config.jre = null
    Servlet.doGet(request, response)
    verify(response).sendError(404, "not found")
  }

  @Test
  def nodeApiList() = {
    // build request and response
    val (request, response) = makeRequestResponse("/api/node/list")
    Nodes.addNode(new Node("0"))
    val nodesJson = Nodes.getNodes.map(_.toJson(expanded = true))

    Servlet.doGet(request, response)

    // verify response
    assertEquals(stringWriter.toString, new JSONArray(nodesJson.toList).toString() + "\n")

    Nodes.reset()
  }
  @Test def nodeApiAddUpdate() = {}
  @Test def nodeApiRemove() = {}
  @Test def nodeApiStartStop() = {}
  @Test def clusterApi() = {}
}
