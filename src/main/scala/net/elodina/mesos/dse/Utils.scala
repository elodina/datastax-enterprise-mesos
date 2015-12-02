package net.elodina.mesos.dse

import scala.util.parsing.json.JSON

object Utils {
  private val jsonLock = new Object

  def parseJson(json: String): Map[String, Object] = {
    jsonLock synchronized {
      val node: Map[String, Object] = JSON.parseFull(json).getOrElse(null).asInstanceOf[Map[String, Object]]
      if (node == null) throw new IllegalArgumentException("Failed to parse json: " + json)
      node
    }
  }
}