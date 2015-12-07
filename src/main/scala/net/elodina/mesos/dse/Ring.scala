package net.elodina.mesos.dse

import scala.util.parsing.json.JSONObject
import scala.collection.mutable

class Ring {
  var id: String = null
  var name: String = null

  def this(_id: String) {
    this
    id = _id
  }

  def this(json: Map[String, Any]) {
    this
    fromJson(json)
  }

  def availSeeds: List[String] = {
    val nodes: List[Node] = Cluster.getNodes
      .filter(_.ring == this)
      .filter(_.seed)
      .filter(_.runtime != null)

    nodes.map(_.runtime.hostname).sorted
  }

  def fromJson(json: Map[String, Any]): Unit = {
    id = json("id").asInstanceOf[String]
    if (json.contains("name")) name = json("name").asInstanceOf[String]
  }

  def toJson: JSONObject = {
    val json = new mutable.LinkedHashMap[String, Any]()
    json("id") = id
    if (name != null) json("name") = name
    new JSONObject(json.toMap)
  }

  override def hashCode(): Int = id.hashCode

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[Ring]) false
    id == obj.asInstanceOf[Ring].id
  }

  override def toString: String = id
}
