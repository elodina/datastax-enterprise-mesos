package net.elodina.mesos.dse

import scala.util.parsing.json.JSONObject
import scala.collection.mutable

class Ring {
  var id: String = null
  var name: String = null

  var storagePort: Integer = null
  var jmxPort: Integer = null
  var nativePort: Integer = null
  var rpcPort: Integer = null

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

    if (json.contains("storagePort")) storagePort = json("storagePort").asInstanceOf[Number].intValue()
    if (json.contains("jmxPort")) jmxPort = json("jmxPort").asInstanceOf[Number].intValue()
    if (json.contains("nativePort")) nativePort = json("nativePort").asInstanceOf[Number].intValue()
    if (json.contains("rpcPort")) rpcPort = json("rpcPort").asInstanceOf[Number].intValue()
  }

  def toJson: JSONObject = {
    val json = new mutable.LinkedHashMap[String, Any]()

    json("id") = id
    if (name != null) json("name") = name

    if (storagePort != null) json("storagePort") = storagePort
    if (jmxPort != null) json("jmxPort") = jmxPort
    if (nativePort != null) json("nativePort") = nativePort
    if (rpcPort != null) json("rpcPort") = rpcPort

    new JSONObject(json.toMap)
  }

  override def hashCode(): Int = id.hashCode

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[Ring]) false
    id == obj.asInstanceOf[Ring].id
  }

  override def toString: String = id
}
