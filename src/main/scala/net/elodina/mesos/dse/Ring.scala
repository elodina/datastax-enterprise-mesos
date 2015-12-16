package net.elodina.mesos.dse

import scala.util.parsing.json.JSONObject
import scala.collection.mutable

class Ring {
  var id: String = null
  var name: String = null
  var ports: mutable.HashMap[String, Util.Range] = new mutable.HashMap[String, Util.Range]()

  resetPorts

  def this(_id: String) {
    this
    id = _id
  }

  def this(json: Map[String, Any]) {
    this
    fromJson(json)
  }

  def getNodes: List[Node] = Cluster.getNodes.filter(_.ring == this)

  def active: Boolean = getNodes.exists(_.state != Node.State.INACTIVE)
  def idle: Boolean = !active

  def availSeeds: List[String] = {
    val nodes: List[Node] = Cluster.getNodes
      .filter(_.ring == this)
      .filter(_.seed)
      .filter(_.runtime != null)

    nodes.map(_.runtime.hostname).sorted
  }

  def resetPorts: Unit = {
    ports.clear()
    Node.portNames.foreach(ports(_) = null)
  }

  def fromJson(json: Map[String, Any]): Unit = {
    id = json("id").asInstanceOf[String]
    if (json.contains("name")) name = json("name").asInstanceOf[String]

    resetPorts
    for ((name, range) <- json("ports").asInstanceOf[Map[String, String]])
      ports(name) = new Util.Range(range)
  }

  def toJson: JSONObject = {
    val json = new mutable.LinkedHashMap[String, Any]()

    json("id") = id
    if (name != null) json("name") = name

    val portsJson = new mutable.HashMap[String, Any]()
    for ((name, range) <- ports)
      if (range != null) portsJson(name) = "" + range
    json("ports") = new JSONObject(portsJson.toMap)

    new JSONObject(json.toMap)
  }

  override def hashCode(): Int = id.hashCode

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[Ring]) false
    id == obj.asInstanceOf[Ring].id
  }

  override def toString: String = id
}