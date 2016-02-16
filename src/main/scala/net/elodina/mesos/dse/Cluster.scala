package net.elodina.mesos.dse

import scala.util.parsing.json.JSONObject
import scala.collection.mutable
import Util.BindAddress

class Cluster {
  var id: String = null
  var name: String = null

  var bindAddress: BindAddress = null
  var ports: mutable.HashMap[Node.Port.Value, Util.Range] = new mutable.HashMap[Node.Port.Value, Util.Range]()
  var jmxRemote: Boolean = false

  resetPorts

  def this(_id: String) {
    this
    id = _id
  }

  def this(json: Map[String, Any]) {
    this
    fromJson(json)
  }

  def getNodes: List[Node] = Nodes.getNodes.filter(_.cluster == this)

  def active: Boolean = getNodes.exists(_.state != Node.State.IDLE)
  def idle: Boolean = !active

  def availSeeds: List[String] = {
    val nodes: List[Node] = Nodes.getNodes
      .filter(_.cluster == this)
      .filter(_.seed)
      .filter(_.runtime != null)
      .filter(_.runtime.address != null)

    nodes.map(_.runtime.address).sorted
  }

  def resetPorts: Unit = {
    ports.clear()
    Node.Port.values.foreach(ports(_) = null)
  }

  def fromJson(json: Map[String, Any]): Unit = {
    id = json("id").asInstanceOf[String]

    if (json.contains("bindAddress")) bindAddress = new BindAddress(json("bindAddress").asInstanceOf[String])

    resetPorts
    for ((port, range) <- json("ports").asInstanceOf[Map[String, String]])
      ports(Node.Port.withName(port)) = new Util.Range(range)

    if (json.contains("jmxRemote")) jmxRemote = json("jmxRemote").asInstanceOf[Boolean]
  }

  def toJson: JSONObject = {
    val json = new mutable.LinkedHashMap[String, Any]()
    json("id") = id

    if (bindAddress != null) json("bindAddress") = "" + bindAddress

    val portsJson = new mutable.HashMap[String, Any]()
    for ((port, range) <- ports)
      if (range != null) portsJson("" + port) = "" + range
    json("ports") = new JSONObject(portsJson.toMap)

    json("jmxRemote") = jmxRemote

    new JSONObject(json.toMap)
  }

  override def hashCode(): Int = id.hashCode

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[Cluster]) false
    else id == obj.asInstanceOf[Cluster].id
  }

  override def toString: String = id
}