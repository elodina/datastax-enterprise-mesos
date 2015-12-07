package net.elodina.mesos.dse

import org.junit.Test
import org.junit.Assert._
import scala.util.parsing.json.JSONObject

class RingTest {
  @Test
  def toJSON_fromJSON {
    val ring: Ring = new Ring("1")
    ring.name = "name"

    val json: JSONObject = ring.toJson
    val read = new Ring(json.obj)

    assertEquals(ring.id, read.id)
    assertEquals(ring.name, read.name)
  }
}
