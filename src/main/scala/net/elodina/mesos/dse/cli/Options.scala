/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.elodina.mesos.dse.cli

import play.api.libs.json._

import scala.concurrent.duration.{Duration, _}
import scala.language.postfixOps

object DurationFormats {
  implicit val formats = new Format[Duration] {
    override def writes(o: Duration): JsValue = JsString(o.toString)

    override def reads(json: JsValue): JsResult[Duration] = json.validate[String].map(Duration.apply)
  }
}

sealed trait Options {
  val api: String
}

object NoOptions extends Options {
  val api = ""
}
case class StartOptions(id: String = "", api: String = "", timeout: Duration = 2 minutes) extends Options

object StartOptions {
  implicit val durationFormats = DurationFormats.formats
  implicit val formats = Json.format[StartOptions]
}

case class StopOptions(id: String = "", api: String = "") extends Options

object StopOptions {
  implicit val formats = Json.format[StopOptions]
}

case class RemoveOptions(id: String = "", api: String = "") extends Options

object RemoveOptions {
  implicit val formats = Json.format[RemoveOptions]
}