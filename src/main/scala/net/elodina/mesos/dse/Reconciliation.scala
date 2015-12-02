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

package net.elodina.mesos.dse

import java.util.{Collections, Date}

import org.apache.log4j.Logger
import org.apache.mesos.Protos.{TaskID, TaskState, TaskStatus}
import org.apache.mesos.SchedulerDriver

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

trait Reconciliation[T <: Node] {
  protected val reconcileDelay: Duration
  protected val reconcileMaxTries: Int

  protected def nodes: Traversable[T]

  private val logger = Logger.getLogger(this.getClass)
  private[dse] var reconciles = 0
  private[dse] var reconcileTime = new Date(0)

  def implicitReconcile(driver: SchedulerDriver, now: Date = new Date()) {
    this.reconcile(driver, isImplicit = true, now)
  }

  def explicitReconcile(driver: SchedulerDriver, now: Date = new Date()) {
    this.reconcile(driver, isImplicit = false, now)
  }

  private def reconcile(driver: SchedulerDriver, isImplicit: Boolean, now: Date = new Date()) {
    if (now.getTime - reconcileTime.getTime >= reconcileDelay.toMillis) {
      if (!nodes.exists(node => node.state == Node.State.Reconciling && node.runtime != null)) reconciles = 0
      reconciles += 1
      reconcileTime = now

      nodes.filter(_.runtime == null).foreach { node =>
        if (node.state == Node.State.Staging || node.state == Node.State.Running) node.state = Node.State.Stopped
      }

      if (reconciles > reconcileMaxTries) {
        nodes.filter(_.state == Node.State.Reconciling).foreach { node =>
          logger.info(s"Reconciling exceeded $reconcileMaxTries tries for node ${node.id}, sending killTask for node ${node.id}")
          driver.killTask(TaskID.newBuilder().setValue(node.id).build())
        }
      } else {
        if (isImplicit) {
          nodes.foreach(_.state = Node.State.Reconciling)
          driver.reconcileTasks(Collections.emptyList())
        } else {
          val statuses = nodes.filter(_.runtime != null).flatMap { node =>
            if (node.state == Node.State.Reconciling) {
              logger.info(s"Reconciling $reconciles/$reconcileMaxTries state of node ${node.id} with node id ${node.runtime.taskId}")
              Some(TaskStatus.newBuilder()
                .setTaskId(TaskID.newBuilder().setValue(node.runtime.taskId))
                .setState(TaskState.TASK_STAGING)
                .build)
            } else None
          }.toList

          driver.reconcileTasks(statuses)
        }
      }
    }
  }
}