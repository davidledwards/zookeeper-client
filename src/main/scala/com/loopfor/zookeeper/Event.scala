/*
 * Copyright 2020 David Edwards
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.loopfor.zookeeper

import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.Watcher.Event.EventType.{None => NodeNone}
import scala.language._

/**
 * An ''event'' generated by ZooKeeper.
 */
sealed trait Event

/**
 * A category of [[Event events]] representing state changes to sessions.
 */
trait StateEvent extends Event

/**
 * An event indicating that the client is not connected to ZooKeeper.
 */
case object Disconnected extends StateEvent

/**
 * An event indicating that the client is connected to ZooKeeper.
 */
case object Connected extends StateEvent

/**
 * An event indicating that the client failed to authenticate with ZooKeeper.
 */
case object AuthenticationFailed extends StateEvent

/**
 * An event indicating that the client is connected to ZooKeeper in a read-only manner.
 */
case object ConnectedReadOnly extends StateEvent

/**
 * An event indicating that the client successfully authenticated with ZooKeeper.
 */
case object Authenticated extends StateEvent

/**
 * An event indicating that the client session has been expired by ZooKeeper.
 */
case object Expired extends StateEvent

/**
 * An event indicating that the client has closed its session with ZooKeeper.
 */
case object Closed extends StateEvent

private[zookeeper] object StateEvent {
  private val events = Map(
        KeeperState.Disconnected.getIntValue -> Disconnected,
        KeeperState.SyncConnected.getIntValue -> Connected,
        KeeperState.AuthFailed.getIntValue -> AuthenticationFailed,
        KeeperState.ConnectedReadOnly.getIntValue -> ConnectedReadOnly,
        KeeperState.SaslAuthenticated.getIntValue -> Authenticated,
        KeeperState.Expired.getIntValue -> Expired,
        KeeperState.Closed.getIntValue -> Closed
        )

  def apply(state: KeeperState): StateEvent = apply(state.getIntValue)

  def apply(state: Int): StateEvent = events.get(state) match {
    case Some(event) => event
    case _ => new Unrecognized(state)
  }

  private class Unrecognized(state: Int) extends StateEvent {
    override def toString: String = "UnrecognizedStateEvent[" + state + "]"
  }
}

/**
 * A category of [[Event events]] representing changes to nodes.
 */
trait NodeEvent extends Event {
  /**
   * Returns the path of the node corresponding to this event.
   * 
   * @return the path of the node corresponding to this event
   */
  def path: String
}

/**
 * An event indicating that a node was created.
 */
case class Created(path: String) extends NodeEvent

/**
 * An event indicating that a node was deleted.
 */
case class Deleted(path: String) extends NodeEvent

/**
 * An event indicating that the data associated with a node was changed.
 */
case class DataChanged(path: String) extends NodeEvent

/**
 * An event indicating that the children associated with a node were changed.
 */
case class ChildrenChanged(path: String) extends NodeEvent

/**
 * An event indicating that a child watch was removed.
 */
case class ChildWatchRemoved(path: String) extends NodeEvent

/**
 * An event indicating that a data watch was removed.
 */
case class DataWatchRemoved(path: String) extends NodeEvent

/**
 * An event indicating that a persistent watch was removed.
 */
case class PersistentWatchRemoved(path: String) extends NodeEvent

private[zookeeper] object NodeEvent {
  private val events = Map[Int, String => NodeEvent](
        EventType.NodeCreated.getIntValue -> { p => new Created(p) },
        EventType.NodeDeleted.getIntValue -> { p => new Deleted(p) },
        EventType.NodeDataChanged.getIntValue -> { p => new DataChanged(p) },
        EventType.NodeChildrenChanged.getIntValue -> { p => new ChildrenChanged(p) },
        EventType.ChildWatchRemoved.getIntValue -> { p => new ChildWatchRemoved(p) },
        EventType.DataWatchRemoved.getIntValue -> { p => new DataWatchRemoved(p) },
        EventType.PersistentWatchRemoved.getIntValue -> { p => new PersistentWatchRemoved(p) }
        )

  def apply(event: EventType, path: String): NodeEvent = apply(event.getIntValue, path)

  def apply(event: Int, path: String): NodeEvent = events.get(event) match {
    case Some(fn) => fn(path)
    case _ => new Unrecognized(event, path)
  }

  private class Unrecognized(event: Int, val path: String) extends NodeEvent {
    override def toString: String = "UnrecognizedNodeEvent[" + event + "](" + path + ")"
  }
}

private[zookeeper] object Event {
  def apply(event: WatchedEvent): Event = event.getType match {
    case NodeNone => StateEvent(event.getState)
    case e @ _ => NodeEvent(e, event.getPath)
  }
}
