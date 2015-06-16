/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.streamingdemo


// ----------------------------------------------------------------------------
//  Event Types
// ----------------------------------------------------------------------------

object EventType {

  type EventType = Int
  
  val a : EventType = 1
  val b : EventType = 2
  val c : EventType = 3
  val d : EventType = 4
  val e : EventType = 5
  val f : EventType = 6
  val g : EventType = 7
}



// ----------------------------------------------------------------------------
//  Events
// ----------------------------------------------------------------------------

case class Event(sourceAddress: Int, event: EventType.EventType)

case class Alert(address: Int, state: State, transition: EventType.EventType)