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

package com.dataartisans.flink.example.eventpattern

/**
 * Data type for events, consisting of the originating IP address and an event type.
 * 
 * @param sourceAddress The originating address (think 32 bit IPv4 address).
 * @param event The event type.
 */
case class Event(sourceAddress: Int, event: Event.EventType) {

  override def toString: String = {
    s"Event ${Event.formatAddress(sourceAddress)} : ${Event.eventTypeName(event)}"
  }
}

/**
 * Data type for alerts.
 * 
 * @param address The originating address (think 32 bit IPv4 address).
 * @param state The state that the event state machine found.
 * @param transition The transition that was considered invalid.
 */
case class Alert(address: Int, state: State, transition: Event.EventType) {

  override def toString: String = {
    s"ALERT ${Event.formatAddress(address)} : ${state.name} -> ${Event.eventTypeName(transition)}"
  }
}


/**
 * Companion object to the Event type with event type definition and utility methods.
 */
object Event {

  // NOTE: The reason why we are not using Scala Enums is that they are currently not
  //       recognized by Flink's serialization stack (pending issue) and would be handled
  //       as objects of a custom dynamic class hierarchy. That is vastly less efficient 
  //       than numbers, so we map the types to integers right now
  // 
  // NOTE:  Using a Java enum is efficiently supported right now
  
  type EventType = Int
  
  val a : EventType = 1
  val b : EventType = 2
  val c : EventType = 3
  val d : EventType = 4
  val e : EventType = 5
  val f : EventType = 6
  val g : EventType = 7

  /**
   * Util method to encode the type name. Part of the workaround to use integers rather
   * than an enumeration type.
   * 
   * @param evt The event type.
   * @return The string name of the event type
   */
  def eventTypeName(evt: EventType): String = {
    String.valueOf(('a' + evt - 1).asInstanceOf[Char])
  }

  /**
   * Util method to create a string representation of a 32 bit integer representing
   * an IPv4 address.
   * 
   * @param address The address, MSB first.
   * @return The IP address string.
   */
  def formatAddress(address: Int): String = {
    val b1 = (address >>> 24) & 0xff
    val b2 = (address >>> 16) & 0xff
    val b3 = (address >>>  8) & 0xff
    val b4 =  address         & 0xff
    
    s"$b1.$b2.$b3.$b4"
  }
}
