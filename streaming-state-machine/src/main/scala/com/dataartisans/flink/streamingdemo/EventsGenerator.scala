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

import java.util.Random
import EventType.EventType

class EventsGenerator {

  private[this] val rnd = new Random()

  private[this] val states = new java.util.LinkedHashMap[Int, State]()
  
  def next(minIp: Int, maxIp: Int): Event = {
    val p = rnd.nextFloat()
    
    if (p * 1000 >= states.size()) {
      // create a new originating address
      val nextIP = rnd.nextInt(maxIp - minIp) + minIp
      val transition: (EventType, State) = InitialState.randomTransition(rnd)
      
      if (!states.containsKey(nextIP)) {
        states.put(nextIP, transition._2)
        Event(nextIP, transition._1)
      }
      else {
        next(minIp, maxIp)
      }
    }
    else {
      
      // skip over some elements in the linked map, then take the next
      // update it, and insert it at the end
      
      val numToSkip = Math.min(20, rnd.nextInt(states.size()))
      val iter = states.entrySet().iterator()
      var i = 0
      while (i < numToSkip) {
        i += 1
        iter.next()
      }
      
      val entry = iter.next()
      val address = entry.getKey
      val currentState = entry.getValue
      iter.remove()
      
      val (event, newState) = currentState.randomTransition(rnd)
      if (!newState.terminal) {
        // reinsert
        states.put(address, newState)
      }
      
      Event(address, event)
    }
  }
  
  
  def numActiveEntries: Int = states.size()
}

object EventsGenerator {
  
  def main(args: Array[String]): Unit = {
    val generator = new EventsGenerator()
    
    while (true) {
      val nextEvent = generator.next(1, Integer.MAX_VALUE)
      System.out.println("(" + generator.numActiveEntries + ") - " + nextEvent)
    }
  }
}