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

import java.util.Random

/**
 * A generator for events. The generator internally maintains a series of state
 * machines (addresses and current associated state) and returns transition events
 * from those state machines. Each time the next event is generators, this
 * generator picks a random state machine and creates a random transition on that
 * state machine.
 * 
 * The generator randomly adds new state machines, and removes state machines as
 * soon as they reach the terminal state. This implementation maintains up to
 * 1000 state machines concurrently.
 */
class EventsGenerator {
  
  /** Probability with this generator generates an illegal state transition */
  private[this] val errorProb: Double = 0.0000001

  /** The random number generator */
  private[this] val rnd = new Random()

  /** The currently active state machines */
  private[this] val states = new java.util.LinkedHashMap[Int, State]()

  /**
   * Creates a new random event. This method randomly pick either
   * one of its currently running state machines, or start a new state machine for
   * a random IP address.
   * 
   * With [[errorProb]] probability, the generated event will be from an illegal state
   * transition of one of the currently running state machines.
   * 
   * @param minIp The lower bound for the range from which a new IP address may be picked.
   * @param maxIp The upper bound for the range from which a new IP address may be picked.
   * @return A next random 
   */
  def next(minIp: Int, maxIp: Int): Event = {
    
    val p = rnd.nextDouble()
    
    if (p * 1000 >= states.size()) {
      // create a new state machine
      val nextIP = rnd.nextInt(maxIp - minIp) + minIp
      
      if (!states.containsKey(nextIP)) {
        val (transition, state) = InitialState.randomTransition(rnd)  
        states.put(nextIP, state)
        Event(nextIP, transition)
      }
      else {
        // collision on IP address, try again
        next(minIp, maxIp)
      }
    }
    else {
      // pick an existing state machine
      
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
      
      if (p < errorProb) {
        val event = currentState.randomInvalidTransition(rnd)
        Event(address, event)
      }
      else {
        val (event, newState) = currentState.randomTransition(rnd)
        if (!newState.terminal) {
          // reinsert
          states.put(address, newState)
        }
        
        Event(address, event)
      }
    }
  }

  /**
   * Creates an event for an illegal state transition of one of the internal
   * state machines. If the generator has not yet started any state machines
   * (for example, because no call to [[next(Int, Int)]] was made, yet), this
   * will return [[None]].
   * 
   * @return An event for a illegal state transition, or [[None]], if not possible.
   */
  def nextInvalid(): Option[Event] = {
    val iter = states.entrySet().iterator()
    if (iter.hasNext) {
      val entry = iter.next()
      val address = entry.getKey
      val currentState = entry.getValue
      iter.remove()

      val event = currentState.randomInvalidTransition(rnd)
      Some(Event(address, event))
    }
    else {
      None
    }
  }
  
  def numActiveEntries: Int = states.size()
}
