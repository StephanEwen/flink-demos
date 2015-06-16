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

import EventType._

// ----------------------------------------------------------------------------
//   The State Machine
// ----------------------------------------------------------------------------
//
//           +--<a>--> W --<b>--> Y --<e>---+
//           |                    ^         |     +-----<g>---> TERM
//   INITIAL-+                    |         |     |
//           |                    |         +--> (Z)---<f>-+
//           +--<c>--> X --<b>----+         |     ^        |
//                     |                    |     |        |
//                     +--------<d>---------+     +--------+
//

case class Transition(event: EventType,
                      targetState: State,
                      prob: Float)


sealed class State (val name: String,
                    val terminal: Boolean,
                    private val transitions: Transition*) extends java.io.Serializable {
  
  override def toString: String = name

  def transition(evt: EventType): State =
    transitions
      .find( transition => transition.event == evt )
      .map(_.targetState)
      .getOrElse(InvalidTransition)

  // to use the state machine as a generator
  
  def randomTransition(rnd: Random): (EventType, State) = {
    if (transitions.isEmpty) {
      throw new RuntimeException("Cannot transition from state " + name)
    }
    else {
      val p = rnd.nextFloat()
      var mass = 0.0f
      var transition: Transition = null
      transitions.foreach { trans => 
        mass += trans.prob
        if (transition == null && p <= mass) {
          transition = trans
        }
      }
      (transition.event, transition.targetState)
    }
  }
}

object InitialState extends State("Initial", false, Transition(a, W, 0.6f), Transition(c, X, 0.4f))

object W extends State("W", false, Transition(b, Y, 1.0f))

object X extends State("X", false, Transition(b, Y, 0.2f), Transition(d, Z, 0.8f))

object Y extends State("Y", false, Transition(e, Z, 1.0f))

object Z extends State("Z", false, Transition(g, TerminalState, 1.0f) )

object TerminalState extends State( "Terminal", true)

object InvalidTransition extends State("Invalid Transition", true)