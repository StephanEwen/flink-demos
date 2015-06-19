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

import Event._

//
//   The State Machine implemented by the classes in this file.
// 
//
//           +--<a>--> W --<b>--> Y --<e>---+
//           |                    ^         |     +-----<g>---> TERM
//   INITIAL-+                    |         |     |
//           |                    |         +--> (Z)---<f>-+
//           +--<c>--> X --<b>----+         |     ^        |
//                     |                    |     |        |
//                     +--------<d>---------+     +--------+
//

/**
 * A possible transition on a given event into a target state. The transition
 * belongs to its originating state and has an associated probability that is
 * used to generate random transition events.
 * 
 * @param event The event that triggers the transition.
 * @param targetState The target state after the transition.
 * @param prob The probability of the transition.
 */
case class Transition(event: EventType, targetState: State, prob: Float)


/**
 * Base class for states in the state machine.
 * 
 * @param name The name of the state.
 * @param transitions The transitions from this state into other states on events. May be empty,
 *                    in which case this state is a terminal state.
 */
sealed class State (val name: String,
                    private val transitions: Transition*) extends java.io.Serializable {
  /**
   * Returns the name of the state.
   * @return The name of the state.
   */
  override def toString: String = name

  /**
   * Checks if this state is a terminal state.
   * @return True if this state is a terminal state, false otherwise.
   */
  def terminal: Boolean = transitions.isEmpty

  /**
   * Gets the state after transitioning from this state based on the given event.
   * If the transition is valid, this returns the new state, and if this transition
   * is illegal, it returns [[InvalidTransition]].
   * 
   * @param evt The event that defined the transition.
   * @return The new state, or [[InvalidTransition]].
   */
  def transition(evt: EventType): State =
    transitions
      .find( transition => transition.event == evt )
      .map(_.targetState)
      .getOrElse(InvalidTransition)

  /**
   * Picks a random transition, based on the probabilities of the outgoing transitions
   * of this state.
   * 
   * @param rnd The random number generator to use.
   * @return A pair ot (transition event , new state).
   */
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

  /**
   * Returns an event type that, if applied as a transition on this state, will result
   * in an illegal state transition.
   *         
   * @param rnd The random number generator to use.
   * @return And event type for an illegal state transition.
   */
  def randomInvalidTransition(rnd: Random): EventType = {
    var value: EventType = -1
    
    while (value == -1) {
      val candidate = rnd.nextInt(g + 1)
      value = if (transition(candidate) == InvalidTransition) candidate else -1
    }
    value
  }
}

/**
 * The initial state from which all state sequences start.
 */
object InitialState extends State("Initial", Transition(a, W, 0.6f), Transition(c, X, 0.4f))

object W extends State("W", Transition(b, Y, 1.0f))

object X extends State("X", Transition(b, Y, 0.2f), Transition(d, Z, 0.8f))

object Y extends State("Y", Transition(e, Z, 1.0f))

object Z extends State("Z", Transition(g, TerminalState, 1.0f) )

/**
 * The terminal state in the state machine.
 */
object TerminalState extends State("Terminal")

/**
 * Special state returned by the State.transition(...) function when attempting
 * an illegal state transition.
 */
object InvalidTransition extends State("Invalid Transition")