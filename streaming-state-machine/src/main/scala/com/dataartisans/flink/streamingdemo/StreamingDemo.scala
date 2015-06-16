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

import java.nio.{ByteOrder, ByteBuffer}
import java.util.Properties

import _root_.kafka.consumer.ConsumerConfig

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.checkpoint.Checkpointed
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.api.persistent.PersistentKafkaSource
import org.apache.flink.streaming.util.serialization.DeserializationSchema
import org.apache.flink.util.Collector

import scala.collection.mutable


object StreamingDemo {

  def main(args: Array[String]): Unit = {
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.enableCheckpointing(1000)
    
    val sampleStream = env.addSource(new EventsGeneratorSource())
    
//    // Kafka
//    val props = new Properties()
//    props.put("zookeeper.connect", "localhost:2181")
//    props.put("group.id", "flink-demo-group")
//    props.put("auto.commit.enable", "false")
//    
//    val kafkaStream = env.addSource(new PersistentKafkaSource[Event]("flink-demo-topic", 
//                                                                     new EventDeserializer(),
//                                                                     new ConsumerConfig(props)))
    sampleStream
        .partitionByHash("sourceAddress")
        .flatMap(new StateMachineMapper())
        .print()
    
    env.execute()
  }
}

// ----------------------------------------------------------------------------
//  State machine evaluation
// ----------------------------------------------------------------------------

class StateMachineMapper extends FlatMapFunction[Event, Alert] with Checkpointed[mutable.HashMap[Int, State]] {
  
  private[this] val states = new mutable.HashMap[Int, State]()
  
  override def flatMap(t: Event, out: Collector[Alert]): Unit = {
    
    // get and remove the current state
    val state = states.remove(t.sourceAddress).getOrElse(InitialState)
    
    val nextState = state.transition(t.event)
    if (nextState == InvalidTransition) {
      out.collect(Alert(t.sourceAddress, state, t.event))
    } 
    else if (!nextState.terminal) {
      states.put(t.sourceAddress, nextState)
    }
  }

  override def snapshotState(l: Long, l1: Long): mutable.HashMap[Int, State] = {
    states
  }

  override def restoreState(t: mutable.HashMap[Int, State]): Unit = {
    states ++= t
  }
}

// ----------------------------------------------------------------------------
//  Event deserializer
// ----------------------------------------------------------------------------

class EventDeserializer extends DeserializationSchema[Event] {
  
  override def deserialize(bytes: Array[Byte]): Event = {
    if (bytes.length == 8) {
      val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
      val address: Int = buffer.getInt(0)
      val eventType: Int = buffer.getInt(4)
      Event(address, eventType)
    }
    else {
      throw new IllegalArgumentException()
    }
  }

  override def isEndOfStream(t: Event): Boolean = false

  override def getProducedType: TypeInformation[Event] = {
    createTypeInformation[Event]
  }
}
