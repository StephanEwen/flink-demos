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

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Properties, UUID}

import com.dataartisans.flink.example.eventpattern.kafka.EventDeSerializer

import org.apache.flink.api.common.functions.{RuntimeContext, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{IndexRequestBuilder, ElasticsearchSink}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.util.Collector

import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
 * Demo streaming program that receives (or generates) a stream of events and evaluates
 * a state machine (per originating IP address) to validate that the events follow
 * the state machine's rules.
 */
object StreamingDemo {

  def main(args: Array[String]): Unit = {
    
    // create the environment to create streams and configure execution
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    
    // data stream from kafka topic.
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", "localhost:2181")
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("group.id", UUID.randomUUID().toString)
    kafkaProps.setProperty("auto.commit.enable", "false")
    kafkaProps.setProperty("auto.offset.reset", "largest")

    val elasticConfig = new java.util.HashMap[String, String]
    elasticConfig.put("bulk.flush.max.actions", "1")
    elasticConfig.put("cluster.name", "elasticsearch")
    
    
    val stream = env.addSource(new FlinkKafkaConsumer08[Event](
                                     "flink-demo-topic-1", new EventDeSerializer(), kafkaProps))
    val alerts = stream
      // partition on the address to make sure equal addresses
      // end up in the same state machine flatMap function
      .keyBy("sourceAddress")
      
      // the function that evaluates the state machine over the sequence of events
      .flatMap(new StateMachineMapper())

    
      alerts.print()
    
      alerts.addSink(new ElasticsearchSink[Alert](elasticConfig, new IndexRequestBuilder[Alert]() {
        
          override def createIndexRequest(element: Alert, ctx: RuntimeContext): IndexRequest = {
            
            val now: AnyRef = System.currentTimeMillis().asInstanceOf[AnyRef]
            
            val json = new util.HashMap[String, AnyRef]()
            json.put("message", element.toString)
            json.put("time", now)

            Requests.indexRequest()
              .index("alerts-idx")
              .`type`("numalerts")
              .source(json)
          }
      }))
//      // output to standard-out
//      .print()
    
    // trigger program execution
    env.execute()
  }
}

/**
 * The function that maintains the per-IP-address state machines and verifies that the
 * events are consistent with the current state of the state machine. If the event is not
 * consistent with the current state, the function produces an alert.
 */
class StateMachineMapper extends RichFlatMapFunction[Event, Alert] {
  
  private[this] var currentState: ValueState[State] = _
    
  override def open(config: Configuration): Unit = {
    currentState = getRuntimeContext.getState(
      new ValueStateDescriptor("state", classOf[State], InitialState))
  }
  
  override def flatMap(t: Event, out: Collector[Alert]): Unit = {
    val state = currentState.value()
    val nextState = state.transition(t.event)
    
    nextState match {
      case InvalidTransition =>
        out.collect(Alert(t.sourceAddress, state, t.event))
      case x if x.terminal =>
        currentState.clear()
      case x =>
        currentState.update(nextState)
    }
  }
}
