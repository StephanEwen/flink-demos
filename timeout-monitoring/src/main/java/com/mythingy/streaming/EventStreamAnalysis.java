/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mythingy.streaming;

import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Properties;


public class EventStreamAnalysis {

    public static final String KAFKA_TOPIC = "vm_event_topic";
    public static final String KAFKA_BROKERS = "localhost:9092";
    public static final String ZOOKEEPER_CONNECTION = "localhost:2181";
    
    public static final long TIMEOUT = 5000;
    
    public static void main(String[] args) throws Exception {
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        env.enableCheckpointing(1000);

        DataStream<ProgressEvent> eventStream = env.addSource(new EventGenerator());

        // variant 1: local generator - nothing extra needed
        
        // variant 2: Kafka as a queue between generator and consumer
//        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", KAFKA_BROKERS);
//        props.setProperty("zookeeper.connect", ZOOKEEPER_CONNECTION);
//        props.setProperty("group.id", "myGroup");
//
//        eventStream.addSink(new FlinkKafkaProducer<>(KAFKA_TOPIC,
//                new ProgressEventJSONSerializer(), props));
//
//        eventStream = env.addSource(new FlinkKafkaConsumer082<>(
//                KAFKA_TOPIC, new ProgressEventJSONSerializer(), props));
        
        // variant 3: RabbitMQ as a queue between event producer and event consumer
        
        
        
        DataStream<Result> results = eventStream
                .keyBy("instanceId")
                .window(GlobalWindows.create())
                .trigger(new TimeoutAndClosingTrigger())
                .apply(new WindowFunction<ProgressEvent, Result, Tuple, GlobalWindow>() {

                    @Override
                    public void apply(Tuple key,
                                      GlobalWindow window,
                                      Iterable<ProgressEvent> events,
                                      Collector<Result> out) {
                        
                        // find the last event, which tells us whether we completed OK or expired
                        ProgressEvent last = null;
                        for (ProgressEvent evt : events) {
                            last = evt;
                        }
                        
                        if (last != null) {
                            boolean expired = !("trove.instance.create".equals(last.eventType));
                            out.collect(new Result(last.instanceId, last.eventType, expired));
                        }
                    }
                });

        results.print();

        env.execute("Timeout Detection");
    }

    // ------------------------------------------------------------------------
    //  The trigger that implements the session logic
    // ------------------------------------------------------------------------
    
    
    private static class TimeoutAndClosingTrigger implements Trigger<ProgressEvent, GlobalWindow> {
        
        @Override
        public TriggerResult onElement(ProgressEvent progressEvent,
                                       long timestamp,
                                       GlobalWindow globalWindow,
                                       TriggerContext triggerContext) throws IOException {
            
            OperatorState<Boolean> inProgress = triggerContext.getKeyValueState("progress_flag", false);
            
            if ("compute.instance.create.start".equals(progressEvent.eventType)) {
                // first event, schedule session timeout and go on
                triggerContext.registerProcessingTimeTimer(progressEvent.timestamp + TIMEOUT);
                
                // mark the window as in progress. this lets us discard late elements later
                inProgress.update(true);
                return TriggerResult.CONTINUE;
            }
            else if (inProgress.value()) {
                // the window is still maintained, so we react to these elements
                
                if ("trove.instance.create".equals(progressEvent.eventType)) {
                    // last event, completed successfully, evaluate and throw away window
                    return TriggerResult.FIRE_AND_PURGE;
                }
                else {
                    // all else, simply go on
                    return TriggerResult.CONTINUE;
                }
            }
            else {
                // window is no longer in progress, throw away late element
                return TriggerResult.PURGE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(long timestamp,
                                              GlobalWindow globalWindow,
                                              TriggerContext triggerContext) throws IOException {
            
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onEventTime(long timestamp,
                                         GlobalWindow globalWindow,
                                         TriggerContext triggerContext) {
            throw new IllegalStateException("Should never have any event time triggers");
        }
    }
}

