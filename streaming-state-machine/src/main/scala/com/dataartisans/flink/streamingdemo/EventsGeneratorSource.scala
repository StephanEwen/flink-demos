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

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, ParallelSourceFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

class EventsGeneratorSource extends RichParallelSourceFunction[Event] {
  
  private[this] var running = true
  
  private[this] var count = 0
  
  override def run(sourceContext: SourceContext[Event]): Unit = {
    
    val logger = new Runnable {
      override def run(): Unit = {

        var lastCount = 0
        var lastTimeStamp = System.currentTimeMillis()
        
        while (running) {
          Thread.sleep(1000)
          
          val ts = System.currentTimeMillis()
          val currCount = count
          val factor: Double = (ts - lastTimeStamp) / 1000
          val perSec = (currCount - lastCount) / factor
          lastTimeStamp = ts
          lastCount = currCount
          
          System.out.println(perSec + " / sec")
        }
      }
    }
    
    new Thread(logger).start()
    
    val generator = new EventsGenerator()
    
    val parallelism = getRuntimeContext.getNumberOfParallelSubtasks
    val pos = getRuntimeContext.getIndexOfThisSubtask
    
    val range = Integer.MAX_VALUE / parallelism
    val min = pos * range
    val max = min + range
    
    while (running) {
      sourceContext.collect(generator.next(min, max))
      count += 1
    }
    
    running = false
  }

  override def cancel(): Unit = {
    running = false
  }
}
