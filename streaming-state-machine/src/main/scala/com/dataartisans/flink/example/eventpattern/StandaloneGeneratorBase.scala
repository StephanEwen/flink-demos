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

import org.apache.flink.util.Collector

/**
 * Base for standalone generators that use the state machine to create event
 * sequences and push them for example into Kafka.
 */
abstract class StandaloneGeneratorBase {
  
  def runGenerator(collectors: Array[_ <: Collector[Event]]): Unit = {

    val threads = new Array[GeneratorThread](collectors.length)
    val range = Integer.MAX_VALUE / collectors.length
    
    // create the generator threads
    for (i <- 0 until threads.length) {

      val min = range * i
      val max = min + range
      val thread = new GeneratorThread(collectors(i), min, max)
      
      threads(i) = thread
      thread.setName("Generator " + i)
    }
    
    var delay: Long = 2L
    var nextErroneous: Int = 0
    var running: Boolean = true
    
    threads.foreach( _.setDelay(delay) )
    threads.foreach( _.start() )
    
    val throughputLogger = new ThroughputLogger(threads)
    throughputLogger.start()
    
    println("Commands:")
    println(" -> q : Quit")
    println(" -> + : increase latency")
    println(" -> - : decrease latency")
    println(" -> e : inject invalid state transition")

    // input loop
    
    while (running) {
      val next: Int = System.in.read()
      
      next match {
        case 'q' =>
          println("Quitting...")
          running = false
        
        case 'e' =>
          println("Injecting erroneous transition ...")
          threads(nextErroneous).sendInvalidStateTransition()
          nextErroneous = (nextErroneous + 1) % threads.length

        case '+' =>
          delay = Math.max(delay * 2, 1)
          println("Delay is " + delay)
          threads.foreach( _.setDelay(delay) )

        case '-' =>
          delay /= 2
          println("Delay is " + delay)
          threads.foreach( _.setDelay(delay) )

        case _ =>
      }
    }
    
    // shutdown
    throughputLogger.shutdown()
    threads.foreach( _.shutdown() )
    threads.foreach( _.join() )
  }  
}



/**
 * A thread running a [[EventsGenerator]] and pushes generated events to the given collector
 * (such as Kafka / Socket / ...)
 * 
 * @param out The collector to push the generated records to. 
 * @param minAddress The lower bound for the range from which a new IP address may be picked.
 * @param maxAddress The upper bound for the range from which a new IP address may be picked.
 */
class GeneratorThread(private[this] val out: Collector[Event],
                      private[this] val minAddress: Int,
                      private[this] val maxAddress: Int) extends Thread {
  
  private[this] var runningThread: Thread = _
  
  private[this] var delay: Long = 0
  
  private[this] var count: Long = 0
  
  private[this] var running: Boolean = true
  
  private[this] var injectInvalid: Boolean = false
  
  
  override def run(): Unit = {
    runningThread = Thread.currentThread()
    val generator = new EventsGenerator()
    
    while (running) {
      if (injectInvalid) {
        injectInvalid = false
        generator.nextInvalid() match {
          case Some(evt) => out.collect(evt)
          case None =>
        }
      }
      else {
        out.collect(generator.next(minAddress, maxAddress))
      }
      
      count += 1
      
      // sleep the delay to throttle
      if (delay > 0) {
        try {
          Thread.sleep(delay)
        }
        catch {
          case e: InterruptedException =>
        }
      }
    }
  }
  
  def currentCount: Long = count
  
  def shutdown(): Unit = {
    running = false
    
    if (runningThread != null) {
      runningThread.interrupt()
    }
  }
  
  def setDelay(delay: Long): Unit = {
    this.delay = delay
  }
  
  def sendInvalidStateTransition(): Unit = {
    this.injectInvalid = true
  }
}

/**
 * Thread that periodically print the number of elements generated per second.
 * 
 * @param generators The generator threads whose aggregate throughput should be logged.
 */
class ThroughputLogger(private[this] val generators: Array[GeneratorThread]) extends Thread {
  
  private[this] var running = true
  
  override def run(): Unit = {
    var lastCount: Long = 0L
    var lastTimeStamp: Long = System.currentTimeMillis()

    while (running) {
      Thread.sleep(1000)

      val ts = System.currentTimeMillis()
      val currCount: Long = generators.foldLeft(0L)( (v, gen) => v + gen.currentCount)
      val factor: Double = (ts - lastTimeStamp) / 1000
      val perSec: Double = (currCount - lastCount) / factor
      lastTimeStamp = ts
      lastCount = currCount

      System.out.println(perSec + " / sec")
    }
  }

  def shutdown(): Unit = {
    running = false
  }
}
