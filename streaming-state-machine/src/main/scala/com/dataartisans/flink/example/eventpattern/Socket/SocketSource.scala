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

package com.dataartisans.flink.example.eventpattern.Socket

import java.net.{InetAddress, InetSocketAddress}
import java.nio.channels.ServerSocketChannel
import java.nio.{ByteBuffer, ByteOrder}
import com.dataartisans.flink.example.eventpattern.Event
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
 * Flink stream source that reads the Event class elements from a socket.
 * It open a server socket and waits for the generator to connect and push data.
 */
class SocketSource extends RichParallelSourceFunction[Event] {
  
  private[this] var running = true
  
  override def run(sourceContext: SourceContext[Event]): Unit = {
    
    val port = SocketGenerator.BASE_PORT + getRuntimeContext.getIndexOfThisSubtask()
    
    val server = ServerSocketChannel.open()
    server.configureBlocking(true)
    server.bind(new InetSocketAddress(InetAddress.getByName("localhost"), port))
    
    val channel = server.accept()
    
    val buffer = ByteBuffer.allocateDirect(4096)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    
    while (running) {
      channel.read(buffer)
      buffer.flip()
      
      while (running && buffer.remaining() >= 8) {
        val address = buffer.getInt()
        val event = buffer.getInt()
        
        sourceContext.collect(Event(address, event))
      }
      buffer.compact()
    }
    
    channel.close()
  }

  override def cancel(): Unit = {
    running = false
  }
}
