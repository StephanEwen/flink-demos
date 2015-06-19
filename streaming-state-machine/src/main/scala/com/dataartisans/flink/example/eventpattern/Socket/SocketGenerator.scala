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
import java.nio.{ByteOrder, ByteBuffer}
import java.nio.channels.SocketChannel

import com.dataartisans.flink.example.eventpattern.{StandaloneGeneratorBase, Event}
import org.apache.flink.util.Collector

object SocketGenerator extends StandaloneGeneratorBase {

  val BASE_PORT = 51762

  def main(args: Array[String]): Unit = {

    val numPartitions = 4 //args(0).toInt
    val collectors = new Array[SocketCollector](numPartitions)

    // create the generator threads
    for (i <- 0 until collectors.length) {
      collectors(i) = new SocketCollector(BASE_PORT + i)
    }

    runGenerator(collectors)
  }
}

class SocketCollector(val port: Int) extends Collector[Event] {

  val channel = SocketChannel.open(new InetSocketAddress(InetAddress.getByName("localhost"), port))
  channel.configureBlocking(true)
  channel.finishConnect()

  val buffer = ByteBuffer.allocateDirect(4096).order(ByteOrder.LITTLE_ENDIAN)

  override def collect(t: Event): Unit = {
    if (buffer.remaining() < 8) {
      buffer.flip()
      channel.write(buffer)
      buffer.clear()
    }

    buffer.putInt(t.sourceAddress)
    buffer.putInt(t.event)
  }

  override def close(): Unit = {
    if (buffer.position() > 0) {
      buffer.flip()
      channel.write(buffer)
    }
    channel.close()
  }
}
