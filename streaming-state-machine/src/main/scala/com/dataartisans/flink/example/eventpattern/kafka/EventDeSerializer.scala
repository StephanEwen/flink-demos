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

package com.dataartisans.flink.example.eventpattern.kafka

import java.nio.{ByteBuffer, ByteOrder}

import com.dataartisans.flink.example.eventpattern.Event
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

/**
 * A serializer / Deserializer for converting [[Event]] objects from/to byte sequences
 * for Kafka.
 */
class EventDeSerializer extends DeserializationSchema[Event] with SerializationSchema[Event, Array[Byte]] {
  
  override def deserialize(bytes: Array[Byte]): Event = {
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    val address: Int = buffer.getInt(0)
    val eventType: Int = buffer.getInt(4)
    Event(address, eventType)
  }

  override def serialize(t: Event): Array[Byte] = {
    val byteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
    byteBuffer.putInt(0, t.sourceAddress)
    byteBuffer.putInt(4, t.event)
    byteBuffer.array()
  }

  override def isEndOfStream(t: Event): Boolean = false

  override def getProducedType: TypeInformation[Event] = {
    createTypeInformation[Event]
  }
}
