/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mythingy.streaming;

import java.text.SimpleDateFormat;
import java.util.UUID;

public class ProgressEvent {

    public String eventType;

    public String imageName;

    public long timestamp;

    public UUID instanceId;
    
    
    public ProgressEvent() {
    }

    public ProgressEvent(String eventType, String imageName, long timestamp, UUID instanceId) {
        this.eventType = eventType;
        this.imageName = imageName;
        this.timestamp = timestamp;
        this.instanceId = instanceId;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && getClass() == o.getClass()) {
            ProgressEvent that = (ProgressEvent) o;

            return timestamp == that.timestamp &&
                    eventType.equals(that.eventType) &&
                    imageName.equals(that.imageName) &&
                    instanceId.equals(that.instanceId);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = eventType != null ? eventType.hashCode() : 0;
        result = 31 * result + (imageName != null ? imageName.hashCode() : 0);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (instanceId != null ? instanceId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String formattedTimestamp = format.format(timestamp);

        return "{\n" +
                "  \"event_type\": \"" + eventType + "\"\n," +
                "  \"image_name\": \"" + imageName + "\"\n," +
                "  \"timestamp\": \"" + formattedTimestamp + "\"\n," +
                "  \"instance_id\": \"" + instanceId + "\"\n" +
                "}";
    }
}
