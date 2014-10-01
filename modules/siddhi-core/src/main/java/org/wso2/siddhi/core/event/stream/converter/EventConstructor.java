package org.wso2.siddhi.core.event.stream.converter;
/*
 * Copyright (c) 2005 - 2014, WSO2 Inc. (http://www.wso2.org)
 * All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;

public abstract class EventConstructor {
    protected StreamEventPool streamEventPool;

    /**
     * Method where real construction happens. Every child class should implement this appropriately.
     *
     * @param data
     * @param isExpected
     * @param timestamp
     * @return
     */
    protected abstract StreamEvent constructStreamEvent(Object[] data, boolean isExpected, long timestamp);

    /**
     * Method to construct StreamEvent form Event
     *
     * @param event Event to be converted
     * @return constructed StreamEvent
     */
    public StreamEvent constructStreamEvent(Event event) {
        if (event.isTimerEvent()) {
            return new StreamEvent(event.getTimestamp());
        } else {
            return constructStreamEvent(event.getData(), event.isExpired(), event.getTimestamp());
        }
    }

    /**
     * Method to construct(change format) new StreamEvent from StreamEvent
     *
     * @param streamEvent StreamEvent to be Converted
     * @return constructed StreamEvent
     */
    public StreamEvent constructStreamEvent(StreamEvent streamEvent) {
        if (streamEvent.isTimerEvent()) {
            return streamEvent;
        } else {
            return constructStreamEvent(streamEvent.getOutputData(), streamEvent.isExpired(), streamEvent.getTimestamp());
        }
    }

    /**
     * Method to construct(change format) timeStamp and data from StreamEvent
     *
     * @param timeStamp timeStamp of the event
     * @param data      output data of the event
     * @return constructed StreamEvent
     */
    public StreamEvent constructStreamEvent(long timeStamp, Object[] data) {
        if (data == null) {
            return new StreamEvent(timeStamp);
        } else {
            return constructStreamEvent(data, false, timeStamp);
        }
    }

    /**
     * Return the used event back to the pool
     *
     * @param streamEvent used stream event
     */
    public void returnEvent(StreamEvent streamEvent) {
        streamEventPool.returnEvent(streamEvent);
    }
}
