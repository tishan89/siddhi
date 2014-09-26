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

package org.wso2.siddhi.core.stream.input;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.StreamJunction;

public class InputHandler {

    private String streamId;
    private StreamJunction.Publisher publisher;
    private StreamJunction.Publisher pausedPublisher;

    public InputHandler(String streamId, StreamJunction streamJunction) {
        this.streamId = streamId;
        this.publisher = streamJunction.constructPublisher();
        this.pausedPublisher=publisher;
    }

    public String getStreamId() {
        return streamId;
    }

    public void send(Object[] data) throws InterruptedException {
        if (publisher != null) {
            publisher.send(System.currentTimeMillis(), data);
        }
    }

    public void send(long timeStamp, Object[] data) throws InterruptedException {
        if (publisher != null) {
            publisher.send(timeStamp, data);
        }
    }

    public void send(Event event) throws InterruptedException {
        if (publisher != null) {
            publisher.send(event);
        }
    }

    public void send(Event[] events) throws InterruptedException {
        if (publisher != null) {
            for (Event event : events) {
                publisher.send(event);
            }
        }
    }

    protected void sendTimerEvent(long timeStamp) {
        publisher.send(timeStamp, null);     //null to distinguish timer event
    }

    void disconnect() {
        this.publisher = null;
    }

    void resume() {
        this.publisher = pausedPublisher;
    }
}
