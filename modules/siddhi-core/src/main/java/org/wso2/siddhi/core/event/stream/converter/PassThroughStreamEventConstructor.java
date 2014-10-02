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
package org.wso2.siddhi.core.event.stream.converter;

import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;

public class PassThroughStreamEventConstructor extends EventConstructor {

    public PassThroughStreamEventConstructor(StreamEventPool streamEventPool) {
        super.streamEventPool = streamEventPool;
    }

    protected StreamEvent constructStreamEvent(Object[] data, boolean isExpected, long timestamp) {
        StreamEvent streamEvent = streamEventPool.borrowEvent();
        System.arraycopy(data, 0, streamEvent.getOutputData(), 0, data.length);
        streamEvent.setExpired(isExpected);
        streamEvent.setTimestamp(timestamp);
        streamEvent.setTimerEvent(false);

        return streamEvent;
    }
}
