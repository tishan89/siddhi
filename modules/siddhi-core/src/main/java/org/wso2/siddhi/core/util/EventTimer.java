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
package org.wso2.siddhi.core.util;

import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.input.TimerInputStreamHandler;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class EventTimer {

    private TimerInputStreamHandler timerInputStreamHandler = new TimerInputStreamHandler();
    private ScheduledExecutorService scheduledExecutorService;
    private int timerFrequency = SiddhiConstants.DEFAULT_TIMER_FREQUENCY;

    public void startTimerEvent() {
        scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
        scheduledExecutorService.scheduleAtFixedRate(timerInputStreamHandler, timerFrequency, timerFrequency, TimeUnit.MILLISECONDS);
    }

    public void addInputHandler(InputHandler inputHandler) {
        timerInputStreamHandler.addInputHandler(inputHandler);
    }

    public int getTimerFrequency() {
        return timerFrequency;
    }

    public void setTimerFrequency(int timerFrequency) {
        this.timerFrequency = timerFrequency;
    }

    public void shutdown() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
        }
    }
}
