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

package org.wso2.siddhi.core;

import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.exception.DefinitionNotExistException;
import org.wso2.siddhi.core.exception.DifferentDefinitionAlreadyExistException;
import org.wso2.siddhi.core.exception.QueryNotExistException;
import org.wso2.siddhi.core.partition.PartitionRuntime;
import org.wso2.siddhi.core.query.QueryRuntime;
import org.wso2.siddhi.core.query.output.callback.InsertIntoStreamCallback;
import org.wso2.siddhi.core.query.output.callback.OutputCallback;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.QueryStreamReceiver;
import org.wso2.siddhi.core.stream.StreamJunction;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.input.InputHandlerManager;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.stream.runtime.SingleStreamRuntime;
import org.wso2.siddhi.core.stream.runtime.StreamRuntime;
import org.wso2.siddhi.core.util.EventTimer;
import org.wso2.siddhi.core.util.parser.OutputParser;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

/**
 * keep streamDefinitions, partitionRuntimes, queryRuntimes of an executionPlan
 * and streamJunctions and inputHandlers used
 */
public class ExecutionPlanRuntime {
    private ConcurrentMap<String, AbstractDefinition> streamDefinitionMap = new ConcurrentHashMap<String, AbstractDefinition>(); //contains stream definition
    private InputHandlerManager inputHandlerManager = new InputHandlerManager();
    private ConcurrentMap<String, QueryRuntime> queryProcessorMap = new ConcurrentHashMap<String, QueryRuntime>();
    private ConcurrentMap<String, StreamJunction> streamJunctionMap = new ConcurrentHashMap<String, StreamJunction>(); //contains stream junctions
    private ConcurrentMap<String, PartitionRuntime> partitionMap = new ConcurrentHashMap<String, PartitionRuntime>(); //contains partitions
    private SiddhiContext siddhiContext;
    private EventTimer eventTimer = new EventTimer();                                          //new event timer with default configs

    public ExecutionPlanRuntime(SiddhiContext siddhiContext) {
        this.siddhiContext = siddhiContext;
    }

    public void defineStream(StreamDefinition streamDefinition) {
        validateStreamDefinition(streamDefinition);
        //InputHandler inputHandler = inputHandlerManager.getInputHandler(streamDefinition.getId());
        if (!streamDefinitionMap.containsKey(streamDefinition.getId())) {
            streamDefinitionMap.put(streamDefinition.getId(), streamDefinition);
            StreamJunction streamJunction = streamJunctionMap.get(streamDefinition.getId());
            if (streamJunction == null) {
                streamJunction = new StreamJunction(streamDefinition, (ExecutorService) siddhiContext.getExecutorService(),
                        siddhiContext.getDefaultEventBufferSize());
                streamJunctionMap.put(streamDefinition.getId(), streamJunction);
            }
            //inputHandler = new InputHandler(streamDefinition.getId(), streamJunction);
            //return inputHandlerManager.setIfAbsentInputHandler(streamDefinition.getId(), inputHandler);
       /* } else {
            return inputHandler;
        }*/
        }
    }

    private void validateStreamDefinition(StreamDefinition streamDefinition) {
        if (streamDefinitionMap.containsKey(streamDefinition.getId()) && !(streamDefinitionMap.get(streamDefinition.getId())
                .equalsIgnoreAnnotations(streamDefinition))) {
            throw new DifferentDefinitionAlreadyExistException("Different stream definition same as output stream definition is already " +
                    "exist under stream name " + streamDefinition.getId());
        }
    }

    public void addPartition(PartitionRuntime partitionRuntime) {
        partitionMap.put(partitionRuntime.getPartitionId(), partitionRuntime);
    }

    public String addQuery(QueryRuntime queryRuntime) {
        queryProcessorMap.put(queryRuntime.getQueryId(), queryRuntime);
        StreamRuntime streamRuntime = queryRuntime.getStreamRuntime();
        if (streamRuntime instanceof SingleStreamRuntime) {
            QueryStreamReceiver queryStreamReceiver = ((SingleStreamRuntime) streamRuntime).getQueryStreamReceiver();
            streamJunctionMap.get(queryStreamReceiver.getStreamId()).subscribe(queryStreamReceiver);
        }//TODO: for join

        OutputCallback outputCallback = OutputParser.constructOutputCallback(queryRuntime.getQuery().getOutputStream(),
                streamJunctionMap, queryRuntime.getOutputStreamDefinition(), siddhiContext);
        queryRuntime.setOutputCallback(outputCallback);
        if (outputCallback != null && outputCallback instanceof InsertIntoStreamCallback) {
            defineStream(((InsertIntoStreamCallback) outputCallback).getOutputStreamDefinition());
        }
        return queryRuntime.getQueryId();
    }

    public void addCallback(String streamId, StreamCallback streamCallback) {
        streamCallback.setStreamId(streamId);
        StreamJunction streamJunction = streamJunctionMap.get(streamId);
        if (streamJunction == null) {
            streamJunction = new StreamJunction((StreamDefinition) streamDefinitionMap.get(streamId), (
                    ExecutorService) siddhiContext.getExecutorService(), siddhiContext.getDefaultEventBufferSize());
            streamJunctionMap.put(streamId, streamJunction);
        }
        streamJunction.subscribe(streamCallback);
    }

    public void addCallback(String queryName, QueryCallback callback) {
        callback.setContext(siddhiContext);
        QueryRuntime queryRuntime = queryProcessorMap.get(queryName);
        callback.setQuery(queryRuntime.getQuery());
        if (queryRuntime == null) {
            throw new QueryNotExistException("No query fund for " + queryName);
        }
        queryRuntime.addCallback(callback);
    }

    public InputHandler getInputHandler(String streamId) {
        InputHandler inputHandler = inputHandlerManager.getInputHandler(streamId);
        if (inputHandler == null) {
            StreamJunction streamJunction = streamJunctionMap.get(streamId);
            if (streamJunction == null) {
                StreamDefinition definition = (StreamDefinition) streamDefinitionMap.get(streamId);
                if (definition != null) {
                    streamJunction = new StreamJunction(definition, (ExecutorService) siddhiContext.getExecutorService(),
                            siddhiContext.getDefaultEventBufferSize());
                    streamJunctionMap.put(definition.getId(), streamJunction);
                } else {
                    throw new DefinitionNotExistException("Definition with ID " + streamId + " does not exist. Hence can not create input handler");
                }
            }
            inputHandler = new InputHandler(streamId, streamJunction);
            eventTimer.addInputHandler(inputHandler);
            return inputHandlerManager.setIfAbsentInputHandler(streamId, inputHandler);
        } else {
            return inputHandler;
        }
    }

    public void addQueryRuntime(QueryRuntime queryRuntime) {
        queryProcessorMap.put(queryRuntime.getQueryId(), queryRuntime);
    }

    public ConcurrentMap<String, StreamJunction> getStreamJunctions() {
        return streamJunctionMap;
    }

    public ConcurrentMap<String, AbstractDefinition> getStreamDefinitionMap() {
        return streamDefinitionMap;
    }

    public void setTimerFrequency(int timerFrequency) {
        this.eventTimer.setTimerFrequency(timerFrequency);
    }

    public void shutdown() {
        inputHandlerManager.stopProcessing();
        for (StreamJunction streamJunction : streamJunctionMap.values()) {
            streamJunction.stopProcessing();
        }
        eventTimer.shutdown();
    }
}
