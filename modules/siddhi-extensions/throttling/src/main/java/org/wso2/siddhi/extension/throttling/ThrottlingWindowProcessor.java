/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.extension.throttling;

import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

public class ThrottlingWindowProcessor extends WindowProcessor implements Job{

    private ComplexEventChunk<StreamEvent> currentEventChunk;
    private ComplexEventChunk<StreamEvent> expiredEventChunk;
    private Scheduler scheduler;
    private String jobName;
    private final String jobGroup = "CronWindowGroup";
    private String cronString;
    private StreamEvent clonedStreamEvent;
    private Boolean persisted = false;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length == 1) {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                cronString = (String) (((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue());
            } else {
                throw new ExecutionPlanValidationException("Throttling window should have constant parameter " +
                        "attribute but found a dynamic attribute " + attributeExpressionExecutors[0].getClass().getCanonicalName());
            }
        } else if (attributeExpressionExecutors.length == 2) {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                cronString = (String) (((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue());
            } else {
                throw new ExecutionPlanValidationException("Throttling window should have constant parameter " +
                        "attribute but found a dynamic attribute " + attributeExpressionExecutors[0].getClass().getCanonicalName());
            }
            persisted = (Boolean) (((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue());
            currentEventChunk = new ComplexEventChunk<StreamEvent>();
            expiredEventChunk = new ComplexEventChunk<StreamEvent>();

        } else {
            throw new ExecutionPlanValidationException("Throttling window should only have one parameter (string " +
                    "cronExpression), but found " + attributeExpressionExecutors.length + " input attributes");
        }
    }

    @Override
    protected synchronized void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                        StreamEventCloner streamEventCloner) {

        if (persisted) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                currentEventChunk.add(clonedStreamEvent);
            }
        } else {
                StreamEvent streamEvent = streamEventChunk.next();
                clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public void start() {
        scheduleCronJob(cronString, elementId);
    }

    @Override
    public void stop() {
        try {
            scheduler.deleteJob(new JobKey(jobName, jobGroup));
        } catch (SchedulerException e) {
            log.error("Error while removing the cron job for Throttling window: " + e.getMessage(), e);
        }
    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] state) {

    }

    private void scheduleCronJob(String cronString, String elementId) {
        try {
            SchedulerFactory schedulerFactory = new StdSchedulerFactory();
            scheduler = schedulerFactory.getScheduler();
            scheduler.start();

            JobDataMap dataMap = new JobDataMap();
            dataMap.put("windowProcessor", this);

            jobName = "EventRemoverJob_" + elementId;
            JobDetail job = org.quartz.JobBuilder.newJob(ThrottlingWindowProcessor.class)
                    .withIdentity(jobName, jobGroup)
                    .usingJobData(dataMap)
                    .build();

            Trigger trigger = org.quartz.TriggerBuilder.newTrigger()
                    .withIdentity("EventRemoverTrigger_" + elementId, jobGroup)
                    .withSchedule(CronScheduleBuilder.cronSchedule(cronString))
                    .build();

            scheduler.scheduleJob(job, trigger);

        } catch (SchedulerException e) {
            log.error("Error while instantiating quartz scheduler for Throttling window", e);
        }
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        if (log.isDebugEnabled()) {
            log.debug("Running Event Remover Job");
        }

        JobDataMap dataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        ThrottlingWindowProcessor windowProcessor = (ThrottlingWindowProcessor) dataMap.get("windowProcessor");
        windowProcessor.dispatchEvents();
    }

    private synchronized void dispatchEvents() {

        ComplexEventChunk<StreamEvent> resetChunk = new ComplexEventChunk<StreamEvent>();
        if (persisted) {
            if (currentEventChunk.getFirst() != null) {
                while (currentEventChunk.hasNext()) {
                    StreamEvent currentEvent = currentEventChunk.next();
                    StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEvent);
                    toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                    resetChunk.add(toExpireEvent);
                }
                currentEventChunk.clear();
                nextProcessor.process(resetChunk);
            } else {
                log.info("No events have arrived for the current throttling cron window of " + cronString);
            }
        } else {
            clonedStreamEvent.setType(ComplexEvent.Type.RESET);
            resetChunk.add(clonedStreamEvent);
            nextProcessor.process(resetChunk);
        }

    }
}
