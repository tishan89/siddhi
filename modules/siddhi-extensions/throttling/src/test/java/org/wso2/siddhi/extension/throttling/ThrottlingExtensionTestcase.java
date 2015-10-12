package org.wso2.siddhi.extension.throttling;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.Random;

public class ThrottlingExtensionTestcase {
    static final Logger log = Logger.getLogger(ThrottlingExtensionTestcase.class);


    @Test
    public void testThrottlingWindow() throws InterruptedException {
        int maxReqCount = 2;
        log.info("ThrottlingExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@config(async = 'true')define stream inputStream (ip string, maxCount int);";
        String query1 = ("@info(name = 'query1')" +
                /*"partition with (ip of inputStream)" +
                "begin " +*/
                "from inputStream#window.throttle:onRequest('*/30 * * * * ?',true) " +
                "select ip , (count(ip) >= maxCount) as isThrottled, count(ip) as ipCount " +
                "insert all events into outputStream;" +

                "from every e1=outputStream, e2=outputStream[(e1.isThrottled != e2.isThrottled)] " +
                "select e1.ip, e2.isThrottled " +
                "insert into finalThrottlingStream;" +

                "from e1=outputStream " +
                "select e1.ip, e1.isThrottled " +
                "insert into finalThrottlingStream;"/*+
                "end;"*/);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query1);

        executionPlanRuntime.addCallback("finalThrottlingStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                ThrottleManager.setIsThrottled((Boolean) events[0].getData()[1]);
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        sendEvent(inputHandler, maxReqCount);
        Thread.sleep(100);
        sendEvent(inputHandler, maxReqCount);
        Thread.sleep(1000);
        sendEvent(inputHandler, maxReqCount);
        sendEvent(inputHandler, maxReqCount);
        Thread.sleep(1000*65);
        sendEvent(inputHandler, maxReqCount);
        sendEvent(inputHandler, maxReqCount);
        executionPlanRuntime.shutdown();
    }

    private void sendEvent(InputHandler inputHandler, int maxReqCount) throws InterruptedException {
        if(!ThrottleManager.getIsThrottled()) {
            inputHandler.send(new Object[]{"10.100.5.99", maxReqCount});
            log.info("event sent");
        } else {
            log.info("event declined");
        }
    }

    @Test
    public void testRateLimitting() throws InterruptedException {
        int maxReqCount = 3;
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@config(async = 'true')define stream inputStream (ip string, maxCount int);";
        String query1 = ("@info(name = 'query1')" +
                "partition with (ip of inputStream)" +
                "begin " +
                "from inputStream#window.time(5000) " +
                "select ip , (count(ip) >= maxCount) as isThrottled " +
                "insert all events into #outputStream;" +

                "from every e1=#outputStream, e2=#outputStream[(e1.isThrottled != e2.isThrottled)] " +
                "select e1.ip, e2.isThrottled " +
                "insert into finalThrottlingStream;" +

                "from e1=#outputStream " +
                "select e1.ip, e1.isThrottled " +
                "insert into finalThrottlingStream;"+
                "end;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query1);
        executionPlanRuntime.addCallback("finalThrottlingStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                ThrottleManager.setIsThrottled((Boolean) events[0].getData()[1]);
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        Random rand = new Random();
        long time = System.currentTimeMillis();
        int count = 0;
        while(true){

            if(!ThrottleManager.getIsThrottled()){
                inputHandler.send(new Object[]{"10.100.5.99", maxReqCount});
                count++;
                log.info("Sent " + count);
                if(count>=1000){
                    log.info("Throughput : "+ 1000*1000l/(System.currentTimeMillis()-time));
                    break;
                }
                Thread.sleep(rand.nextInt(2500));
            } else {
                log.info("Declined");
                Thread.sleep(rand.nextInt(2500));
            }
        }
    }
}
