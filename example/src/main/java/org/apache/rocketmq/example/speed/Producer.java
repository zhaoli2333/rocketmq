/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.speed;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Producer {

    private static final String TOPIC = "SpeedTest";
    private static final int maxDelaySeconds = 60;
    private static final int loopTimesEachThread = 100000;
    private static final int sendThreads = 10;

    private DefaultMQProducer defaultMQProducer;
    private ExecutorService executorService;
    private int workThreadNum;
    private AtomicLong sendSuccess;
    private volatile boolean stoped = false;
    private long startTime;
    private long endTime;
    private CountDownLatch countDownLatch;


    public static void main(String[] args) throws MQClientException {
        Producer producer = new Producer(sendThreads, "127.0.0.1:9876");
        producer.start();

        producer.waitUntilSendFinished();

        producer.destory();

    }

    public Producer(int workThreadNum, String namesrvAddr) {
        this.sendSuccess = new AtomicLong(0);
        this.workThreadNum = workThreadNum;
        this.countDownLatch = new CountDownLatch(this.workThreadNum);
        this.executorService = Executors.newFixedThreadPool(this.workThreadNum);

        this.defaultMQProducer = new DefaultMQProducer("please_rename_unique_group_name");
        this.defaultMQProducer.setNamesrvAddr(namesrvAddr);
    }

    public void waitUntilSendFinished() {
        try {
            this.countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void start() throws MQClientException {
        this.defaultMQProducer.start();
        this.startTime = System.currentTimeMillis();
        for(int i = 0; i < this.workThreadNum; i++) {
            executorService.submit(new SendMsgTask());
        }
    }


    private void destory() {
        this.stoped = true;
        this.executorService.shutdown();
        this.endTime = System.currentTimeMillis();
        this.defaultMQProducer.shutdown();
        System.out.printf("已成功发送消息 %d", this.sendSuccess.get());
        System.out.println();
        System.out.printf("发送结束，tps:" + this.sendSuccess.get()/((this.endTime-this.startTime)/1000));
        System.out.println();
    }

    class SendMsgTask implements Runnable {

        @Override
        public void run() {
            int i = 0;
            Random random = new Random();
            while(i < loopTimesEachThread) {
                i ++ ;
                try {

                    Message msg = new Message(TOPIC /* Topic */,
                            "TagA" /* Tag */,
                            ("Hello RocketMQ").getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                    );

                    msg.setDelayTime(random.nextInt(maxDelaySeconds) + 1);
                    SendResult sendResult = defaultMQProducer.send(msg);

                    long sendNum = 0;
                    if(sendResult.getSendStatus() == SendStatus.SEND_OK) {
                        sendNum = sendSuccess.getAndIncrement();
                    }

                    if(sendNum % 1000 == 0) {
                        System.out.printf("已成功发送消息%d", sendNum);
                        System.out.println();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println();
            System.out.printf("已成功发送消息%d", sendSuccess.get());
            countDownLatch.countDown();
        }
    }
}
