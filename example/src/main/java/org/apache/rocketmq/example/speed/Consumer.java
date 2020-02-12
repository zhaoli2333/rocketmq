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

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class Consumer {

    private DefaultMQPushConsumer defaultMQPushConsumer;
    private AtomicLong received = new AtomicLong(0);
    private Thread printTask;

    public Consumer() {
        this.printTask = new Thread(new PrintTask());
        this.defaultMQPushConsumer = new DefaultMQPushConsumer("ConsumerGroupTestSpeed");
        this.defaultMQPushConsumer.setNamesrvAddr("127.0.0.1:9876");
        this.defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
    }


    private void init() throws MQClientException {
        this.defaultMQPushConsumer.subscribe("SpeedTest", "*");
        this.defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
//                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                long receivedNum = received.getAndAdd(msgs.size());
                if(receivedNum % 1000 == 0) {
                    System.out.printf("已成功收到消息 %d", receivedNum);
                    System.out.println();
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        this.defaultMQPushConsumer.start();
        this.printTask.start();
        System.out.printf("Consumer Started.%n");
    }


    class PrintTask implements Runnable{

        @Override
        public void run() {
            while (true) {
                System.out.printf("已成功收到消息 %d", received.get());
                System.out.println();
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void main(String[] args) throws InterruptedException, MQClientException {
        Consumer consumer = new Consumer();
        consumer.init();

    }
}
