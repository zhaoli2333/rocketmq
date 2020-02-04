# RocketMq支持任意延迟的延时消息

<a name="2Piv3"></a>
### 主要特性
支持任意延迟的延时消息，精确到秒，最长延迟时间为1年。

<a name="4RnXI"></a>
### 使用方法
<a name="hkrpC"></a>
#### producer
```

        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");

        producer.setNamesrvAddr("127.0.0.1:9876");

        producer.start();

        for (int i = 0; i < 100000; i++) {
            try {

                Message msg = new Message("TopicTest" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );

		/*
                 * 设置延迟时间，单位：秒
                 */
                msg.setDelayTime(5);
                /*
                 * Call send message to deliver message to one of brokers.
                 */
                SendResult sendResult = producer.send(msg);

                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        /*
         * Shut down once the producer instance is not longer in use.
         */
        producer.shutdown();
```

<a name="iJfkl"></a>
#### consumer
和普通 consumer相同
