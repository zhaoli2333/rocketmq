# RocketMq支持任意延迟的延时消息方案

<a name="2Piv3"></a>
### 主要特性
支持任意延迟的延时消息，精确到秒，最长延迟时间为1年。

<a name="4RnXI"></a>
### 使用方法
<a name="oz0hb"></a>
#### 配置broker.conf
```
segmentScale=60  ##每个时间桶的时间范围，单位分钟，默认60，可选值0-60之间，延迟消息并发数越高，配置的值应该越小，如：10
dispatchLogKeepTime=72 ##dispatchLog过期后保存的小时数，默认72
```
<a name="hkrpC"></a>
#### 
<a name="vmcMt"></a>
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
                
                SendResult sendResult = producer.send(msg);

                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        producer.shutdown();


```

<a name="iJfkl"></a>
#### consumer
和普通 consumer相同

<a name="QhglS"></a>
### 主要修改点

<a name="Cr51e"></a>
#### MessageStoreConfig
扩展了broker.conf配置文件，新增了延迟消息相关配置

<a name="Lxniu"></a>
#### DefaultMessageStore
新增CommitLogDispatcherBuildDelayLog用于转发延迟消息保存任务。

<a name="Oq9js"></a>
#### org.apache.rocketmq.store
新增delay目录，提供delay log的存储和消费进度保存等功能。


