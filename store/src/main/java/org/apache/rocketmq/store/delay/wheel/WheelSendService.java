package org.apache.rocketmq.store.delay.wheel;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.delay.DelayLogFacade;
import org.apache.rocketmq.store.delay.common.Switchable;
import org.apache.rocketmq.store.delay.config.DelayMessageStoreConfiguration;
import org.apache.rocketmq.store.delay.model.DispatchLogRecord;
import org.apache.rocketmq.store.delay.model.ScheduleIndex;
import org.apache.rocketmq.store.delay.model.ScheduleLogRecord;

import java.util.concurrent.*;


public class WheelSendService extends ServiceThread implements Switchable {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final int DEFAULT_QUEUE_SIZE = 1000000;

    private int queueSize = DEFAULT_QUEUE_SIZE;

    private BlockingQueue<ScheduleIndex> queue;
    private DelayLogFacade delayLogFacade;
    private DefaultMessageStore defaultMessageStore;
    private DelayMessageStoreConfiguration delayConfig;


    public WheelSendService(DelayLogFacade delayLogFacade, DefaultMessageStore defaultMessageStore, DelayMessageStoreConfiguration delayConfig)  {
        this.delayLogFacade = delayLogFacade;
        this.defaultMessageStore = defaultMessageStore;
        this.queue = new LinkedBlockingQueue<>(this.queueSize);
        this.delayConfig = delayConfig;
    }


    public WheelSendService(DelayLogFacade delayLogFacade, DefaultMessageStore defaultMessageStore, DelayMessageStoreConfiguration delayConfig, int queueSize) {
        this.delayLogFacade = delayLogFacade;
        this.defaultMessageStore = defaultMessageStore;
        this.queueSize = queueSize;
        this.queue = new LinkedBlockingQueue<>(this.queueSize);
        this.delayConfig = delayConfig;
    }

    public boolean addTask(ScheduleIndex scheduleIndex) {
        if(this.delayConfig.getBrokerRole() == BrokerRole.SLAVE) {
            return true;
        }

        return this.queue.offer(scheduleIndex);
    }


    @Override
    public String getServiceName() {
        return "schedule-log-send-task";
    }

    @Override
    public void run() {
        while (!this.isStopped()) {
            ScheduleIndex index = null;
            try {
                index = this.queue.poll(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignore) {}
            if(index != null) {
                ScheduleLogRecord record = this.delayLogFacade.recoverLogRecord(index);
                if (record != null) {
                    MessageExt msgExt = null;
                    try {
                        msgExt = MessageDecoder.decode(record.getRecord(), true, false);
                    } finally {
                        record.release();
                    }

                    if (msgExt != null) {
                        try {
                            MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                            PutMessageResult putMessageResult = this.defaultMessageStore.putMessage(msgInner);

                            if (putMessageResult != null
                                    && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                this.delayLogFacade.appendDispatchLog(new DispatchLogRecord(record.getTopic(), record.getMessageId(), record.getScheduleTime(), record.getSequence()));
                            } else {
                                LOGGER.error(
                                        "DelayMessageManager, a message time up, but reput it failed, topic: {} msgId {}",
                                        msgExt.getTopic(), msgExt.getMsgId());

                            }
                        } catch (Exception e) {
                            LOGGER.error(
                                    "DelayMessageManager, messageTimeup execute error, drop it. msgExt="
                                            + msgExt);
                        }

                    }
                }
            }

        }
    }

    private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);
        // 删除延迟属性
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME);

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        // 恢复topic
        msgInner.setTopic(msgInner.getProperties().get(MessageConst.PROPERTY_REAL_TOPIC));
        msgInner.setQueueId(msgExt.getQueueId());

        return msgInner;
    }





}
