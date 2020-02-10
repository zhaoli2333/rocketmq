package org.apache.rocketmq.store.delay.wheel;

import org.apache.rocketmq.common.ThreadFactoryImpl;
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
import org.apache.rocketmq.store.delay.config.DelayMessageStoreConfiguration;
import org.apache.rocketmq.store.delay.model.DispatchLogRecord;
import org.apache.rocketmq.store.delay.model.ScheduleIndex;
import org.apache.rocketmq.store.delay.model.ScheduleSetRecord;

import java.util.concurrent.*;


public class ScheduleLogSender implements Runnable{

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final int DEFAULT_QUEUE_SIZE = 1000000;
    private static final int DEFAULT_PROCESS_THREADS = Runtime.getRuntime().availableProcessors() + 1;

    private int queueSize = DEFAULT_QUEUE_SIZE;
    private int threads = DEFAULT_PROCESS_THREADS;

    private BlockingQueue<ScheduleIndex> queue;
    private ThreadPoolExecutor executor;
    private DelayLogFacade delayLogFacade;
    private DefaultMessageStore defaultMessageStore;
    private DelayMessageStoreConfiguration delayConfig;


    public ScheduleLogSender(DelayLogFacade delayLogFacade, DefaultMessageStore defaultMessageStore, DelayMessageStoreConfiguration delayConfig)  {
        this.delayLogFacade = delayLogFacade;
        this.defaultMessageStore = defaultMessageStore;
        this.queue = new LinkedBlockingQueue<>(this.queueSize);
        this.executor = new ThreadPoolExecutor(1, this.threads, 1L, TimeUnit.MINUTES,
                new ArrayBlockingQueue<Runnable>(1), new ThreadFactoryImpl("schedule-log-send-task", true));
        this.executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
        this.delayConfig = delayConfig;
    }


    public ScheduleLogSender(DelayLogFacade delayLogFacade, DefaultMessageStore defaultMessageStore, DelayMessageStoreConfiguration delayConfig, int queueSize, int threads) {
        this.delayLogFacade = delayLogFacade;
        this.defaultMessageStore = defaultMessageStore;
        this.queueSize = queueSize;
        this.threads = threads;
        this.queue = new LinkedBlockingQueue<>(this.queueSize);
        this.executor = new ThreadPoolExecutor(1, this.threads, 1L, TimeUnit.MINUTES,
                new ArrayBlockingQueue<Runnable>(1), new ThreadFactoryImpl("schedule-log-send-task", true));
        this.executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
        this.delayConfig = delayConfig;
    }

    public void destroy() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    public boolean addTask(ScheduleIndex scheduleIndex) {
        if(this.delayConfig.getBrokerRole() == BrokerRole.SLAVE) {
            return true;
        }

        boolean offer = this.queue.offer(scheduleIndex);
        if (offer) {
            this.executor.execute(this);
        }
        return offer;
    }


    @Override
    public void run() {
        while (!this.queue.isEmpty()) {
            ScheduleIndex index = this.queue.poll();
            if(index != null) {
                ScheduleSetRecord record = delayLogFacade.recoverLogRecord(index);
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
                                delayLogFacade.appendDispatchLog(new DispatchLogRecord(record.getTopic(), record.getMessageId(), record.getScheduleTime(), record.getSequence()));
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
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME);

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        msgInner.setTopic(msgExt.getTopic());
        msgInner.setQueueId(msgExt.getQueueId());

        return msgInner;
    }


}
