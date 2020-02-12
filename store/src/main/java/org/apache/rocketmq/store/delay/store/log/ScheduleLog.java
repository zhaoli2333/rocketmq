/*
 * Copyright 2018 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store.delay.store.log;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.delay.store.PeriodicFlushService;
import org.apache.rocketmq.store.delay.store.appender.ScheduleSetAppender;
import org.apache.rocketmq.store.delay.base.LongHashSet;
import org.apache.rocketmq.store.delay.common.Disposable;
import org.apache.rocketmq.store.delay.config.DelayMessageStoreConfiguration;
import org.apache.rocketmq.store.delay.model.*;
import org.apache.rocketmq.store.delay.store.validator.DefaultDelaySegmentValidator;
import org.apache.rocketmq.store.delay.store.validator.ScheduleLogValidatorSupport;
import org.apache.rocketmq.store.delay.store.visitor.LogVisitor;
import org.apache.rocketmq.store.delay.wheel.WheelLoadCursor;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;


public class ScheduleLog extends AbstractDelayLog<ScheduleLogSequence> implements Disposable {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * log flush interval,500ms
     */
    private static final int DEFAULT_FLUSH_INTERVAL = 500;

    private final AtomicBoolean open;
    private final DelayMessageStoreConfiguration config;
    private final AtomicLong maxPhysicOffset ;
    private ScheduleLogValidatorSupport validatorSupport;

    public ScheduleLog(DelayMessageStoreConfiguration storeConfiguration) {
        super(new ScheduleLogSegmentContainer(
                storeConfiguration,
                new File(storeConfiguration.getScheduleLogStorePath()),
                new DefaultDelaySegmentValidator(),
                new ScheduleSetAppender(storeConfiguration.getSingleMessageLimitSize())));
        this.config = storeConfiguration;
        this.open = new AtomicBoolean(true);
        this.validatorSupport  = new ScheduleLogValidatorSupport(this.config);
        this.maxPhysicOffset = new AtomicLong(reValidate(storeConfiguration.getSingleMessageLimitSize()));
    }

    private long reValidate(int singleMessageLimitSize) {
        if(this.validatorSupport.load()) {
            Map<Long, Map<String, Long>> offsetTable =  validatorSupport.getOffsetTable();
            LOGGER.info("load file schedule_offset_checkpoint.json success, content: {}", JSON.toJSONString(offsetTable));
            return ((ScheduleLogSegmentContainer) container).reValidate(offsetTable, singleMessageLimitSize);
        }
        return -1;
    }

    public long getMaxPhysicOffset() {
        return maxPhysicOffset.get();
    }

    public AppendLogResult<ScheduleIndex> appendLog(LogRecord record) {
        if (!open.get()) {
            return new AppendLogResult<>(MessageProducerCode.STORE_ERROR, "schedule log closed");
        }

        if(record.getSequence() <= getMaxPhysicOffset()) {
            return new AppendLogResult<>(MessageProducerCode.MESSAGE_DUPLICATE, "message duplicate");
        }

        AppendLogResult<RecordResult<ScheduleLogSequence>> result = this.append(record);
        int code = result.getCode();
        if (MessageProducerCode.SUCCESS != code) {
            LOGGER.error("appendMessageLog schedule set error,log:{} {},code:{}", record.getTopic(), record.getMessageId(), code);
            return new AppendLogResult<>(MessageProducerCode.STORE_ERROR, "appendScheduleSetError");
        }

        maxPhysicOffset.set(record.getSequence());

        RecordResult<ScheduleLogSequence> recordResult = result.getAdditional();
        ScheduleIndex index = new ScheduleIndex(
                record.getTopic(),
                record.getScheduleTime(),
                recordResult.getResult().getWroteOffset(),
                recordResult.getResult().getWroteBytes(),
                recordResult.getResult().getAdditional().getSequence());

        return new AppendLogResult<>(MessageProducerCode.SUCCESS, "", index);
    }

    @Override
    public boolean clean(Long key) {
        return container.clean(key);
    }

    @Override
    public void flush() {
        if (open.get()) {
            container.flush();
        }
    }

    public ScheduleLogRecord recoverLogRecord(ScheduleIndex scheduleIndex) {
        ScheduleLogRecord logRecord = ((ScheduleLogSegmentContainer)container).recover(scheduleIndex.getScheduleTime(), scheduleIndex.getSize(), scheduleIndex.getOffset());
        if (logRecord == null) {
            LOGGER.error("schedule log recover null record");
        }

        return logRecord;
    }

    public void clean() {
        ((ScheduleLogSegmentContainer) container).clean();
    }

    public WheelLoadCursor.Cursor loadUnDispatch(ScheduleLogSegment segment, final LongHashSet dispatchedSet, final Consumer<ScheduleIndex> func) {
        LogVisitor<ScheduleIndex> visitor = segment.newVisitor(0, config.getSingleMessageLimitSize());
        try {
            long offset = 0;
            while (true) {
                Optional<ScheduleIndex> recordOptional = visitor.nextRecord();
                if (!recordOptional.isPresent()) break;
                ScheduleIndex index = recordOptional.get();
                long sequence = index.getSequence();
                offset = index.getOffset() + index.getSize();
                if (!dispatchedSet.contains(sequence)) {
                    func.accept(index);
                }
            }
            return new WheelLoadCursor.Cursor(segment.getSegmentBaseOffset(), offset);
        } finally {
            visitor.close();
            LOGGER.info("schedule log recover {} which is need to continue to dispatch.", segment.getSegmentBaseOffset());
        }
    }

    public ScheduleLogSegment loadSegment(long segmentBaseOffset) {
        return ((ScheduleLogSegmentContainer) container).loadSegment(segmentBaseOffset);
    }

    @Override
    public void destroy() {
        open.set(false);
        this.validatorSupport.setOffsetTable(checkOffsets());
        this.validatorSupport.persist();
    }

    private Map<Long, Map<String, Long>> checkOffsets() {
        return ((ScheduleLogSegmentContainer) container).countSegments();
    }

    public long higherBaseOffset(long low) {
        return ((ScheduleLogSegmentContainer) container).higherBaseOffset(low);
    }

    public PeriodicFlushService.FlushProvider getProvider() {
        return new PeriodicFlushService.FlushProvider() {
            @Override
            public int getInterval() {
                return DEFAULT_FLUSH_INTERVAL;
            }

            @Override
            public void flush() {
                ScheduleLog.this.flush();
            }
        };
    }
}
