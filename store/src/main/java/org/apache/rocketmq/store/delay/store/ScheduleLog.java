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

package org.apache.rocketmq.store.delay.store;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.delay.appender.ScheduleSetAppender;
import org.apache.rocketmq.store.delay.base.LongHashSet;
import org.apache.rocketmq.store.delay.common.Disposable;
import org.apache.rocketmq.store.delay.config.DelayMessageStoreConfiguration;
import org.apache.rocketmq.store.delay.model.*;
import org.apache.rocketmq.store.delay.validator.DefaultDelaySegmentValidator;
import org.apache.rocketmq.store.delay.validator.ScheduleLogValidatorSupport;
import org.apache.rocketmq.store.delay.visitor.LogVisitor;
import org.apache.rocketmq.store.delay.wheel.WheelLoadCursor;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


public class ScheduleLog implements Log<ScheduleIndex, LogRecord>, Disposable {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final ScheduleSet scheduleSet;
    private final AtomicBoolean open;
    private final DelayMessageStoreConfiguration config;

    public ScheduleLog(DelayMessageStoreConfiguration storeConfiguration) {
        final ScheduleSetSegmentContainer setContainer = new ScheduleSetSegmentContainer(
                storeConfiguration,
                new File(storeConfiguration.getScheduleLogStorePath()),
                new DefaultDelaySegmentValidator(),
                new ScheduleSetAppender(storeConfiguration.getSingleMessageLimitSize()));

        this.config = storeConfiguration;
        this.scheduleSet = new ScheduleSet(setContainer);
        this.open = new AtomicBoolean(true);
        reValidate(storeConfiguration.getSingleMessageLimitSize());
    }

    private void reValidate(int singleMessageLimitSize) {
        ScheduleLogValidatorSupport support = ScheduleLogValidatorSupport.getSupport(config);
        Map<Long, Long> offsets = support.loadScheduleOffsetCheckpoint();
        scheduleSet.reValidate(offsets, singleMessageLimitSize);
    }

    @Override
    public AppendLogResult<ScheduleIndex> append(LogRecord record) {
        if (!open.get()) {
            return new AppendLogResult<>(MessageProducerCode.STORE_ERROR, "schedule log closed");
        }
        AppendLogResult<RecordResult<ScheduleSetSequence>> result = scheduleSet.append(record);
        int code = result.getCode();
        if (MessageProducerCode.SUCCESS != code) {
            LOGGER.error("appendMessageLog schedule set error,log:{} {},code:{}", record.getTopic(), record.getMessageId(), code);
            return new AppendLogResult<>(MessageProducerCode.STORE_ERROR, "appendScheduleSetError");
        }

        RecordResult<ScheduleSetSequence> recordResult = result.getAdditional();
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
        return scheduleSet.clean(key);
    }

    @Override
    public void flush() {
        if (open.get()) {
            scheduleSet.flush();
        }
    }

    public ScheduleSetRecord recoverLogRecord(ScheduleIndex scheduleIndex) {
        ScheduleSetRecord logRecord = scheduleSet.recoverRecord(scheduleIndex);
        if (logRecord == null) {
            LOGGER.error("schedule log recover null record");
        }

        return logRecord;
    }

    public void clean() {
        scheduleSet.clean();
    }

    public WheelLoadCursor.Cursor loadUnDispatch(ScheduleSetSegment segment, final LongHashSet dispatchedSet, final Consumer<ScheduleIndex> func) {
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

    public ScheduleSetSegment loadSegment(long segmentBaseOffset) {
        return scheduleSet.loadSegment(segmentBaseOffset);
    }

    @Override
    public void destroy() {
        open.set(false);
        ScheduleLogValidatorSupport.getSupport(config).saveScheduleOffsetCheckpoint(checkOffsets());
    }

    private Map<Long, Long> checkOffsets() {
        return scheduleSet.countSegments();
    }

    public long higherBaseOffset(long low) {
        return scheduleSet.higherBaseOffset(low);
    }
}
