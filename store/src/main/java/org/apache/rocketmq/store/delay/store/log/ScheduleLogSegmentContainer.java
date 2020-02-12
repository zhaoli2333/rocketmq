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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.delay.store.appender.LogAppender;
import org.apache.rocketmq.store.delay.config.DelayMessageStoreConfiguration;
import org.apache.rocketmq.store.delay.model.*;
import org.apache.rocketmq.store.delay.store.validator.DelaySegmentValidator;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;


public class ScheduleLogSegmentContainer extends AbstractDelaySegmentContainer<ScheduleLogSequence> {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final DelayMessageStoreConfiguration config;

    ScheduleLogSegmentContainer(DelayMessageStoreConfiguration config, File logDir, DelaySegmentValidator validator, LogAppender<ScheduleLogSequence, LogRecord> appender) {
        super(config.getSegmentScale(), logDir, validator, appender);
        this.config = config;
    }

    @Override
    protected void loadLogs(DelaySegmentValidator validator) {
        LOGGER.info("Loading logs.");
        File[] files = this.logDir.listFiles();
        if (files != null) {
            for (final File file : files) {
                if (file.getName().startsWith(".")) {
                    continue;
                }
                if (file.isDirectory()) {
                    continue;
                }

                DelaySegment<ScheduleLogSequence> segment;
                try {
                    segment = new ScheduleLogSegment(file);
                    long size = validator.validate(segment);
                    segment.setWrotePosition(size);
                    segment.setFlushedPosition(size);
                    segments.put(segment.getSegmentBaseOffset(), segment);
                } catch (IOException e) {
                    LOGGER.error("Load {} failed.", file.getAbsolutePath(), e);
                }
            }
        }
        LOGGER.info("Load logs done.");
    }

    @Override
    protected RecordResult<ScheduleLogSequence> retResult(AppendMessageResult<ScheduleLogSequence> result) {
        switch (result.getStatus()) {
            case SUCCESS:
                return new AppendScheduleLogRecordResult(PutMessageStatus.SUCCESS, result);
            default:
                return new AppendScheduleLogRecordResult(PutMessageStatus.UNKNOWN_ERROR, result);
        }
    }

    @Override
    protected DelaySegment<ScheduleLogSequence> allocSegment(long segmentBaseOffset) {
        File nextSegmentFile = new File(logDir, String.valueOf(segmentBaseOffset));
        try {
            DelaySegment<ScheduleLogSequence> logSegment = new ScheduleLogSegment(nextSegmentFile);
            segments.put(segmentBaseOffset, logSegment);
            LOGGER.info("alloc new schedule set segment file {}", ((ScheduleLogSegment) logSegment).fileName);
            return logSegment;
        } catch (IOException e) {
            LOGGER.error("Failed create new schedule set segment file. file: {}", nextSegmentFile.getAbsolutePath(), e);
        }
        return null;
    }

    ScheduleLogRecord recover(long scheduleTime, int size, long offset) {
        ScheduleLogSegment segment = (ScheduleLogSegment) locateSegment(scheduleTime);
        if (segment == null) {
            LOGGER.error("schedule set recover null value, scheduleTime:{}, size:{}, offset:{}", scheduleTime, size, offset);
            return null;
        }

        return segment.recover(offset, size);
    }

    public void clean() {
        long checkTime = ScheduleOffsetResolver.resolveSegment(System.currentTimeMillis() - config.getDispatchLogKeepTime() - config.getCheckCleanTimeBeforeDispatch(), segmentScale);
        for (DelaySegment<ScheduleLogSequence> segment : segments.values()) {
            if (segment.getSegmentBaseOffset() < checkTime) {
                clean(segment.getSegmentBaseOffset());
            }
        }
    }

    ScheduleLogSegment loadSegment(long segmentBaseOffset) {
        return (ScheduleLogSegment) segments.get(segmentBaseOffset);
    }

    Map<Long, Map<String, Long>> countSegments() {
        final Map<Long, Map<String, Long>> offsetTable = new  HashMap<Long, Map<String, Long>>(segments.size());
        for(Map.Entry<Long, DelaySegment<ScheduleLogSequence>> entry : segments.entrySet()) {
            Map<String, Long> map = new HashMap<String, Long>();

            long baseOffset = entry.getKey();
            ScheduleLogSegment delaySegment = (ScheduleLogSegment)entry.getValue();

            map.put(ScheduleLogCheckPointKey.SCHEDULE_LOG_OFFSET, delaySegment.getWrotePosition());
            map.put(ScheduleLogCheckPointKey.COMMIT_LOG_OFFSET, delaySegment.getMaxCommitLogOffset());
            offsetTable.put(baseOffset, map);
        }
        return offsetTable;
    }

    long reValidate(final Map<Long, Map<String, Long>>  offsetTable, int singleMessageLimitSize) {
        ConcurrentSkipListSet<Long> commitLogOffsets = new ConcurrentSkipListSet<Long>();
        segments.values().parallelStream().forEach(segment -> {
            Map<String, Long> offsetMap = offsetTable.get(segment.getSegmentBaseOffset());
            Long scheduleLogOffset = null;
            Long commitLogOffset = null;
            if(offsetMap != null) {
                scheduleLogOffset = offsetMap.get(ScheduleLogCheckPointKey.SCHEDULE_LOG_OFFSET);
                commitLogOffset = offsetMap.get(ScheduleLogCheckPointKey.COMMIT_LOG_OFFSET);
            }
            long wrotePosition = segment.getWrotePosition();
            if (null == scheduleLogOffset || null == commitLogOffset || scheduleLogOffset != wrotePosition) {
                ScheduleLogValidateResult validateResult = doValidate((ScheduleLogSegment) segment, singleMessageLimitSize);
                scheduleLogOffset = validateResult.getMaxScheduleLogOffset();
                commitLogOffset = validateResult.getMaxCommitLogOffset();
                commitLogOffsets.add(commitLogOffset);
            } else {
                scheduleLogOffset = wrotePosition;
                commitLogOffsets.add(commitLogOffset);
            }

            ((ScheduleLogSegment) segment).loadOffset(scheduleLogOffset, commitLogOffset);
        });

        return commitLogOffsets.size() > 0  ? commitLogOffsets.last() : -1 ;
    }

    private ScheduleLogValidateResult doValidate(ScheduleLogSegment segment, int singleMessageLimitSize) {
        return segment.doValidate(singleMessageLimitSize);
    }
}
