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

package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.delay.base.LongHashSet;
import org.apache.rocketmq.store.delay.cleaner.LogCleaner;
import org.apache.rocketmq.store.delay.cleaner.LogFlusher;
import org.apache.rocketmq.store.delay.config.DelayMessageStoreConfiguration;
import org.apache.rocketmq.store.delay.model.AppendLogResult;
import org.apache.rocketmq.store.delay.model.LogRecord;
import org.apache.rocketmq.store.delay.model.ScheduleIndex;
import org.apache.rocketmq.store.delay.model.ScheduleSetRecord;
import org.apache.rocketmq.store.delay.store.log.DispatchLog;
import org.apache.rocketmq.store.delay.store.log.DispatchLogSegment;
import org.apache.rocketmq.store.delay.store.log.ScheduleLog;
import org.apache.rocketmq.store.delay.store.log.ScheduleSetSegment;
import org.apache.rocketmq.store.delay.wheel.WheelLoadCursor;

import java.util.function.Consumer;

public class DefaultDelayLogFacade implements DelayLogFacade {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final ScheduleLog scheduleLog;
    private final DispatchLog dispatchLog;
    private final LogFlusher logFlusher;
    private final LogCleaner logCleaner;

    public DefaultDelayLogFacade(final DelayMessageStoreConfiguration config) {
        this.scheduleLog = new ScheduleLog(config);
        this.dispatchLog = new DispatchLog(config);
        this.logFlusher = new LogFlusher(scheduleLog, dispatchLog);
        this.logCleaner = new LogCleaner(config, dispatchLog, scheduleLog);

    }

    @Override
    public void start() {
        logFlusher.start();
        logCleaner.start();
    }


    @Override
    public void shutdown() {
        logCleaner.shutdown();
        logFlusher.shutdown();
        scheduleLog.destroy();
    }


    @Override
    public AppendLogResult<ScheduleIndex> appendScheduleLog(LogRecord record) {
        return scheduleLog.append(record);
    }


    @Override
    public ScheduleSetRecord recoverLogRecord(final ScheduleIndex scheduleIndex) {
        return scheduleLog.recoverLogRecord(scheduleIndex);
    }

    @Override
    public void appendDispatchLog(LogRecord record) {
        dispatchLog.append(record);
    }

    @Override
    public DispatchLogSegment latestDispatchSegment() {
        return dispatchLog.latestSegment();
    }

    @Override
    public DispatchLogSegment lowerDispatchSegment(final long baseOffset) {
        return dispatchLog.lowerSegment(baseOffset);
    }

    @Override
    public ScheduleSetSegment loadScheduleLogSegment(final long segmentBaseOffset) {
        return scheduleLog.loadSegment(segmentBaseOffset);
    }

    @Override
    public WheelLoadCursor.Cursor loadUnDispatch(final ScheduleSetSegment setSegment, final LongHashSet dispatchedSet, final Consumer<ScheduleIndex> refresh) {
        return scheduleLog.loadUnDispatch(setSegment, dispatchedSet, refresh);
    }

    @Override
    public long higherScheduleBaseOffset(long index) {
        return scheduleLog.higherBaseOffset(index);
    }

    @Override
    public long higherDispatchLogBaseOffset(long segmentBaseOffset) {
        return dispatchLog.higherBaseOffset(segmentBaseOffset);
    }



}
