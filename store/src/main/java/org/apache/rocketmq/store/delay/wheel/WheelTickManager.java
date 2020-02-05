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

package org.apache.rocketmq.store.delay.wheel;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.delay.DelayLogFacade;
import org.apache.rocketmq.store.delay.base.LongHashSet;
import org.apache.rocketmq.store.delay.common.Switchable;
import org.apache.rocketmq.store.delay.config.DelayMessageStoreConfiguration;
import org.apache.rocketmq.store.delay.model.ScheduleIndex;
import org.apache.rocketmq.store.delay.store.log.DispatchLogSegment;
import org.apache.rocketmq.store.delay.store.log.ScheduleOffsetResolver;
import org.apache.rocketmq.store.delay.store.log.ScheduleSetSegment;
import org.apache.rocketmq.store.delay.store.visitor.LogVisitor;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class WheelTickManager implements Runnable, Switchable, HashedWheelTimer.Processor {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 每500ms一个tick
    private static final int TICKS_PER_WHEEL = 2 * 60 * 60;

    // 默认60分钟
    private final int segmentScale;
    private final ScheduledExecutorService loadScheduler;
    private final DelayMessageStoreConfiguration delayConfig;
    private final DelayLogFacade facade;
    private final HashedWheelTimer timer;
    private final AtomicBoolean started;
    private final WheelLoadCursor loadingCursor;
    private final WheelLoadCursor loadedCursor;
    private final DefaultMessageStore defaultMessageStore;
    private final ScheduleLogSender scheduleLogSender;

    public WheelTickManager(DelayMessageStoreConfiguration delayConfig, DelayLogFacade facade, DefaultMessageStore defaultMessageStore) {
        this.delayConfig = delayConfig;
        this.segmentScale = delayConfig.getSegmentScale();
        this.timer = new HashedWheelTimer(new ThreadFactoryBuilder().setNameFormat("delay-send-%d").build(), 500, TimeUnit.MILLISECONDS, TICKS_PER_WHEEL, this);
        this.facade = facade;
        this.started = new AtomicBoolean(false);
        this.loadingCursor = WheelLoadCursor.create();
        this.loadedCursor = WheelLoadCursor.create();
        this.defaultMessageStore = defaultMessageStore;
        this.scheduleLogSender = new ScheduleLogSender(facade, defaultMessageStore, delayConfig);

        this.loadScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("wheel-segment-loader-%d").build());
    }

    @Override
    public void start() {
        if (!isStarted()) {
            timer.start();
            started.set(true);
            recover();
            loadScheduler.scheduleWithFixedDelay(this, 0, delayConfig.getLoadSegmentDelayMinutes(), TimeUnit.MINUTES);
            LOGGER.info("wheel started.");
        }
    }

    private void recover() {
        LOGGER.info("wheel recover...");
        DispatchLogSegment currentDispatchedSegment = facade.latestDispatchSegment();
        if (currentDispatchedSegment == null) {
            LOGGER.warn("load latest dispatch segment null");
            return;
        }

        long latestOffset = currentDispatchedSegment.getSegmentBaseOffset();
        DispatchLogSegment lastSegment = facade.lowerDispatchSegment(latestOffset);
        if (null != lastSegment) doRecover(lastSegment);

        doRecover(currentDispatchedSegment);
        LOGGER.info("wheel recover done. currentOffset:{}", latestOffset);
    }

    private void doRecover(DispatchLogSegment dispatchLogSegment) {
        long segmentBaseOffset = dispatchLogSegment.getSegmentBaseOffset();
        ScheduleSetSegment scheduleSetSegment = facade.loadScheduleLogSegment(segmentBaseOffset);
        if (scheduleSetSegment == null) {
            LOGGER.error("load schedule index error,dispatch segment:{}", segmentBaseOffset);
            return;
        }

        LongHashSet dispatchedSet = loadDispatchLog(dispatchLogSegment);
        WheelLoadCursor.Cursor loadCursor = facade.loadUnDispatch(scheduleSetSegment, dispatchedSet, this::refresh);
        long baseOffset = loadCursor.getBaseOffset();
        loadingCursor.shiftCursor(baseOffset, loadCursor.getOffset());
        loadedCursor.shiftCursor(baseOffset);
    }

    private LongHashSet loadDispatchLog(final DispatchLogSegment currentDispatchLog) {
        LogVisitor<Long> visitor = currentDispatchLog.newVisitor(0);
        final LongHashSet recordSet = new LongHashSet(currentDispatchLog.entries());
        try {
            while (true) {
                Optional<Long> recordOptional = visitor.nextRecord();
                if (!recordOptional.isPresent()) break;
                recordSet.set(recordOptional.get());
            }
            return recordSet;
        } finally {
            visitor.close();
        }
    }

    private boolean isStarted() {
        return started.get();
    }


    /**
     * resolve wheel-load start index
     *
     * @return generally, result > 0, however the result might be -1. -1 mean that no higher key.
     */
    private long resolveStartIndex() {
        WheelLoadCursor.Cursor loadedEntry = loadedCursor.cursor();
        long startIndex = loadedEntry.getBaseOffset();
        long offset = loadedEntry.getOffset();

        if (offset < 0) return facade.higherScheduleBaseOffset(startIndex);

        return startIndex;
    }

    private void loadSegment(ScheduleSetSegment segment) {
        long baseOffset = segment.getSegmentBaseOffset();
        long offset = segment.getWrotePosition();
        if (!loadingCursor.shiftCursor(baseOffset, offset)) {
            LOGGER.error("doLoadSegment error,shift loadingCursor failed,from {}-{} to {}-{}", loadingCursor.baseOffset(), loadingCursor.offset(), baseOffset, offset);
            return;
        }

        WheelLoadCursor.Cursor loadedCursorEntry = loadedCursor.cursor();
        // have loaded
        if (baseOffset < loadedCursorEntry.getBaseOffset()) return;

        long startOffset = 0;
        // last load action happened error
        if (baseOffset == loadedCursorEntry.getBaseOffset() && loadedCursorEntry.getOffset() > -1)
            startOffset = loadedCursorEntry.getOffset();

        LogVisitor<ScheduleIndex> visitor = segment.newVisitor(startOffset, delayConfig.getSingleMessageLimitSize());
        try {
            loadedCursor.shiftCursor(baseOffset, startOffset);

            long currentOffset = startOffset;
            while (currentOffset < offset) {
                Optional<ScheduleIndex> recordOptional = visitor.nextRecord();
                if (!recordOptional.isPresent()) break;
                ScheduleIndex index = recordOptional.get();
                currentOffset = index.getOffset() + index.getSize();
                refresh(index);
                loadedCursor.shiftOffset(currentOffset);
            }
            loadedCursor.shiftCursor(baseOffset);
            LOGGER.info("loaded segment:{} {}", loadedCursor.baseOffset(), currentOffset);
        } finally {
            visitor.close();
        }
    }

    private void refresh(ScheduleIndex index) {
        long now = System.currentTimeMillis();
        long scheduleTime = now;
        try {
            scheduleTime = index.getScheduleTime();
            timer.newTimeout(index, scheduleTime - now, TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            LOGGER.error("wheel refresh error, scheduleTime:{}, delay:{}", scheduleTime, scheduleTime - now);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void shutdown() {
        if (isStarted()) {
            loadScheduler.shutdown();
            timer.stop();
            scheduleLogSender.destroy();
            started.set(false);
            LOGGER.info("wheel shutdown.");
        }
    }

    public void addWheel(ScheduleIndex index) {
        refresh(index);
    }

    public boolean canAdd(long scheduleTime, long offset) {
        WheelLoadCursor.Cursor currentCursor = loadingCursor.cursor();
        long currentBaseOffset = currentCursor.getBaseOffset();
        long currentOffset = currentCursor.getOffset();

        long baseOffset = ScheduleOffsetResolver.resolveSegment(scheduleTime, segmentScale);
        if (baseOffset < currentBaseOffset) return true;

        if (baseOffset == currentBaseOffset) {
            return currentOffset <= offset;
        }
        return false;
    }

    @Override
    public void process(ScheduleIndex index) {
        scheduleLogSender.addTask(index);
    }



    @Override
    public void run() {
        long next = System.currentTimeMillis() + delayConfig.getLoadInAdvanceTimesInMillis();
        long prepareLoadBaseOffset = ScheduleOffsetResolver.resolveSegment(next, segmentScale);
        try {
            loadUntil(prepareLoadBaseOffset);
        } catch (InterruptedException ignored) {
            LOGGER.debug("load segment interrupted");
        }
    }

    private void loadUntil(long until) throws InterruptedException {
        long loadedBaseOffset = loadedCursor.baseOffset();
        // have loaded
        if (loadedBaseOffset > until) return;

        do {
            // wait next turn when loaded error.
            if (!loadUntilInternal(until)) break;

            // load successfully(no error happened) and current wheel loading cursor < until
            if (this.loadingCursor.baseOffset() < until) {
                long thresholdTime = System.currentTimeMillis() + delayConfig.getLoadBlockingExitTimesInMillis();
                // exit in a few minutes in advance
                if (ScheduleOffsetResolver.resolveSegment(thresholdTime, segmentScale) >= until) {
                    this.loadingCursor.shiftCursor(until);
                    this.loadedCursor.shiftCursor(until);
                    break;
                }
            }

            Thread.sleep(100);
        } while (loadedCursor.baseOffset() < until);

        LOGGER.info("wheel load until {} <= {}", loadedCursor.baseOffset(), until);
    }

    private boolean loadUntilInternal(long until) {
        long index = resolveStartIndex();
        if (index < 0) return true;

        try {
            while (index <= until) {
                ScheduleSetSegment segment = facade.loadScheduleLogSegment(index);
                if (segment == null) {
                    long nextIndex = facade.higherScheduleBaseOffset(index);
                    if (nextIndex < 0) return true;
                    index = nextIndex;
                    continue;
                }

                loadSegment(segment);
                long nextIndex = facade.higherScheduleBaseOffset(index);
                if (nextIndex < 0) return true;

                index = nextIndex;
            }
        } catch (Throwable e) {
            LOGGER.error("wheel load segment failed,currentSegmentOffset:{} until:{}", loadedCursor.baseOffset(), until, e);
            return false;
        }

        return true;
    }

}
