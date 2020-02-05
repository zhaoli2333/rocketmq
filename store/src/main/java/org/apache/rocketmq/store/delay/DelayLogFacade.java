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

import org.apache.rocketmq.store.delay.base.LongHashSet;
import org.apache.rocketmq.store.delay.model.AppendLogResult;
import org.apache.rocketmq.store.delay.model.LogRecord;
import org.apache.rocketmq.store.delay.model.ScheduleIndex;
import org.apache.rocketmq.store.delay.model.ScheduleSetRecord;
import org.apache.rocketmq.store.delay.store.log.DispatchLogSegment;
import org.apache.rocketmq.store.delay.store.log.ScheduleSetSegment;
import org.apache.rocketmq.store.delay.wheel.WheelLoadCursor;

import java.util.function.Consumer;

public interface DelayLogFacade {

    void start();

    void shutdown();

    AppendLogResult<ScheduleIndex> appendScheduleLog(LogRecord event);

    public ScheduleSetRecord recoverLogRecord(final ScheduleIndex scheduleIndex);

    public void appendDispatchLog(LogRecord record);

    public DispatchLogSegment latestDispatchSegment();

    public DispatchLogSegment lowerDispatchSegment(final long baseOffset);

    public ScheduleSetSegment loadScheduleLogSegment(final long segmentBaseOffset);

    public WheelLoadCursor.Cursor loadUnDispatch(final ScheduleSetSegment setSegment, final LongHashSet dispatchedSet, final Consumer<ScheduleIndex> refresh);

    public long higherScheduleBaseOffset(long index);

    public long higherDispatchLogBaseOffset(long segmentBaseOffset);

}
