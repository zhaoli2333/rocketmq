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

import org.apache.rocketmq.store.delay.model.*;

import java.util.Map;

public class ScheduleSet extends AbstractDelayLog<ScheduleSetSequence> {

    ScheduleSet(SegmentContainer<RecordResult<ScheduleSetSequence>, LogRecord> container) {
        super(container);
    }

    ScheduleSetRecord recoverRecord(ScheduleIndex index) {
        return ((ScheduleSetSegmentContainer) container).recover(index.getScheduleTime(), index.getSize(), index.getOffset());
    }

    public void clean() {
        ((ScheduleSetSegmentContainer) container).clean();
    }

    ScheduleSetSegment loadSegment(long segmentBaseOffset) {
        return ((ScheduleSetSegmentContainer) container).loadSegment(segmentBaseOffset);
    }

    synchronized Map<Long, Long> countSegments() {
        return ((ScheduleSetSegmentContainer) container).countSegments();
    }

    void reValidate(final Map<Long, Long> offsets, int singleMessageLimitSize) {
        ((ScheduleSetSegmentContainer) container).reValidate(offsets, singleMessageLimitSize);
    }

    long higherBaseOffset(long low) {
        return ((ScheduleSetSegmentContainer) container).higherBaseOffset(low);
    }
}
