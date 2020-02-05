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
import org.apache.rocketmq.store.delay.model.ScheduleIndex;
import org.apache.rocketmq.store.delay.model.ScheduleSetRecord;
import org.apache.rocketmq.store.delay.model.ScheduleSetSequence;
import org.apache.rocketmq.store.delay.store.visitor.LogVisitor;
import org.apache.rocketmq.store.delay.store.visitor.ScheduleIndexVisitor;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class ScheduleSetSegment extends AbstractDelaySegment<ScheduleSetSequence> {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    ScheduleSetSegment(File file) throws IOException {
        super(file);
    }

    @Override
    public long validate() throws IOException {
        return fileChannel.size();
    }

    ScheduleSetRecord recover(long offset, int size) {
        // 交给gc，不能给每个segment分配一个局部buffer
        ByteBuffer result = ByteBuffer.allocateDirect(size);
        try {
            int bytes = fileChannel.read(result, offset);
            if (bytes != size) {
                DirectBufCloser.close(result);
                LOGGER.error("schedule set segment recovered failed,need read more bytes,segment:{},offset:{},size:{}, readBytes:{}, segmentTotalSize:{}", fileName, offset, size, bytes, fileChannel.size());
                return null;
            }
            result.flip();
            long scheduleTime = result.getLong();
            long sequence = result.getLong();
            result.getInt();

            int messageIdSize = result.getInt();
            byte[] messageId = new byte[messageIdSize];
            result.get(messageId);
            int topicSize = result.getInt();
            byte[] topic = new byte[topicSize];
            result.get(topic);
            return new ScheduleSetRecord(new String(messageId, StandardCharsets.UTF_8), new String(topic, StandardCharsets.UTF_8), scheduleTime, offset, size, sequence, result.slice());
        } catch (Throwable e) {
            LOGGER.error("schedule set segment recovered error,segment:{}, offset-size:{} {}", fileName, offset, size, e);
            return null;
        }
    }

    void loadOffset(long scheduleSetWroteOffset) {
        if (getWrotePosition() != scheduleSetWroteOffset) {
            setWrotePosition(scheduleSetWroteOffset);
            setFlushedPosition(scheduleSetWroteOffset);
            LOGGER.warn("schedule set load offset,exist invalid message,segment base offset:{}, wroteOffset:{}", getSegmentBaseOffset(), scheduleSetWroteOffset);
        }
    }

    public LogVisitor<ScheduleIndex> newVisitor(long from, int singleMessageLimitSize) {
        return new ScheduleIndexVisitor(from, fileChannel, singleMessageLimitSize);
    }

    long doValidate(int singleMessageLimitSize) {
        LOGGER.info("validate schedule log {}", getSegmentBaseOffset());
        LogVisitor<ScheduleIndex> visitor = newVisitor(0, singleMessageLimitSize);
        try {
            while (true) {
                Optional<ScheduleIndex> optionalRecord = visitor.nextRecord();
                if (optionalRecord.isPresent()) {
                    continue;
                }
                break;
            }
            return visitor.visitedBufferSize();
        } finally {
            visitor.close();
        }
    }
}
