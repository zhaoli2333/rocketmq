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

package org.apache.rocketmq.store.delay.store.appender;


import org.apache.rocketmq.store.delay.model.AppendMessageStatus;
import org.apache.rocketmq.store.delay.model.AppendRecordResult;
import org.apache.rocketmq.store.delay.model.LogRecord;
import org.apache.rocketmq.store.delay.model.ScheduleLogSequence;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantLock;

public class ScheduleSetAppender implements LogAppender<ScheduleLogSequence, LogRecord> {

    private final ByteBuffer workingBuffer;
    private final ReentrantLock lock = new ReentrantLock();

    public ScheduleSetAppender(int singleMessageSize) {
        this.workingBuffer = ByteBuffer.allocate(singleMessageSize);
    }

    @Override
    public AppendRecordResult<ScheduleLogSequence> appendLog(LogRecord log) {
        workingBuffer.clear();
        workingBuffer.flip();
        final byte[] topicBytes = log.getTopic().getBytes(StandardCharsets.UTF_8);
        final byte[] messageIdBytes = log.getMessageId().getBytes(StandardCharsets.UTF_8);
        int recordSize = getRecordSize(log, topicBytes.length, messageIdBytes.length);
        workingBuffer.limit(recordSize);

        long scheduleTime = log.getScheduleTime();
        long sequence = log.getSequence();
        workingBuffer.putLong(scheduleTime);
        workingBuffer.putLong(sequence);
        workingBuffer.putInt(log.getPayloadSize());
        workingBuffer.putInt(messageIdBytes.length);
        workingBuffer.put(messageIdBytes);
        workingBuffer.putInt(topicBytes.length);
        workingBuffer.put(topicBytes);
        workingBuffer.put(log.getRecord());
        workingBuffer.flip();
        ScheduleLogSequence record = new ScheduleLogSequence(scheduleTime, sequence);
        return new AppendRecordResult<>(AppendMessageStatus.SUCCESS, 0, recordSize, workingBuffer, record);
    }

    private int getRecordSize(LogRecord record, int topic, int messageId) {
        return 8 + 8
                + 4
                + 4
                + 4
                + topic
                + messageId
                + record.getPayloadSize();
    }

    @Override
    public void lockAppender() {
        lock.lock();
    }

    @Override
    public void unlockAppender() {
        lock.unlock();
    }
}
