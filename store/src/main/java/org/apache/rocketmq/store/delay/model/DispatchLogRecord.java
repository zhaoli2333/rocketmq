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

package org.apache.rocketmq.store.delay.model;

import java.nio.ByteBuffer;

public class DispatchLogRecord implements LogRecord {
    private final LogRecordHeader header;

    public DispatchLogRecord(String topic, String messageId, long scheduleTime, long sequence) {
        this.header = new LogRecordHeader(topic, messageId, scheduleTime, sequence);
    }

    @Override
    public String getTopic() {
        return header.getTopic();
    }

    @Override
    public String getMessageId() {
        return header.getMessageId();
    }

    @Override
    public long getScheduleTime() {
        return header.getScheduleTime();
    }

    @Override
    public int getPayloadSize() {
        return 0;
    }

    @Override
    public ByteBuffer getRecord() {
        return null;
    }

    @Override
    public long getStartWroteOffset() {
        return 0;
    }

    @Override
    public int getRecordSize() {
        return Long.BYTES;
    }

    @Override
    public long getSequence() {
        return header.getSequence();
    }
}
