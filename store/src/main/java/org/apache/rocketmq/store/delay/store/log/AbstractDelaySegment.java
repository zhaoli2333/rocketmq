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
import org.apache.rocketmq.store.delay.model.AppendMessageResult;
import org.apache.rocketmq.store.delay.model.AppendMessageStatus;
import org.apache.rocketmq.store.delay.model.AppendRecordResult;
import org.apache.rocketmq.store.delay.model.LogRecord;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractDelaySegment<T> implements DelaySegment<T> {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final File file;
    private final long segmentBaseOffset;
    private final AtomicLong wrotePosition = new AtomicLong(0);
    private final AtomicLong flushedPosition = new AtomicLong(0);
    private final AtomicBoolean needFlush = new AtomicBoolean(true);

    final String fileName;

    FileChannel fileChannel;

    AbstractDelaySegment(File file) throws IOException {
        this.file = file;
        this.fileName = file.getAbsolutePath();
        this.segmentBaseOffset = Long.parseLong(file.getName());
        boolean success = false;
        try {
            fileChannel = new RandomAccessFile(file, "rw").getChannel();
            success = true;
        } catch (FileNotFoundException e) {
            LOGGER.error("create file channel failed. file: {}", fileName, e);
            throw e;
        } finally {
            if (!success && null != fileChannel) {
                fileChannel.close();
            }
        }
    }

    public abstract void afterAppendSuccess(LogRecord log);

    @Override
    public AppendMessageResult<T> append(LogRecord log, LogAppender<T, LogRecord> appender) {
        appender.lockAppender();
        try {
            long currentPos = wrotePosition.get();
            AppendRecordResult<T> result = appender.appendLog(log);

            AppendMessageStatus status = result.getStatus();
            if (AppendMessageStatus.SUCCESS != result.getStatus()) {
                LOGGER.error("appendMessageLog delay segment error，topic:{},status:{},segment file:{}", log.getTopic(), status.name(), fileName);
                return new AppendMessageResult<>(AppendMessageStatus.UNKNOWN_ERROR, -1, -1);
            }

            int wroteBytes = result.getWroteBytes();

            // This method would not modify this channel's position.
            int writes = fileChannel.write(result.getBuffer(), currentPos);
            if (writes != wroteBytes) {
                LOGGER.error("appendMessageLog delay segment error,appendMessageLog size is ex,segment file:{},record size:{},written:{}", fileName, wroteBytes, writes);
                return new AppendMessageResult<>(AppendMessageStatus.APPEND_FAILED, -1, -1);
            }

            long channelPosition = wrotePosition.addAndGet(wroteBytes);
            this.needFlush.set(true);
            fileChannel.position(channelPosition);
            afterAppendSuccess(log);
            return new AppendMessageResult<>(AppendMessageStatus.SUCCESS, currentPos, wroteBytes, result.getAdditional());
        } catch (Exception e) {
            LOGGER.error("appendMessageLog delay segment error,io ex,segment file:{}", fileName, e);
            return new AppendMessageResult<>(AppendMessageStatus.UNKNOWN_ERROR, -1, -1);
        } finally {
            appender.unlockAppender();
        }
    }

    @Override
    public void setWrotePosition(long position) {
        wrotePosition.set(position);
    }

    @Override
    public long getWrotePosition() {
        return wrotePosition.get();
    }

    @Override
    public long getFlushedPosition() {
        return flushedPosition.get();
    }

    @Override
    public void setFlushedPosition(long position) {
        flushedPosition.set(position);
    }

    @Override
    public long getSegmentBaseOffset() {
        return segmentBaseOffset;
    }

    @Override
    public boolean destroy() {
        close();
        return file.delete();
    }

    private void close() {
        try {
            fileChannel.close();
        } catch (Exception e) {
            LOGGER.error("close file channel failed. file: {}", fileName, e);
        }
    }

    @Override
    public long flush() {
        if (!this.needFlush.get()) {
            return getFlushedPosition();
        }

        long value = wrotePosition.get();
        try {
            fileChannel.force(true);
        } catch (Throwable e) {
            LOGGER.error("Error occurred when flush data to disk.", e);
            return getFlushedPosition();
        }
        flushedPosition.set(value);
        this.needFlush.set(false);

        return getFlushedPosition();
    }

    @Override
    public String toString() {
        return "DelaySegment{" +
                "file=" + fileName +
                "}";
    }

}
