package org.apache.rocketmq.store.delay.store.log;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.delay.model.AppendMessageResult;
import org.apache.rocketmq.store.delay.model.LogRecord;
import org.apache.rocketmq.store.delay.store.appender.LogAppender;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class AbstractMappedDelaySegment<T> implements DelaySegment<T> {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final long segmentBaseOffset;
    private final AtomicLong wrotePosition = new AtomicLong(0);
    private final AtomicLong flushedPosition = new AtomicLong(0);
    private final AtomicBoolean needFlush = new AtomicBoolean(true);

    private final String fileName;
    private final MappedFile  mappedFile;

    AbstractMappedDelaySegment(File file) throws IOException {
        this.fileName = file.getAbsolutePath();
        this.segmentBaseOffset = Long.parseLong(file.getName());
        boolean success = false;
        try {
            this.mappedFile = new MappedFile(file.getName(), (int)file.length());
            success = true;
        } catch (IOException e) {
            LOGGER.error("create mapped file failed. file: {}", fileName, e);
            throw e;
        } finally {

        }
    }




    @Override
    public AppendMessageResult<T> append(LogRecord log, LogAppender<T, LogRecord> appender) {
        return null;
    }

    @Override
    public void setWrotePosition(long position) {

    }

    @Override
    public long getWrotePosition() {
        return 0;
    }

    @Override
    public void setFlushedPosition(long position) {

    }

    @Override
    public long getFlushedPosition() {
        return 0;
    }

    @Override
    public long getSegmentBaseOffset() {
        return 0;
    }

    @Override
    public long validate() throws IOException {
        return 0;
    }

    @Override
    public boolean destroy() {
        return false;
    }

    @Override
    public long flush() {
        return 0;
    }
}
