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

package org.apache.rocketmq.store.delay.store;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.delay.model.*;

public abstract class AbstractDelayLog<T> implements Log<RecordResult<T>, LogRecord> {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    SegmentContainer<RecordResult<T>, LogRecord> container;

    AbstractDelayLog(SegmentContainer<RecordResult<T>, LogRecord> container) {
        this.container = container;
    }

    @Override
    public AppendLogResult<RecordResult<T>> append(LogRecord record) {
        String topic = record.getTopic();
        RecordResult<T> result = container.append(record);
        PutMessageStatus status = result.getStatus();
        if (PutMessageStatus.SUCCESS != status) {
            LOGGER.error("appendMessageLog schedule set file error,topic:{},status:{}", topic, status.name());
            return new AppendLogResult<>(MessageProducerCode.STORE_ERROR, status.name(), null);
        }

        return new AppendLogResult<>(MessageProducerCode.SUCCESS, status.name(), result);
    }

    @Override
    public boolean clean(Long key) {
        return container.clean(key);
    }

    @Override
    public void flush() {
        container.flush();
    }

}
