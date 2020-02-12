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

package org.apache.rocketmq.store.delay.store.validator;

import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.delay.config.DelayMessageStoreConfiguration;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class ScheduleLogValidatorSupport extends ConfigManager {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final String SCHEDULE_OFFSET_CHECKPOINT = "schedule_offset_checkpoint.json";

    private final transient DelayMessageStoreConfiguration config;

    private Map<Long, Map<String, Long>> offsetTable =  new HashMap<>();

    public ScheduleLogValidatorSupport(DelayMessageStoreConfiguration config) {
        this.config = config;
    }

    public synchronized Map<Long, Map<String, Long>> getOffsetTable() {
        return offsetTable;
    }

    public synchronized void setOffsetTable(Map<Long, Map<String, Long>> offsetTable) {
        this.offsetTable = offsetTable;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return config.getScheduleOffsetCheckpointPath() + File.separator + SCHEDULE_OFFSET_CHECKPOINT;
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ValidatorSerializeWrapper obj = RemotingSerializable.fromJson(jsonString, ValidatorSerializeWrapper.class);
            if (obj != null) {
                this.offsetTable = obj.getOffsetTable();
            }
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }


}
