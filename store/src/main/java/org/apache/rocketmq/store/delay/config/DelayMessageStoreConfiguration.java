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

package org.apache.rocketmq.store.delay.config;

import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import java.io.File;
import java.util.concurrent.TimeUnit;

public class DelayMessageStoreConfiguration {

    private static final String SCHEDULE_LOG = "schedulelog";
    private static final String DISPATCH_LOG = "dispatchlog";
    private static final String CHECKPOINT = "checkpoint";

    private static final long MS_PER_HOUR = TimeUnit.HOURS.toMillis(1);
    private static final long MS_PER_MINUTE = TimeUnit.MINUTES.toMillis(1);
    private static final long MS_PER_SECONDS = TimeUnit.SECONDS.toMillis(1);
    private static final int SEC_PER_MINUTE = (int) TimeUnit.MINUTES.toSeconds(1);

    private static final int SINGLE_MESSAGE_LIMIT_SIZE = 50 * 1024 * 1024;
    private static final int SEGMENT_LOAD_DELAY_TIMES_IN_MIN = 1;
    private static final int SCHEDULE_CLEAN_BEFORE_DISPATCH_TIMES_IN_HOUR = 24;
    private static final int DEFAULT_LOG_CLEANER_INTERVAL_SECONDS = 60;

    private volatile int segmentScale;
    private volatile long inAdvanceLoadMillis;
    private volatile long loadBlockingExitMillis;

    private final MessageStoreConfig config;

    public DelayMessageStoreConfiguration(MessageStoreConfig config) {
        setup(config);
        this.config = config;
    }

    private void setup(MessageStoreConfig config) {
        int segmentScale = config.getSegmentScale();
        validateArguments((segmentScale >= 5) && (segmentScale <= 60), "segment scale in [5, 60] min");
        int inAdvanceLoadMin = (segmentScale + 1) / 2;
        validateArguments((inAdvanceLoadMin >= 1) && (inAdvanceLoadMin <= ((segmentScale + 1) / 2)), "load in advance time in [1, segmentScale/2] min");
        int loadBlockingExitSec = SEC_PER_MINUTE * (inAdvanceLoadMin + 2) / 3;
        int inAdvanceLoadSec = inAdvanceLoadMin * SEC_PER_MINUTE;
        int loadBlockExitFront = inAdvanceLoadSec / 3;
        int loadBlockExitRear = inAdvanceLoadSec / 2;
        validateArguments((loadBlockingExitSec >= loadBlockExitFront) && (loadBlockingExitSec <= loadBlockExitRear), "load exit block exit time in [inAdvanceLoadMin/3,inAdvanceLoadMin/2] sec. note.");

        this.segmentScale = segmentScale;
        this.inAdvanceLoadMillis = inAdvanceLoadMin * MS_PER_MINUTE;
        this.loadBlockingExitMillis = loadBlockingExitSec * MS_PER_SECONDS;
    }

    private void validateArguments(boolean expression, String errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    public MessageStoreConfig getConfig() {
        return config;
    }


    public String getScheduleLogStorePath() {
        return buildStorePath(SCHEDULE_LOG);
    }

    public String getDispatchLogStorePath() {
        return buildStorePath(DISPATCH_LOG);
    }

    public String getCheckpointStorePath() {
        return buildStorePath(CHECKPOINT);
    }


    public long getDispatchLogKeepTime() {
        return config.getDispatchLogKeepTime() * MS_PER_HOUR;
    }

    public long getCheckCleanTimeBeforeDispatch() {
        return SCHEDULE_CLEAN_BEFORE_DISPATCH_TIMES_IN_HOUR * MS_PER_HOUR;
    }

    public long getLogCleanerIntervalSeconds() {
        return DEFAULT_LOG_CLEANER_INTERVAL_SECONDS;
    }

    public String getScheduleOffsetCheckpointPath() {
        return buildStorePath(CHECKPOINT);
    }

    public long getLoadInAdvanceTimesInMillis() {
        return inAdvanceLoadMillis;
    }

    public long getLoadBlockingExitTimesInMillis() {
        return loadBlockingExitMillis;
    }

    public boolean isDeleteExpiredLogsEnable() {
        return config.isDeleteExpiredLogsEnable();
    }

    public int getSegmentScale() {
        return segmentScale;
    }

    public int getLoadSegmentDelayMinutes() {
        return SEGMENT_LOAD_DELAY_TIMES_IN_MIN;
    }

    public int getSingleMessageLimitSize() {
        return SINGLE_MESSAGE_LIMIT_SIZE;
    }

    public BrokerRole getBrokerRole() {
        return config.getBrokerRole();
    }

    private String buildStorePath(final String name) {
        final String root = config.getStorePathRootDir() + File.separator + "delay";
        return new File(root, name).getAbsolutePath();
    }

}
