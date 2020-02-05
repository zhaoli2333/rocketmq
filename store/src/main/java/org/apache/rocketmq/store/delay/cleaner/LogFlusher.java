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

package org.apache.rocketmq.store.delay.cleaner;

import org.apache.rocketmq.store.delay.common.Switchable;
import org.apache.rocketmq.store.delay.store.log.DispatchLog;
import org.apache.rocketmq.store.delay.store.PeriodicFlushService;
import org.apache.rocketmq.store.delay.store.log.ScheduleLog;

public class LogFlusher implements Switchable {
    private final PeriodicFlushService dispatchLogFlushService;
    private final PeriodicFlushService scheduleLogFlushService;

    public LogFlusher(ScheduleLog scheduleLog, DispatchLog dispatchLog) {
        this.dispatchLogFlushService = new PeriodicFlushService(dispatchLog.getProvider());
        this.scheduleLogFlushService = new PeriodicFlushService(scheduleLog.getProvider());
    }

    @Override
    public void start() {
        dispatchLogFlushService.start();
        scheduleLogFlushService.start();
    }

    @Override
    public void shutdown() {
        dispatchLogFlushService.close();
        scheduleLogFlushService.close();
    }

}
