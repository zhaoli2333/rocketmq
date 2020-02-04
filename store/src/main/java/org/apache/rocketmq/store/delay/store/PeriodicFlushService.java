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

import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class PeriodicFlushService implements AutoCloseable {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final String name;
    private final FlushProvider flushProvider;
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> future;

    public PeriodicFlushService(final FlushProvider flushProvider) {
        this.name = flushProvider.getClass().getSimpleName();
        this.flushProvider = flushProvider;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(name));
    }

    public void start() {
        future = scheduler.scheduleWithFixedDelay(
                new FlushRunnable(),
                flushProvider.getInterval(),
                flushProvider.getInterval(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        try {
            if (future != null) {
                future.cancel(false);
            }
            LOGGER.info("will flush one more time for {} before shutdown flush service.", name);
            flushProvider.flush();
        } catch (Exception e) {
            LOGGER.error("shutdown flush service for {} failed.", name, e);
        }
    }

    public interface FlushProvider {
        int getInterval();

        void flush();
    }

    private class FlushRunnable implements Runnable {
        @Override
        public void run() {
            try {
                flushProvider.flush();
            } catch (Throwable e) {
                LOGGER.error("flushProvider {} flush failed.", name, e);
            }
        }
    }
}
