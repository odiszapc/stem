/*
 * Copyright 2014 Alexey Plotnik
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

package org.stem.client.v2;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class StemCluster {
    private static final Logger logger = LoggerFactory.getLogger(StemCluster.class);
    private static final int DEFAULT_THREAD_KEEP_ALIVE = 30;
    final Manager manager;

    public StemCluster(Configuration configuration) {
        manager = new Manager(configuration);
    }

    static long timeSince(long startNanos, TimeUnit destUnit) {
        return destUnit.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    private static ThreadFactory threadFactory(String nameFormat) {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
    }

    private static ListeningExecutorService newExecutor(int threads, String name) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(threads,
                threads,
                DEFAULT_THREAD_KEEP_ALIVE,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                threadFactory(name));
        executor.allowCoreThreadTimeOut(true);
        return MoreExecutors.listeningDecorator(executor);
    }

    class Manager {
        Configuration configuration;
        final Connection.Factory connectionFactory;

        final ListeningExecutorService executor;
        final ListeningExecutorService blockingExecutor;

        public Manager(Configuration configuration) {
            this.configuration = configuration;
            this.connectionFactory = new Connection.Factory(configuration);
            this.executor = newExecutor(Runtime.getRuntime().availableProcessors(), "Stem Client worker-%d");
            this.blockingExecutor = newExecutor(2, "Stem Client blocking tasks worker-%d");
        }

        public Connection.Factory getConnectionFactory() {
            return connectionFactory;
        }

        public ReconnectionPolicy reconnectionPolicy() {
            return configuration.getPolicies().getReconnectionPolicy();
        }
    }
}
