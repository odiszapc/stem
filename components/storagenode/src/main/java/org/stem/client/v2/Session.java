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

import com.datastax.driver.core.Host;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.Striped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.transport.Message;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

public class Session extends AbstractSession {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    final StemCluster cluster;
    final ConcurrentMap<Host, ConnectionPool> pools;
    final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();

    private final Striped<Lock> poolCreationLocks = Striped.lazyWeakLock(5);

    private volatile boolean isInit;
    private volatile boolean isClosing;

    public Session(StemCluster cluster) {
        this.cluster = cluster;
        this.pools = new ConcurrentHashMap<>();
    }

    public synchronized Session init() {
        if (isInit)
            return this;

        cluster.init();

        for (Host host : cluster.getMetadata().allHosts()) {
            try {
                maybeAddPool(host, executor()).get();
            }
            catch (ExecutionException e) {
                throw new StemClientInternalError(e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        isInit = true;
        return this;
    }

    ListenableFuture<Boolean> maybeAddPool(final Host host, ListeningExecutorService executor) {
        ConnectionPool previous = pools.get(host);
        if (previous != null && !previous.isClosed())
            return Futures.immediateFuture(true);

        return executor.submit(new Callable<Boolean>() {
            public Boolean call() {
                try {
                    while (true) {
                        ConnectionPool previous = pools.get(host);
                        if (previous != null && !previous.isClosed())
                            return true;

                        if (replacePool(host, distance, previous)) {
                            logger.debug("Added connection pool for {}", host);
                            return true;
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error creating pool to " + host, e);
                    return false;
                }
            }
        });
    }


    Configuration configuration() {
        return cluster.manager.configuration;
    }

    Connection.Factory connectionFactory() {
        return cluster.manager.connectionFactory;
    }

    ListeningExecutorService executor() {
        return cluster.manager.executor;
    }

    ListeningExecutorService blockingExecutor() {
        return cluster.manager.blockingExecutor;
    }

    @Override
    public DefaultResponseFuture executeAsync(Message.Request request) {
        DefaultResponseFuture future = new DefaultResponseFuture(this, request);
        return future;
    }

    @Override
    public void close() {

    }
}
