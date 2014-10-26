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

import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.exceptions.ConnectionException;
import org.stem.transport.Message;

import java.util.ArrayList;
import java.util.List;
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
            } catch (ExecutionException e) {
                throw new StemClientInternalError(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        isInit = true;
        return this;
    }

    Configuration configuration() {
        return cluster.manager.configuration;
    }

    Connection.Factory connectionFactory() {
        return cluster.manager.connectionFactory;
    }

    @Override
    public DefaultResponseFuture executeAsync(Message.Request request) {
        DefaultResponseFuture future = new DefaultResponseFuture(this, request);
        return future;
    }

    void execute(RequestHandler.Callback callback) {
        if (!isInit)
            init();
        new RequestHandler(this, callback).sendRequest();
    }

    @Override
    public void close() {

    }

    public CloseFuture closeAsync() {
        CloseFuture future = closeFuture.get();
        if (future != null)
            return future;

        isClosing = true;
        cluster.manager.removeSession(this);

        List<CloseFuture> futures = new ArrayList<CloseFuture>(pools.size());
        for (ConnectionPool pool : pools.values())
            futures.add(pool.closeAsync());

        future = new CloseFuture.Forwarding(futures);

        return closeFuture.compareAndSet(null, future)
                ? future
                : closeFuture.get(); // We raced, it's ok, return the future that was actually set
    }

    public boolean isClosed() {
        return closeFuture.get() != null;
    }

    public StemCluster getCluster() {
        return cluster;
    }

    ReconnectionPolicy reconnectionPolicy() {
        return cluster.manager.reconnectionPolicy();
    }

    ListeningExecutorService executor() {
        return cluster.manager.executor;
    }

    ListeningExecutorService blockingExecutor() {
        return cluster.manager.blockingExecutor;
    }

    ListenableFuture<Boolean> forceRenewPool(final Host host, ListeningExecutorService executor) {
        return executor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                while (true) {
                    try {
                        if (isClosing)
                            return true;

                        ConnectionPool newPool = new ConnectionPool(host, Session.this);
                        ConnectionPool previous = pools.put(host, newPool);
                        if (previous == null) {
                            logger.debug("Added connection pool for {}", host);
                        } else {
                            logger.debug("Renewed connection pool for {}", host);
                            previous.closeAsync();
                        }

                        if (isClosing)
                            newPool.closeAsync();

                        return true;
                    } catch (Exception e) {
                        logger.error("Error creating pool to " + host, e);
                        return false;
                    }
                }
            }
        });
    }

    private boolean replacePool(final Host host, ConnectionPool condition) throws ConnectionException {
        if (isClosing)
            return true;
        Lock l = poolCreationLocks.get(host);
        l.lock();
        try {
            ConnectionPool previous = pools.get(host);
            if (previous != condition)
                return false;

            ConnectionPool newPool = new ConnectionPool(host, this);
            pools.put(host, newPool);

            if (isClosing)
                newPool.closeAsync();

            return true;
        }
        finally {
            l.unlock();
        }
    }

    ListenableFuture<Boolean> maybeAddPool(final Host host, ListeningExecutorService executor) {
        ConnectionPool previous = pools.get(host);
        if (previous != null && !previous.isClosed())
            return Futures.immediateFuture(true);

        return executor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                try {
                    while (true) {
                        ConnectionPool previous = pools.get(host);
                        if (previous != null && !previous.isClosed())
                            return true;

                        if (replacePool(host, previous)) {
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

    void updateCreatedPools(ListeningExecutorService executor) {
        try {
            List<Host> toRemove = new ArrayList<Host>();
            List<ListenableFuture<?>> poolCreationFutures = new ArrayList<ListenableFuture<?>>();

            for (Host h : cluster.getMetadata().allHosts()) {
                ConnectionPool pool = pools.get(h);

                if (pool == null) { // no pool for this host
                    if (h.isUp())
                        poolCreationFutures.add(maybeAddPool(h, executor));
                } else {
                    pool.ensureCoreConnections();
                }
            }


            Futures.allAsList(poolCreationFutures).get();

            List<ListenableFuture<?>> poolRemovalFutures = new ArrayList<ListenableFuture<?>>(toRemove.size());
            for (Host h : toRemove)
                poolRemovalFutures.add(removePool(h));

            Futures.allAsList(poolRemovalFutures).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Unexpected error while refreshing connection pools", e.getCause());
        }
    }

    CloseFuture removePool(Host host) {
        final ConnectionPool pool = pools.remove(host);
        return pool == null
                ? CloseFuture.immediateFuture()
                : pool.closeAsync();
    }

    void onDown(Host host) throws InterruptedException, ExecutionException {
        removePool(host).force().get();
        updateCreatedPools(MoreExecutors.sameThreadExecutor());
    }

    void onSuspected(Host host) {
    }

    void onRemove(Host host) throws InterruptedException, ExecutionException {
        onDown(host);
    }
}
