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

package org.stem.client;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.api.ClusterManagerClient;
import org.stem.api.REST;
import org.stem.coordination.ZookeeperClient;
import org.stem.coordination.ZookeeperFactoryCached;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;


// TODO: implement close(), isClosed()
public class StemCluster {

    private static final Logger logger = LoggerFactory.getLogger(StemCluster.class);

    private static final int DEFAULT_THREAD_KEEP_ALIVE = 30;
    final Manager manager;

    public static StemCluster buildFrom(Initializer initializer) {
        return new StemCluster(initializer.getClusterManagerUrl(), initializer.getConfiguration());
    }

    public StemCluster(String clusterManagerUrl, Configuration configuration) {
        manager = new Manager(clusterManagerUrl, configuration);
    }

    public StemCluster init() {
        this.manager.init();
        return this;
    }

    public Metadata getMetadata() {
        manager.init();
        return manager.metadata;
    }

    public CloseFuture closeAsync() {
        return manager.close();
    }

    public void close() {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            throw DefaultResultFuture.extractCauseFromExecutionException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean isClosed() {
        return manager.closeFuture.get() != null;
    }


    public Session connect() {
        init();
        Session session = manager.newSession();
        session.init();
        return session;
    }

    public Session newSession() {
        return manager.newSession();
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

    public static interface Initializer {

        String getClusterManagerUrl();
        Configuration getConfiguration();
    }

    /**
     * For internal purposes
     */
    class Manager {

        private boolean isInit;
        private volatile boolean isFullyInit;

        Metadata metadata;
        final Set<Session> sessions = new CopyOnWriteArraySet<Session>();
        Configuration configuration;
        final Connection.Factory connectionFactory;

        final ScheduledExecutorService reconnectionExecutor = Executors.newScheduledThreadPool(2, threadFactory("Reconnection-%d"));
        final ListeningExecutorService executor;
        final ListeningExecutorService blockingExecutor;

        ClusterManagerClient managerClient;
        MetaStoreClient metaStoreClient;
        ZookeeperClient coordinationClient;
        ClusterDescriber clusterDescriber;

        final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();

        public Manager(String managerUrl, Configuration configuration) {
            this.metadata = new Metadata(this);
            this.configuration = configuration;
            this.connectionFactory = new Connection.Factory(this, configuration);
            this.executor = newExecutor(Runtime.getRuntime().availableProcessors(), "Stem Client worker-%d");
            this.blockingExecutor = newExecutor(2, "Stem Client blocking tasks worker-%d");
            this.managerClient = new ClusterManagerClient(managerUrl);
            this.clusterDescriber = new ClusterDescriber(this);
        }

        StemCluster getCluster() {
            return StemCluster.this;
        }

        public Connection.Factory getConnectionFactory() {
            return connectionFactory;
        }

        public ReconnectionPolicy reconnectionPolicy() {
            return configuration.getPolicies().getReconnectionPolicy();
        }

        synchronized void init() {
            if (isClosed())
                throw new IllegalStateException("Can't use this StemCluster instance because it was previously closed");
            if (isInit)
                return;

            isInit = true;
            Set<Host> hosts = Sets.newLinkedHashSet(metadata.allHosts());
            try {
                REST.Cluster descriptor = managerClient.describeCluster().getCluster();
                metadata.setDescriptor(descriptor);
                metaStoreClient = new MetaStoreClient(descriptor.getMetaStoreContactPoints());
                metaStoreClient.start();

                coordinationClient = ZookeeperFactoryCached.newClient(descriptor.getZookeeperEndpoint());
                clusterDescriber.start();

                hosts.addAll(metadata.allHosts());

                isFullyInit = true;

                for (Host host : hosts)
                    triggerOnAdd(host);
            } catch (Exception e) {
                close();
                throw new ClientException("Failed to connect to cluster", e);
            }
        }

        private Session newSession() {
            Session session = new Session(StemCluster.this);
            sessions.add(session);
            return session;
        }

        boolean removeSession(Session session) {
            return sessions.remove(session);
        }

        public ListenableFuture<?> triggerOnUp(final Host host) {
            return executor.submit(new ExceptionCatchingRunnable() {
                @Override
                public void runMayThrow() throws InterruptedException, ExecutionException {
                    onUp(host);
                }
            });
        }

        private void onUp(final Host host) throws InterruptedException, ExecutionException {
            onUp(host, blockingExecutor);
        }

        private void onUp(final Host host, ListeningExecutorService poolCreationExecutor) throws InterruptedException, ExecutionException {
            logger.debug("Host {} is UP", host);

            if (isClosed())
                return;

            if (host.state == Host.State.UP)
                return;

            ScheduledFuture<?> scheduledAttempt = host.reconnectionAttempt.getAndSet(null);
            if (scheduledAttempt != null) {
                logger.debug("Cancelling reconnection attempt since node is UP");
                scheduledAttempt.cancel(false);
            }

            logger.trace("Adding/renewing host pools for newly UP host {}", host);

            List<ListenableFuture<Boolean>> futures = new ArrayList<ListenableFuture<Boolean>>(sessions.size());
            for (Session s : sessions)
                futures.add(s.forceRenewPool(host, poolCreationExecutor));

            ListenableFuture<List<Boolean>> f = Futures.allAsList(futures);
            Futures.addCallback(f, new FutureCallback<List<Boolean>>() {
                public void onSuccess(List<Boolean> poolCreationResults) {
                    // If any of the creation failed, they will have signaled a connection failure
                    // which will trigger a reconnection to the node. So don't bother marking UP.
                    if (Iterables.any(poolCreationResults, Predicates.equalTo(false))) {
                        logger.debug("Connection pool cannot be created, not marking {} UP", host);
                        return;
                    }

                    host.setUp();

                    // TODO:
//                    for (Host.StateListener listener : listeners)
//                        listener.onUp(host);
                }

                public void onFailure(Throwable t) {
                    // That future is not really supposed to throw unexpected exceptions
                    if (!(t instanceof InterruptedException))
                        logger.error("Unexpected error while marking node UP: while this shouldn't happen, this shouldn't be critical", t);
                }
            });

            f.get();

            for (Session s : sessions)
                s.updateCreatedPools(blockingExecutor);
        }

        public ListenableFuture<?> triggerOnAdd(final Host host) {
            return executor.submit(new ExceptionCatchingRunnable() {
                @Override
                public void runMayThrow() throws InterruptedException, ExecutionException {
                    onAdd(host);
                }
            });
        }

        private void onAdd(final Host host) throws ExecutionException, InterruptedException { // Executed in a separate thread
            if (isClosed())
                return;

            logger.info("New storage node host {} added", host);

            host.setUp();

            List<ListenableFuture<Boolean>> futures = new ArrayList<ListenableFuture<Boolean>>(sessions.size());
            for (Session s : sessions)
                futures.add(s.maybeAddPool(host, blockingExecutor));

            ListenableFuture<List<Boolean>> f = Futures.allAsList(futures);
            Futures.addCallback(f, new FutureCallback<List<Boolean>>() {
                @Override
                public void onSuccess(List<Boolean> poolCreationResults) {
                    if (Iterables.any(poolCreationResults, Predicates.equalTo(false))) {
                        logger.debug("Connection pool cannot be created, not marking {} UP", host);
                        return;
                    }

                    host.setUp();

                    //TODO: for (Host.StateListener listener : listeners)
                    //    listener.onAdd(host);
                }

                @Override
                public void onFailure(Throwable t) {
                    if (!(t instanceof InterruptedException))
                        logger.error("Unexpected error while adding node: while this shouldn't happen, this shouldn't be critical", t);
                }
            });

            f.get();

            for (Session s : sessions)
                s.updateCreatedPools(blockingExecutor);
        }

        public void removeHost(Host host, boolean isInitialConnection) {
            if (host == null)
                return;

            if (metadata.remove(host)) {
                if (isInitialConnection) {
                    logger.warn("You listed {} in your contact points, but it could not be reached at startup", host);
                } else {
                    logger.info("Host {} removed", host);
                    triggerOnRemove(host);
                }
            }
        }

        public ListenableFuture<?> triggerOnRemove(final Host host) {
            return executor.submit(new ExceptionCatchingRunnable() {
                @Override
                public void runMayThrow() throws InterruptedException, ExecutionException {
                    onRemove(host);
                }
            });
        }

        private void onRemove(Host host) throws InterruptedException, ExecutionException {
            if (isClosed())
                return;

            host.setDown();

            logger.debug("Removing host {}", host);
            clusterDescriber.onRemove(host);
            for (Session s : sessions)
                s.onRemove(host);

            // TODO: for (Host.StateListener listener : listeners)
            //     listener.onRemove(host);
        }

        boolean isClosed() {
            return closeFuture.get() != null;
        }

        private CloseFuture close() {
            CloseFuture future = closeFuture.get();
            if (future != null)
                return future;

            logger.debug("Shutting down");

            reconnectionExecutor.shutdownNow();
            //blockingExecutor.shutdownNow(); TODO: think about this line
            executor.shutdown();

            metaStoreClient.stop();

            List<CloseFuture> futures = new ArrayList<CloseFuture>(sessions.size() + 1);
            futures.add(clusterDescriber.closeAsync());
            for (Session session : sessions)
                futures.add(session.closeAsync());

            future = new ClusterCloseFuture(futures);

            return closeFuture.compareAndSet(null, future)
                    ? future
                    : closeFuture.get();
        }

        public boolean signalConnectionFailure(Host host, ConnectionException exception, boolean isHostAddition, boolean markSuspected) {
            if (!isFullyInit || isClosed())
                return true;

            boolean isDown = host.signalConnectionFailure(exception); // TODO: implement ConvictionPolicy ?
            if (isDown) {
                if (isHostAddition || !markSuspected) {
                    triggerOnDown(host, isHostAddition);
                } else {
                    onSuspected(host);
                }
            }
            return isDown;
        }

        public ListenableFuture<?> triggerOnDown(final Host host) {
            return triggerOnDown(host, false);
        }

        public ListenableFuture<?> triggerOnDown(final Host host, final boolean isHostAddition) {
            return executor.submit(new ExceptionCatchingRunnable() {
                @Override
                public void runMayThrow() throws InterruptedException, ExecutionException {
                    onDown(host, isHostAddition, false);
                }
            });
        }

        private void onDown(final Host host, final boolean isHostAddition, final boolean isSuspectedVerification) throws InterruptedException, ExecutionException {
            logger.debug("Host {} is DOWN", host);

            if (isClosed())
                return;

            if (!isSuspectedVerification && host.state == Host.State.SUSPECT)
                return;

            if (host.reconnectionAttempt.get() != null)
                return;

            boolean wasUp = host.isUp();
            host.setDown();

            clusterDescriber.onDown(host);
            for (Session s : sessions)
                s.onDown(host);

//            TODO:
//            if (wasUp) {
//                for (Host.StateListener listener : listeners)
//                    listener.onDown(host);
//            }

            logger.debug("{} is down, scheduling connection retries", host);
            new AbstractReconnectionHandler(reconnectionExecutor, reconnectionPolicy().newSchedule(), host.reconnectionAttempt) {

                protected Connection tryReconnect() throws ConnectionException, InterruptedException {
                    logger.debug("Trying to reconnect to {}", host);
                    return connectionFactory.open(host);
                }

                protected void onReconnection(Connection connection) {
                    connection.closeAsync();
                    logger.debug("Successful reconnection to {}, setting host UP", host);

                    clusterDescriber.refreshNodeInfo(host);
                    try {
                        if (isHostAddition)
                            onAdd(host);
                        else
                            onUp(host);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        logger.error("Unexpected error while setting node up", e);
                    }
                }

                protected boolean onConnectionException(ConnectionException e, long nextDelayMs) {
                    if (logger.isDebugEnabled())
                        logger.debug("Failed reconnection to {} ({}), scheduling retry in {} milliseconds", host, e.getMessage(), nextDelayMs);
                    return true;
                }

                protected boolean onUnknownException(Exception e, long nextDelayMs) {
                    logger.error(String.format("Unknown error during control connection reconnection, scheduling retry in %d milliseconds", nextDelayMs), e);
                    return true;
                }

            }.start();
        }

        public void onSuspected(final Host host) {
            logger.debug("Host {} is Suspected", host);

            if (isClosed())
                return;

            //triggerOnDown(host);

            synchronized (host) {
                if (!host.setSuspected() || host.reconnectionAttempt.get() != null)
                    return;

                host.initialReconnectionAttempt.set(executor.submit(new ExceptionCatchingRunnable() {
                    @Override
                    public void runMayThrow() throws InterruptedException, ExecutionException {
                        try {
                            connectionFactory.open(host).closeAsync();
                            onUp(host, MoreExecutors.sameThreadExecutor());
                        } catch (Exception e) {
                            onDown(host, false, true);
                        }
                    }
                }));
            }

            clusterDescriber.onSuspected(host);
        }


        public void ensurePoolsSizing() {
            for (Session session : sessions) {
                for (ConnectionPool pool : session.pools.values())
                    pool.ensureCoreConnections();
            }
        }

        private class ClusterCloseFuture extends CloseFuture.Forwarding {

            ClusterCloseFuture(List<CloseFuture> futures) {
                super(futures);
            }

            @Override
            public CloseFuture force() {
                executor.shutdownNow();
                return super.force();
            }

            @Override
            protected void onFuturesDone() {
                (new Thread("Shutdown-checker") {
                    @Override
                    public void run() {
                        try {
                            reconnectionExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                            //blockingExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS); TODO: think about this
                            connectionFactory.shutdown();

                            set(null);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            setException(e);
                        }
                    }
                }).start();
            }
        }
    }


    /**
     *
     */
    public static class Builder implements Initializer {

        private String clusterManagerUrl;
        private ReconnectionPolicy reconnectionPolicy;
        private SocketOpts socketOpts;
        private QueryOpts queryOpts;

        @Override
        public String getClusterManagerUrl() {
            return clusterManagerUrl;
        }

        @Override
        public Configuration getConfiguration() {
            Policies policies = new Policies(
                    null == reconnectionPolicy ? Policies.defaultReconnectionPolicy() : reconnectionPolicy
            );

            return new Configuration(policies,
                    null == socketOpts ? new SocketOpts() : socketOpts,
                    new ProtocolOpts(),
                    new PoolingOpts(),
                    null == queryOpts ? new QueryOpts() : queryOpts
            );
        }

        public Builder withClusterManagerUrl(String url) {
            clusterManagerUrl = url;
            return this;
        }

        public Builder withReconnectionPolicy(ReconnectionPolicy policy) {
            this.reconnectionPolicy = policy;
            return this;
        }

        public Builder withSocketOpts(SocketOpts opts) {
            this.socketOpts = opts;
            return this;
        }

        public Builder withQueryOpts(QueryOpts opts) {
            this.queryOpts = opts;
            return this;
        }

        public StemCluster build() {
            return StemCluster.buildFrom(this);
        }
    }
}
