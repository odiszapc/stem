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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.api.ClusterManagerClient;
import org.stem.api.response.ClusterResponse;
import org.stem.coordination.ZookeeperClient;
import org.stem.coordination.ZookeeperClientFactory;

import java.util.Set;
import java.util.concurrent.*;


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
     *
     */
    class Manager {

        Metadata metadata;
        final Set<Session> sessions = new CopyOnWriteArraySet<Session>();
        Configuration configuration;
        final Connection.Factory connectionFactory;

        final ListeningExecutorService executor;
        final ListeningExecutorService blockingExecutor;

        ClusterManagerClient managerClient;
        ZookeeperClient coordinationClient;
        ClusterDescriber clusterDescriber;

        public Manager(String managerUrl, Configuration configuration) {
            this.metadata = new Metadata(this);
            this.configuration = configuration;
            this.connectionFactory = new Connection.Factory(configuration);
            this.executor = newExecutor(Runtime.getRuntime().availableProcessors(), "Stem Client worker-%d");
            this.blockingExecutor = newExecutor(2, "Stem Client blocking tasks worker-%d");
            this.managerClient = new ClusterManagerClient(managerUrl);
            this.clusterDescriber = new ClusterDescriber(this);
        }

        public Connection.Factory getConnectionFactory() {
            return connectionFactory;
        }

        public ReconnectionPolicy reconnectionPolicy() {
            return configuration.getPolicies().getReconnectionPolicy();
        }

        synchronized void init() {
            try {
                ClusterResponse clusterResponse = managerClient.describeCluster();
                ClusterResponse.Cluster descriptor = clusterResponse.getCluster();

                coordinationClient = ZookeeperClientFactory.newClient(descriptor.getZookeeperEndpoint());
                clusterDescriber.start();


            } catch (Exception e) {
                throw new ClientException("Can not connect to cluster", e);
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

        public ListenableFuture<?> triggerOnAdd(final Host host) {
            return executor.submit(new ExceptionCatchingRunnable() {
                @Override
                public void runMayThrow() throws InterruptedException, ExecutionException {
                    onAdd(host);
                }
            });
        }

        private void onAdd(final Host host) {
            logger.info("New Stem Storage Node host {} added", host);
        }
    }


    /**
     *
     */
    public static class Builder implements Initializer {

        private String clusterManagerUrl;
        private ReconnectionPolicy reconnectionPolicy;
        private SocketOpts socketOpts;

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
                    new PoolingOpts()
            );
        }

        public Builder withClusterManagerUrl(String url) {
            clusterManagerUrl = url;
            return this;
        }

        public Builder withSocketOpts(SocketOpts opts) {
            this.socketOpts = opts;
            return this;
        }

        public StemCluster build() {
            return StemCluster.buildFrom(this);
        }
    }
}
