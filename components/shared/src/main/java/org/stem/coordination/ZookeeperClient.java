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

package org.stem.coordination;


import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class ZookeeperClient {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);

    private final String host;
    private final int port;

    private CuratorFramework client;

    public static final String HOST_DEFAULT = "localhost";
    public static final int PORT_DEFAULT = 2181;

    private static final int DEFAULT_CONNECTION_TIMEOUT_SEC = 5;

    private final AtomicReference<StateListeningFuture> initializationFuture = new AtomicReference<>();

    CopyOnWriteArrayList<NodeCache> cachePool = new CopyOnWriteArrayList<>();

    ZookeeperClient(String host, int port) throws ZooException {
        this.host = host;
        this.port = port;

        init();
    }

    private synchronized void init() throws ZooException {
        if (null == client || !isRunning()) {
            client = createClient(endpoint());
            client.start();
            //initializationFuture.set(new StateListeningFuture(client, ConnectionState.CONNECTED));
            waitForConnectionEstablished(DEFAULT_CONNECTION_TIMEOUT_SEC, TimeUnit.SECONDS);
        }
    }

    private String endpoint() {
        return host + ':' + port;
    }

    private CuratorFramework createClient(String endpoint) {
        return CuratorFrameworkFactory.newClient(endpoint, new ExponentialBackoffRetry(1000, 3));
    }

    private void waitForConnectionEstablished(int timeout, TimeUnit unit) throws ZooException {
        // We race while waiting for the readiness of client, it's ok
        if (!initializationFuture.compareAndSet(null, new StateListeningFuture(client, ConnectionState.CONNECTED))) {
            StateListeningFuture future = initializationFuture.get();
            try {
                Long duration = Futures.get(future, Exception.class);
                logger.info("Connected to Zookeeper in {}ms", duration / 1000000);
                return;
            } catch (Exception e) {
                logger.error("Error while connecting to {}", endpoint());
                throw new ZooException(String.format("Error while connecting to %s", endpoint()), e);
            }
        }

        // Normal execution
        StateListeningFuture future = initializationFuture.get();
        try {
            Long duration = Uninterruptibles.getUninterruptibly(future, timeout, unit);
            logger.info("Connected to Zookeeper in {}ms", duration / 1000000);
        } catch (ExecutionException e) {
            logger.error("Error while connecting to {}", endpoint());
            throw new ZooException(String.format("Error while connecting to %s", endpoint()));
        } catch (TimeoutException e) {
            logger.error("Connection timeout ({}ms) to {}", future.duration() / 1000000, endpoint());
            ZooException ex = new ZooException(String.format("Connection to %s timed out (%sms)", endpoint(), future.duration() / 1000000));
            future.onTimeout(ex);
            throw ex;
        }
    }

    public boolean isStarted() {
        return client.getState() == CuratorFrameworkState.STARTED;
    }

    public boolean isUninitialized() {
        return client.getState() == CuratorFrameworkState.LATENT;
    }

    public boolean isStopped() {
        return client.getState() == CuratorFrameworkState.STOPPED;
    }

    public synchronized void close() { // TODO: implemente with AtomicReference<CloseFuture>
        logger.info("Close zookeeper client");
        try {
            for (NodeCache cache : cachePool) {
                logger.info("Close cache {}", cache);
                cache.getListenable().clear();
                cache.close();
            }
            cachePool.clear();
        } catch (IOException e) {
            logger.error("Error while closing zookeeper node listening caches", e);
        }
        client.close();

    }

    public void listenChildren(String path, ZNodeEventHandler handler) throws Exception {
        init();
        PathChildrenCache cache = new PathChildrenCache(client, path, true);
        cache.start();

        PathChildrenCacheListener listener = new ZNodeListener(handler);
        cache.getListenable().addListener(listener);
    }

    /**
     * Listen for a single node
     *
     * @param path
     * @param listener
     * @throws Exception
     */
    public void listenForZNode(String path, ZookeeperEventListener listener) throws Exception {
        init();
        // TODO: simplify code of this method to:
        // ZNodeListener nodeListener = new ZNodeListener(path, listener, client);

        NodeCache cache = new NodeCache(client, path);
        cache.start();

        NodeCacheListener cacheListener = new ZNodeListener(
                listener.getHandler(), cache);

        cache.getListenable().addListener(cacheListener);
        cachePool.add(cache);
    }

    public void forcReadListenForZNode(String path, ZookeeperEventListener listener) throws Exception {
        init();
        NodeCache cache = new NodeCache(client, path);
        cache.start();

        NodeCacheListener cacheListener = new ZNodeListener(
                listener.getHandler(), cache);

        cache.getListenable().addListener(cacheListener);
        cacheListener.nodeChanged();
        cachePool.add(cache);
    }

    public void registerListener(ZNodeListener listener) {
        listener.getNodeCache().getListenable().addListener(listener);
    }

    public void unregisterListener(ZNodeListener listener) throws IOException {
        listener.close();
    }

    /* public void unregisterListener(ZookeeperEventListener listener) {
         listener.getZNodeListener().close();
       }


     */

    public void listenForChildren(String path, ZookeeperEventListener listener) throws Exception {
        init();
        PathChildrenCache cache = new PathChildrenCache(client, path, true);
        cache.start();

        PathChildrenCacheListener cacheListener = new ZNodeListener(
                listener.getHandler());

        cache.getListenable().addListener(cacheListener);
    }

// TODO: ZNode must make the following thing possible
//    public void listenForChildren(ZNode zNode, StemZooEventHandler listener) throws Exception {
//
//    }

    /**
     * @param path z-node to create
     * @return indicated whether node actually was created
     * @throws Exception
     */
    public boolean createIfNotExists(String path) throws Exception {
        init();
        try {
            Stat stat = client.checkExists().forPath(path);
            if (null == stat) {
                client.create().creatingParentsIfNeeded().forPath(path, new byte[]{});
                return true;
            }
            return false;
        } catch (Exception e) {
            throw new Exception("Error occurred during interaction with Zookeeper", e);
        }
    }

    public boolean createNodeIfNotExists(String parent, ZNode znode) throws Exception {
        init();
        if (!nodeExists(parent, znode)) {
            createNode(parent, znode);
            return true;
        }
        return false;
    }

    public boolean nodeExists(String parent, ZNode znode) throws Exception {
        init();
        String path = ZKPaths.makePath(parent, znode.name());
        return null != client.checkExists().forPath(path);
    }

    public void createNode(String parent, ZNode znode) throws Exception // TODO: if already exists?
    {
        init();
        String path = ZKPaths.makePath(parent, znode.name());
        createNode(path, znode.encode());
    }

    public void updateNode(String parent, ZNode znode) throws Exception // TODO: automatically add/remove trailing slash
    {
        init();
        String path = ZKPaths.makePath(parent, znode.name());
        updateNode(path, znode.encode());
    }

    public <T extends ZNode> T readZNodeData(String parent, String nodeName, Class<T> clazz) throws Exception {
        init();
        return readZNodeData(ZKPaths.makePath(parent, nodeName), clazz);
    }

    public <T extends ZNode> T readZNodeData(String path, Class<T> clazz) throws Exception {
        init();
        return readZNodeData(path, clazz, Codecs.JSON);
    }

    public <T extends ZNode> T readZNodeData(String path, Class<T> clazz, ZNode.Codec codec) throws Exception {
        init();
        try {
            byte[] data = client.getData().forPath(path);
            if (0 == data.length) {
                return null;
            }

            return codec.decode(data, clazz); // TODO: handle JSON decode error
        } catch (KeeperException.NoNodeException e) {
            return null;
        }
    }

    public void createNode(String path, byte[] data) throws Exception {
        init();
        if (isRunning()) {
            client.create().creatingParentsIfNeeded().forPath(path, data);
        }
    }

    public void updateNode(String path, byte[] data) throws Exception {
        init();
        if (isRunning()) {
            client.setData().forPath(path, data);
        }
    }

    // TODO: this must be redesigned. We save node concatenate parent node path with one from {@link ZNode#name()};
    // We should do that directly to node
    public void saveNode(String path, ZNode node) throws Exception {
        init();
        if (createNodeIfNotExists(path, node))
            return;

        updateNode(path, node);
    }

    public void removeNode(String path) throws Exception {
        init();
        if (isRunning()) {
            client.delete().forPath(path);
        }
    }

    public boolean isRunning() {
        return CuratorFrameworkState.STARTED == client.getState();
    }


    /**
     *
     */
    private static class StateListeningFuture extends AbstractFuture<Long> {

        private final long startTime;
        private volatile long duration;

        public StateListeningFuture(CuratorFramework client, final ConnectionState state) {
            startTime = System.nanoTime();

            Listenable<ConnectionStateListener> connectionStateListenable = client.getConnectionStateListenable();
            connectionStateListenable.addListener(new ConnectionStateListener() {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    if (newState == state) {
                        duration = System.nanoTime() - startTime;
                        set(duration);
                    }
                }
            });
        }

        public long duration() {
            if (isDone()) {
                return duration;
            } else {
                return System.nanoTime() - startTime;
            }
        }

        public void onTimeout(ZooException ex) {
            cancel(true);
            setException(ex);
        }
    }
}
