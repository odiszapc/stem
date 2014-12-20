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

import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.domain.ExtendedBlobDescriptor;
import org.stem.exceptions.StemException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

public class Session extends AbstractSession implements StemSession {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    final StemCluster cluster;
    final ConcurrentMap<Host, ConnectionPool> pools;
    final RequestRouter router;
    final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();

    private final Striped<Lock> poolCreationLocks = Striped.lazyWeakLock(5);

    private volatile boolean isInit;
    private volatile boolean isClosing;

    public Session(StemCluster cluster) {
        this.cluster = cluster;
        this.pools = new ConcurrentHashMap<>();
        this.router = new RequestRouter(this);
    }

    public synchronized Session init() {
        if (isInit)
            return this;

        cluster.init();

        for (Host host : cluster.getMetadata().allHosts()) {
            try {
                maybeAddPool(host, executor()).get();
            } catch (ExecutionException e) {
                throw new ClientInternalError(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        isInit = true;
        return this;
    }

    @Override
    public Blob get(byte[] key) {
        List<Requests.ReadBlob> requests = prepareReadRequests(key);
        List<DefaultResultFuture> futures = prepareFutures(requests);
        for (DefaultResultFuture future : futures) {
            // TODO: determine host to send request to
            // TODO: ((Requests.ReadBlob) future.request()).diskUuid;
            new RequestHandler(this, future, future.request()).sendRequest();
        }

        try {
            List<Message.Response> responses = Uninterruptibles.getUninterruptibly(Futures.allAsList(futures));

        } catch (ExecutionException e) {
            throw new StemException("Error while reading blob");
        }

        return Blob.create(key, new byte[]{});
    }

    @Override
    public void put(Blob object) {
        List<Requests.WriteBlob> requests = prepareWriteRequests(object);
        sendAndReceive(requests);
    }

    @Override
    public void delete(byte[] key) {
        List<Requests.DeleteBlob> requests = prepareDeleteRequests(key);
        sendAndReceive(requests);
    }

    private Message.Response sendAndReceive(List<? extends Message.Request> requests) {
        List<DefaultResultFuture> futures = prepareFutures(requests);
        ConsistentResponseHandler responseHandler = new ConsistentResponseHandler(this, futures, configuration().getQueryOpts().getConsistency());

        for (DefaultResultFuture future : futures) {
            new RequestHandler(this, future, future.request()).sendRequest();
        }

        try {
            return responseHandler.getResult();
        } catch (Exception e) {
            logger.error("Error while executing request", e);
            throw new StemException(String.format("Error while sending request %s", e.getMessage()));
        }
    }

    private List<DefaultResultFuture> prepareFutures(List<? extends Message.Request> requests) {
        List<DefaultResultFuture> futures = new ArrayList<>();
        for (Message.Request request : requests)
            futures.add(new DefaultResultFuture(this, request));

        return futures;
    }

    private List<Requests.ReadBlob> prepareReadRequests(byte[] key) {
        List<ExtendedBlobDescriptor> pointers = cluster.manager.metaStoreClient.readMeta(key);
        List<Requests.ReadBlob> requests = new ArrayList<>();
        for (ExtendedBlobDescriptor pointer : pointers)
            requests.add(prepareReadRequest(pointer));

        return requests;
    }

    private List<Requests.DeleteBlob> prepareDeleteRequests(byte[] key) {
        List<ExtendedBlobDescriptor> pointers = cluster.manager.metaStoreClient.readMeta(key);
        List<Requests.DeleteBlob> requests = new ArrayList<>();
        for (ExtendedBlobDescriptor pointer : pointers)
            requests.add(prepareDeleteRequest(pointer));

        return requests;
    }

    private List<Requests.WriteBlob> prepareWriteRequests(Blob obj) {
        List<Requests.WriteBlob> requests = new ArrayList<>();

        Set<UUID> locations = router.getLocationsForBlob(obj);
        for (UUID loc : locations)
            requests.add(new Requests.WriteBlob(loc, obj.key, obj.body));

        return requests;
    }

    private Requests.ReadBlob prepareReadRequest(ExtendedBlobDescriptor pointer) {
        return new Requests.ReadBlob(pointer.getDisk(), pointer.getFFIndex(), pointer.getOffset(), pointer.getLength());
    }

    private Requests.DeleteBlob prepareDeleteRequest(ExtendedBlobDescriptor pointer) {
        return new Requests.DeleteBlob(pointer.getDisk(), pointer.getFFIndex(), pointer.getOffset());
    }

    private Requests.WriteBlob prepareWriteRequest(UUID location, Blob obj) {
        return new Requests.WriteBlob(location, obj.key, obj.body);
    }

    @Override
    public DefaultResultFuture executeAsync(Message.Request request) {
        DefaultResultFuture future = new DefaultResultFuture(this, request);
        execute(future);
        return future;
    }

    void execute(RequestHandler.Callback callback) {
        if (!isInit)
            init();
        new RequestHandler(this, callback, callback.request()).sendRequest();
    }

    Configuration configuration() {
        return cluster.manager.configuration;
    }

    Connection.Factory connectionFactory() {
        return cluster.manager.connectionFactory;
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
                        if (previous != null && !previous.isClosed()) // pool is already alive, just skip it
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
