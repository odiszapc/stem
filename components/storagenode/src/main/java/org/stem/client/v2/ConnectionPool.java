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

import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.exceptions.ConnectionException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    private static final int MAX_SIMULTANEOUS_CREATION = 1;
    private static final int MIN_AVAILABLE_STREAMS = 96;

    final Host host;

    final List<PooledConnection> connections;
    private final Set<Connection> trash = new CopyOnWriteArraySet<>();
    private Session session;
    private final AtomicInteger open;
    private volatile int waiter = 0;
    private final Lock waitLock = new ReentrantLock(true);
    private final Condition hasAvailableConnection = waitLock.newCondition();

    private final Runnable newConnectionTask;

    private final AtomicInteger scheduledForCreation = new AtomicInteger();

    private final AtomicReference<CloseFuture> closeFuture = new AtomicReference<>();

    public ConnectionPool(Host host, Session session) throws ConnectionException {
        this.host = host;
        this.session = session;

        this.newConnectionTask = new Runnable() {
            @Override
            public void run() {
                addConnectionIfUnderMaximum();
                scheduledForCreation.decrementAndGet();
            }
        };

        List<PooledConnection> l = new ArrayList<>(options().getStartConnectionsPerHost());
        for (int i = 0; i < options().getStartConnectionsPerHost(); i++) {
            l.add(session.connectionFactory().open(this));
        }
        this.connections = new CopyOnWriteArrayList<>(l);
        this.open = new AtomicInteger(connections.size());

        logger.trace("Created connection pool to host {}", host);
    }

    private PoolingOpts options() {
        return session.configuration().getPoolingOpts();
    }

    public PooledConnection borrowConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
        if (isClosed())
            throw new ConnectionException(host.getAddress(), "Pool is down");

        if (connections.isEmpty()) {
            for (int i = 0; i < options().getStartConnectionsPerHost(); i++) {
                scheduledForCreation.incrementAndGet();
                session.blockingExecutor().submit(newConnectionTask);
            }
            PooledConnection c = waitForConnection(timeout, unit);
            return c;
        }

        int minInFlight = Integer.MAX_VALUE;
        PooledConnection leastBusy = null;
        for (PooledConnection connection : connections) {
            int inFlight = connection.inFlight.get();
            if (inFlight < minInFlight) {
                minInFlight = inFlight;
                leastBusy = connection;
            }
        }

        if (minInFlight >= options().getMaxSimultaneousRequestsPerConnection() && connections.size() < options().getMaxConnectionsPerHost())
            maybeSpawnNewConnection();

        if (null == leastBusy) {
            if (isClosed())
                throw new ConnectionException(host.getAddress(), "Pool is shutdown");
            leastBusy = waitForConnection(timeout, unit);
        } else {
            while (true) {
                int inFlight = leastBusy.inFlight.get();

                if (inFlight >= leastBusy.maxAvailableStreams()) {
                    leastBusy = waitForConnection(timeout, unit);
                    break;
                }

                if (leastBusy.inFlight.compareAndSet(inFlight, inFlight + 1))
                    break;
            }
        }
        return leastBusy;
    }

    public void returnConnection(PooledConnection connection) {
        if (isClosed()) {
            close(connection);
            return;
        }

        int inFlight = connection.inFlight.decrementAndGet();

        if (!connection.isDeactivated()) {
            if (trash.contains(connection) && 0 == inFlight) {
                if (trash.remove(connection))
                    close(connection);
                return;
            }

            if (connections.size() > options().getStartConnectionsPerHost() && inFlight <= options().getMinSimultaneousRequestsPerConnection()) {
                trashConnection(connection);
            } else if (connection.maxAvailableStreams() < MIN_AVAILABLE_STREAMS) {
                replaceConnection(connection);
            } else {
                signalAvailableConnection();
            }
        }
    }

    private void replaceConnection(PooledConnection connection) {
        open.decrementAndGet();
        maybeSpawnNewConnection();
        doTrashConnection(connection);
    }

    private boolean trashConnection(PooledConnection connection) {
        for (; ; ) {
            int opened = open.get();
            if (opened <= options().getStartConnectionsPerHost())
                return false;

            if (open.compareAndSet(opened, opened - 1))
                break;
        }

        doTrashConnection(connection);
        return true;
    }

    private void doTrashConnection(PooledConnection connection) {
        trash.add(connection);
        connections.remove(connection);
        if (0 == connection.inFlight.get() && trash.remove(connection))
            close(connection);
    }

    void replace(final Connection connection) {
        connections.remove(connection);
        connection.close();
        session.blockingExecutor().submit(new Runnable() {
            @Override
            public void run() {
                addConnectionIfUnderMaximum();
            }
        });
    }

    public int opened() {
        return open.get();
    }

    private List<CloseFuture> discardAvailableConnections() {

        List<CloseFuture> futures = new ArrayList<CloseFuture>(connections.size());
        for (Connection connection : connections) {
            CloseFuture future = connection.close();
            future.addListener(new Runnable() {
                public void run() {
                    open.decrementAndGet();
                }
            }, MoreExecutors.sameThreadExecutor());
            futures.add(future);
        }
        return futures;
    }

    public void ensureCoreConnections() {
        if (isClosed())
            return;

        int opened = open.get();
        for (int i = opened; i < options().getStartConnectionsPerHost(); i++) {
            scheduledForCreation.incrementAndGet();
            session.blockingExecutor().submit(newConnectionTask);
        }
    }

    private void close(final Connection connection) {
        connection.close();
    }

    public CloseFuture closeAsync() {

        CloseFuture future = closeFuture.get();
        if (future != null)
            return future;

        // Wake up all threads that waits
        signalAllAvailableConnection();

        future = new CloseFuture.Forwarding(discardAvailableConnections());

        return closeFuture.compareAndSet(null, future)
             ? future
             : closeFuture.get(); // We raced, it's ok, return the future that was actually set
    }

    public boolean isClosed() {
        return closeFuture.get() != null;
    }

    private PooledConnection waitForConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
        long start = System.nanoTime();
        long remaining = timeout;
        do {
            try {
                awaitAvailableConnection(remaining, unit);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                timeout = 0;
            }

            if (isClosed())
                throw new ConnectionException(host.getAddress(), "Pool is shutdown");

            // Looking for a less busy connection
            int minInFlight = Integer.MAX_VALUE;
            PooledConnection leastBusy = null;
            for (PooledConnection connection : connections) {
                int inFlight = connection.inFlight.get();
                if (inFlight < minInFlight) {
                    minInFlight = inFlight;
                    leastBusy = connection;
                }
            }

            if (null != leastBusy) {
                while (true) {
                    int inFlight = leastBusy.inFlight.get();

                    if (inFlight >= leastBusy.maxAvailableStreams())
                        break;

                    if (leastBusy.inFlight.compareAndSet(inFlight, inFlight + 1))
                        return leastBusy;
                }
            }

            remaining = timeout - StemCluster.timeSince(start, unit);
        } while (remaining > 0);

        throw new TimeoutException();
    }

    private boolean addConnectionIfUnderMaximum() {
        for (; ; ) {
            int opened = open.get();
            if (opened >= options().getMaxConnectionsPerHost())
                return false;

            if (open.compareAndSet(opened, opened + 1))
                break;
        }

        if (isClosed()) {
            open.decrementAndGet();
            return false;
        }

        try {
            connections.add(session.connectionFactory().open(this));
            signalAvailableConnection();
            return true;
        }
        catch (ConnectionException e) {
            open.decrementAndGet();
            logger.debug("Connection error to {} while creating additional connection", host);
            return false;
        }

    }


    private void maybeSpawnNewConnection() {
        while (true) {
            int inCreation = scheduledForCreation.get();
            if (inCreation >= MAX_SIMULTANEOUS_CREATION)
                return;
            if (scheduledForCreation.compareAndSet(inCreation, inCreation + 1))
                break;
        }

        logger.debug("Creating new connection on busy pool to {}", host);
        session.blockingExecutor().submit(newConnectionTask);
    }

    private void awaitAvailableConnection(long timeout, TimeUnit unit) throws InterruptedException {
        waitLock.lock();
        waiter++;
        try {
            hasAvailableConnection.await(timeout, unit);
        }
        finally {
            waiter--;
            waitLock.unlock();
        }

    }

    private void signalAvailableConnection() {
        if (0 == waiter)
            return;

        waitLock.lock();
        try {
            hasAvailableConnection.signal();
        }
        finally {
            waitLock.unlock();
        }
    }

    private void signalAllAvailableConnection() {
        if (waiter == 0)
            return;

        waitLock.lock();
        try {
            hasAvailableConnection.signalAll();
        }
        finally {
            waitLock.unlock();
        }
    }


}
