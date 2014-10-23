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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.exceptions.ConnectionException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionPool
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    final Host host;

    final List<PooledConnection> connections;
    private Session session;
    private final AtomicInteger open;
    private volatile int waiter = 0;
    private final Lock waitLock = new ReentrantLock(true);
    private final Condition hasAvailableConnection = waitLock.newCondition();

    private final Runnable newConnectionTask;

    private final AtomicInteger scheduledForCreation = new AtomicInteger();

    private final AtomicReference<CloseFuture> closeFuture = new AtomicReference<>();

    public ConnectionPool(Host host, Session session) throws ConnectionException
    {
        this.host = host;
        this.session = session;

        this.newConnectionTask = new Runnable()
        {
            @Override
            public void run()
            {
                addConnectionIfUnderMaximum();
                scheduledForCreation.decrementAndGet();
            }
        };

        List<PooledConnection> l = new ArrayList<>(options().getStartConnectionsPerHost());
        for (int i = 0; i < options().getStartConnectionsPerHost(); i++)
        {
            l.add(session.connectionFactory().open(this));
        }
        this.connections = new CopyOnWriteArrayList<>(l);
        this.open = new AtomicInteger(connections.size());

        logger.trace("Created connection pool to host {}", host);
    }

    private PoolingOpts options()
    {
        return session.configuration().getPoolingOpts();
    }

    private boolean addConnectionIfUnderMaximum()
    {
        for (; ; )
        {
            int opened = open.get();
            if (opened >= options().getMaxConnectionsPerHost())
                return false;

            if (open.compareAndSet(opened, opened + 1))
                break;
        }

        if (isClosed())
        {
            open.decrementAndGet();
            return false;
        }

        try
        {
            connections.add(session.connectionFactory().open(this));
            signalAvailableConnection();
            return true;
        }
        catch (ConnectionException e)
        {
            open.decrementAndGet();
            logger.debug("Connection error to {} while creating additional connection", host);
            return false;
        }

    }

    private void signalAvailableConnection()
    {
        if (0 == waiter)
            return;

        waitLock.lock();
        try
        {
            hasAvailableConnection.signal();
        }
        finally
        {
            waitLock.unlock();
        }

    }

    public boolean isClosed()
    {
        return closeFuture.get() != null;
    }
}
