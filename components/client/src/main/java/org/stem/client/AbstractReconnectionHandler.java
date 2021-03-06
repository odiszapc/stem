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

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

abstract class AbstractReconnectionHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(AbstractReconnectionHandler.class);

    private final ScheduledExecutorService executor;
    private final ReconnectionPolicy.ReconnectionSchedule schedule;
    private final AtomicReference<ScheduledFuture<?>> currentAttempt;

    private volatile boolean readyForNext;
    private volatile ScheduledFuture<?> localFuture;

    public AbstractReconnectionHandler(ScheduledExecutorService executor, ReconnectionPolicy.ReconnectionSchedule schedule, AtomicReference<ScheduledFuture<?>> currentAttempt) {
        this.executor = executor;
        this.schedule = schedule;
        this.currentAttempt = currentAttempt;
    }

    protected abstract Connection tryReconnect() throws ConnectionException, InterruptedException;
    protected abstract void onReconnection(Connection connection);

    protected boolean onConnectionException(ConnectionException e, long nextDelayMs) {
        return true;
    }

    protected boolean onUnknownException(Exception e, long nextDelayMs) {
        return true;
    }

    public void start() {
        long firstDelay = schedule.nextDelayMs();
        logger.debug("First reconnection scheduled in {}ms", firstDelay);
        try {
            localFuture = executor.schedule(this, firstDelay, TimeUnit.MILLISECONDS);

            while (true) {
                ScheduledFuture<?> previous = currentAttempt.get();
                if (currentAttempt.compareAndSet(previous, localFuture)) {
                    if (previous != null)
                        previous.cancel(false);
                    break;
                }
            }
            readyForNext = true;
        } catch (RejectedExecutionException e) {
            logger.debug("Aborting reconnection handling since the cluster is shutting down");
        }
    }

    @Override
    public void run() {
        if (localFuture.isCancelled())
            return;

        while (!readyForNext)
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);

        try {
            onReconnection(tryReconnect());
            currentAttempt.compareAndSet(localFuture, null);
        } catch (ConnectionException e) {
            long nextDelay = schedule.nextDelayMs();
            if (onConnectionException(e, nextDelay))
                reschedule(nextDelay);
            else
                currentAttempt.compareAndSet(localFuture, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            reschedule(schedule.nextDelayMs());
        } catch (Exception e) {
            long nextDelay = schedule.nextDelayMs();
            if (onUnknownException(e, nextDelay))
                reschedule(nextDelay);
            else
                currentAttempt.compareAndSet(localFuture, null);
        }
    }

    private void reschedule(long nextDelay) {
        readyForNext = false;
        ScheduledFuture<?> newFuture = executor.schedule(this, nextDelay, TimeUnit.MILLISECONDS);
        assert localFuture != null;
        // If it's not our future the current one, then we've been canceled
        if (!currentAttempt.compareAndSet(localFuture, newFuture)) {
            newFuture.cancel(false);
        }
        localFuture = newFuture;
        readyForNext = true;
    }
}
