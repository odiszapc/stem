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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.utils.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsistentResponseHandler {

    private static final Logger logger = LoggerFactory.getLogger(ConsistentResponseHandler.class);

    Session session;
    final Consistency.Level consistency;

    Map<Host, ReplicaResponseHandler> handlers = new HashMap<>();
    List<ReplicaResponseHandler> completedHandlers = new CopyOnWriteArrayList<>();

    private long timeoutMs;
    private int consistencyCondition;
    private final CountDownLatch counter;
    private final AtomicInteger successfulRequests = new AtomicInteger();

    public ConsistentResponseHandler(Session session, List<DefaultResultFuture> futures, Consistency.Level consistency, long timeoutMs) {
        this.session = session;
        this.consistency = consistency;
        this.timeoutMs = timeoutMs;
        consistencyCondition = consistencyCondition(futures.size());
        counter = new CountDownLatch(consistencyCondition);

        for (DefaultResultFuture future : futures) {
            ReplicaResponseHandler handler = new ReplicaResponseHandler(this, future);
            handlers.put(handler.getHost(), handler);
        }
    }

    void onRequestFinished(ReplicaResponseHandler handler) {
        if (!isRunning()) return;

        completedHandlers.add(handler);
        if (handler.isSuccess())
            successfulRequests.incrementAndGet();
        counter.countDown();
    }

    public void waitFor() {
        try {
            for (ReplicaResponseHandler handler : this.handlers.values()) {
                handler.startWithTimeout(this.timeoutMs);
            }
            boolean awaitHappenedOnTime = this.counter.await(this.timeoutMs, TimeUnit.MILLISECONDS);
            if (!awaitHappenedOnTime) {
                logger.error("Timed out({} ms) waiting for responses. Netty should have prevented this from happening by timing out down the stack. Contact devs.", this.timeoutMs);
            }

            if (!isConsistent()) {
                for (ReplicaResponseHandler handler : this.completedHandlers) {
                    if (!handler.isSuccess()) {
                        logger.error("Request to {} failed with error: {}", handler.getHost(), handler.getErrorMessage());
                    }
                }

                throw new ClientException(
                        String.format("Response is inconsistent (%s/%s): %s",
                                successfulRequests, consistencyCondition, replicaState()));
            }
        } catch (InterruptedException e) {
            logger.debug("Thread was interrupted. Ignoring.");
        }
    }

    private String replicaState() {
        List<String> messages = Lists.newArrayList();
        Joiner joiner = Joiner.on(", ");
        for (ReplicaResponseHandler handler : handlers.values()) {
            StringBuilder b = new StringBuilder();

            b
                    .append(handler.getHost())
                    .append(" ")
                    .append('(');

            if (!handler.isCompleted())
                b.append("WAITING");
            else if (handler.isSuccess())
                b.append("SUCCESS");
            else
                b.append("FAILED");

            b.append(')');

            messages.add(b.toString());
        }
        return joiner.join(messages);
    }

    private int consistencyCondition(int totalReplicas) {
        switch (consistency) {
            case ONE:
                return 1;
            case TWO:
                return Math.min(totalReplicas, 2);
            case THREE:
                return Math.min(totalReplicas, 3);
            case QUORUM:
                return totalReplicas / 2 + 1;
            case ALL:
                return totalReplicas;
            default:
                return totalReplicas;
        }
    }

    public boolean isRunning() {
        return counter.getCount() > 0;
    }

    private boolean isConsistent() {
        return successfulRequests.get() >= consistencyCondition;
    }

    public List<Message.Response> getResults() {
        waitFor();

        Iterable<Message.Response> transformed = Iterables.transform(completedHandlers, new Function<ReplicaResponseHandler, Message.Response>() {
            @Override
            public Message.Response apply(ReplicaResponseHandler input) {
                return input.getResponse();
            }
        });

        return Lists.newArrayList(Iterables.filter(transformed, Predicates.notNull()));
    }

    public Message.Response getResult() {
        List<Message.Response> results = getResults();
        return results.get(index(results.size()));
    }

    private int index(int results) {
        switch (session.configuration().getQueryOpts().getMergeMethod()) {
            case RANDOM:
                return Utils.randInt(0, results - 1);
            default:
                return 1; // TODO: handle other methods
        }
    }
}
