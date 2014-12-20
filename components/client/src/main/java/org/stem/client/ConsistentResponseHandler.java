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
import com.google.common.base.Predicate;
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
import java.util.concurrent.atomic.AtomicInteger;

public class ConsistentResponseHandler {

    private static final Logger logger = LoggerFactory.getLogger(ConsistentResponseHandler.class);

    Session session;
    final Consistency.Level consistency;

    Map<Host, ReplicaResponseHandler> handlers = new HashMap<>();
    List<ReplicaResponseHandler> completedHandlers = new CopyOnWriteArrayList<>();


    private int consistencyCondition;
    private final CountDownLatch counter;
    private final AtomicInteger successfulRequests = new AtomicInteger();

    public ConsistentResponseHandler(Session session, List<DefaultResultFuture> futures, Consistency.Level consistency) {
        this.session = session;
        this.consistency = consistency;
        consistencyCondition = consistencyCondition(futures.size());
        counter = new CountDownLatch(consistencyCondition);

        for (DefaultResultFuture future : futures) {
            ReplicaResponseHandler handler = new ReplicaResponseHandler(this, future);
            handlers.put(handler.getEndpoint(), handler);
        }
    }

    public void waitFor() {
        try {
            for (ReplicaResponseHandler handler : handlers.values()) {
                handler.start();
            }
            counter.await();

            if (!isConsistent()) {
                throw new ClientException(
                        String.format("Response is not consistent: %s of %s requests are successful",
                                successfulRequests, consistencyCondition));
            }
        } catch (InterruptedException e) {
            logger.debug("Thread was interrupted. Ignoring.");
        }
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

    void onRequestFinished(ReplicaResponseHandler handler) {
        if (isRunning()) {
            completedHandlers.add(handler);
            if (handler.isSuccess())
                successfulRequests.incrementAndGet();
            counter.countDown();
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

        return Lists.newArrayList(
                Iterables.filter(transformed, new Predicate<Message.Response>() {
                    @Override
                    public boolean apply(Message.Response input) {
                        return input != null;
                    }
                }));
    }

    public Message.Response getResult() {
        List<Message.Response> results = getResults();
        return results.get(index(results.size()));
    }

    private int index(int results) {
        switch (session.configuration().getQueryOpts().getMergeMethod()) {
            case RANDOM:
                return Utils.randInt(1, results);
            default:
                return 1; // TODO: handle other methods
        }
    }
}
