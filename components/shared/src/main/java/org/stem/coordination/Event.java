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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.StringUtils;
import org.stem.api.response.StemResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Event extends ZNodeAbstract {

    public static enum Type {
        JOIN("join", Join.listener);

        private static Map<String, Type> values = new HashMap<>();

        static {
            for (Type val : Type.values()) {
                values.put(val.name, val);
            }
        }

        @JsonCreator
        public static Type byName(String name) {
            if (null == name)
                throw new IllegalStateException("Request type is empty");

            Type type = values.get(name);
            if (null == type)
                throw new IllegalStateException(String.format("Invalid request: %s. Available types: %s",
                        name, StringUtils.join(values.keySet(), ", ")));

            return type;
        }

        @JsonValue
        public String getName() {
            return name;
        }

        final String name;
        public final Listener listener;

        Type(String name, Listener listener) {
            this.name = name;
            this.listener = listener;
        }
    }

    public static Event create(Type type) {
        return new Event(UUID.randomUUID(), type);
    }

    public static Event create(Type type, UUID id) {
        return new Event(id, type);
    }

    final UUID id;
    final Type type;
    StemResponse response;
    // final long started;
    // long completed;

    protected Event(UUID id, Type type) {
        this.id = id;
        this.type = type;
        //this.started = System.nanoTime();
    }

    @Override
    public String name() {
        return id.toString();
    }

    /**
     *
     */
    public static abstract class Factory {

        public static Handler newHandler(UUID requestId, Type type, EventResultFuture future, ZookeeperClient client) {
            return new Handler(requestId, future, type.listener, client);
        }
    }

    /**
     *
     */
    public static class Handler {

        private final UUID requestId;
        private final EventResultFuture future;
        private Listener listener;
        private final ZookeeperClient client;

        public Handler(UUID requestId, EventResultFuture future, Listener listener, ZookeeperClient client) {

            this.requestId = requestId;
            this.future = future;
            this.listener = listener;
            this.client = client;
        }

        public void start() throws Exception {
            // TODO: check already started ?
            client.listenForZNode(ZooConstants.ASYNC_REQUESTS + '/' + requestId.toString(), listener);
        }
    }

    /**
     *
     */
    public abstract static class Listener extends ZookeeperEventListener<Event> {

        public static StemResponse waitFor(UUID requestId, Type type, ZookeeperClient client) throws Exception {
            EventResultFuture future = new EventResultFuture();
            Event.Factory
                    .newHandler(requestId, Event.Type.JOIN, future, client)
                    .start();
            StemResponse result = Uninterruptibles.getUninterruptibly(future);

            return result;
        }

        protected EventResultFuture future;

        @Override
        public Class<Event> getBaseClass() {
            return Event.class;
        }

        @Override
        protected void onNodeUpdated(Event request) {
            StemResponse response = request.response;
            if (null == response)
                return;

            process(response);

            future.set(response);
        }

        protected void process(StemResponse response) {}
    }

    // Events

    public static class Join extends StemResponse {

        public static Listener listener = new Listener() {

        };
    }
}
