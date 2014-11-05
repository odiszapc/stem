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
import org.apache.commons.lang3.StringUtils;
import org.stem.api.response.StemResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public abstract class LongTimeRequest {

    public static enum Type {
        JOIN("join", JoinResponse.listener);

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
        public final RequestNodeListener listener;

        Type(String name, RequestNodeListener listener) {
            this.name = name;
            this.listener = listener;
        }
    }

    /**
     *
     */
    public static abstract class Factory {

        public static Handler newHandler(UUID requestId, Type type, LongTimeFuture future, ZookeeperClient client) {
            return new Handler(requestId, future, type.listener, client);
        }
    }

    /**
     *
     */
    public static class Handler {

        private final UUID requestId;
        private final LongTimeFuture future;
        private RequestNodeListener listener;
        private final ZookeeperClient client;

        public Handler(UUID requestId, LongTimeFuture future, RequestNodeListener listener, ZookeeperClient client) {

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
    private abstract static class RequestNodeListener extends ZookeeperEventListener<LongTimeRequest> {

        protected LongTimeFuture future;

        @Override
        public Class<LongTimeRequest> getBaseClass() {
            return LongTimeRequest.class;
        }

        @Override
        protected void onNodeUpdated(LongTimeRequest request) {
            StemResponse response = request.response;
            if (null == response)
                return;

            future.set(response);
        }
    }

    final UUID id;
    final Type type;
    StemResponse response;

    protected LongTimeRequest(UUID id, Type type) {
        this.id = id;
        this.type = type;
    }

    public static class JoinResponse extends StemResponse {

        public static RequestNodeListener listener = new RequestNodeListener() {

        };
    }
}
