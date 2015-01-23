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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.StringUtils;
import org.stem.api.response.ErrorResponse;
import org.stem.api.response.StemResponse;
import org.stem.exceptions.EventException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

// TODO:  In case of JSON serialization investigate and start using Mix-in feature (http://wiki.fasterxml.com/JacksonMixInAnnotations)

@JsonDeserialize(using = Event.CustomDeserializer.class)
public class Event extends ZNodeAbstract {

    @JsonCreator
    public static Object test(String name) {
        return null;
    }

    public static enum Type {
        JOIN("join", Join.listener, Join.class);

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
        private Class<? extends StemResponse> clazz;

        Type(String name, Listener listener, Class<? extends StemResponse> clazz) {
            this.name = name;
            this.listener = listener;
            this.clazz = clazz;
        }
    }

    public static Event create(Type type) {
        return new Event(UUID.randomUUID(), type);
    }

    public static Event create(Type type, UUID id) {
        return new Event(id, type);
    }

    UUID id;
    Type type;

    private StemResponse response;
    private long started;
    // long completed;
    // boolean disposable // Should this event be discarded once it fired


    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public void setResponse(StemResponse response) {
        this.response = response;
    }

    public StemResponse getResponse() {
        return response;
    }

    protected Event() {
    }

    protected Event(UUID id, Type type) {
        this.id = id;
        this.type = type;
        this.started = System.nanoTime();
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
            listener.register(future);
            client.listenForZNode(ZookeeperPaths.ASYNC_REQUESTS + '/' + requestId.toString(), listener);
        }

        public interface Callback {

            public void onSet(StemResponse response, long latency);
            //public void register(RequestHandler handler);
        }
    }

    /**
     *
     */
    public static class Listener extends ZookeeperEventListener<Event> {

        public static StemResponse waitFor(UUID requestId, Type type, ZookeeperClient client) throws Exception {
            EventResultFuture future = new EventResultFuture();
            Event.Factory
                    .newHandler(requestId, type, future, client)
                    .start();

            StemResponse result = Uninterruptibles.getUninterruptibly(future);
            if (result instanceof ErrorResponse) {
                ErrorResponse response = (ErrorResponse) result;
                throw new EventException(response.toString());
            }

            return result;
        }

        protected EventResultFuture future;

        public void register(EventResultFuture future) {
            this.future = future;
        }

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

            future.onSet(response, System.nanoTime() - request.started);
        }

        protected void process(StemResponse response) {
        }
    }

    static abstract class AsyncResponseAbstract<T extends StemResponse> extends StemResponse {

    }

    // Events

    public static class Join extends StemResponse {

        public static Listener listener = new Listener() {

        };

        Result result;

        public Result getResult() {
            return result;
        }

        public String getMessage() {
            return message;
        }

        @JsonIgnore
        public boolean isSuccess() {
            return Result.SUCCESS == result;
        }

        public Join() {
        }

        public Join(Result result, String message) {
            this.result = result;
            this.message = message;
        }

        public Join(Exception e) {
            this.result = Result.ERROR;
            this.message = e.getMessage();
        }

        public static enum Result {
            SUCCESS("success"),
            ERROR("error");

            private static Map<String, Result> values = new HashMap<>();

            static {
                for (Result val : Result.values()) {
                    values.put(val.name, val);
                }
            }

            @JsonCreator
            public static Result byName(String name) {
                if (null == name)
                    throw new IllegalStateException("value is empty");

                Result result = values.get(name);
                if (null == result)
                    throw new IllegalStateException(String.format("Invalid value: %s. Available types: %s",
                            name, StringUtils.join(values.keySet(), ", ")));

                return result;
            }

            @JsonValue
            public String getName() {
                return name;
            }

            final String name;

            Result(String name) {
                this.name = name;
            }
        }
    }

    static class CustomDeserializer extends JsonDeserializer<Event> {

        private static ObjectMapper mapper = new ObjectMapper();

        @Override
        public Event deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            ObjectCodec codec = jp.getCodec();
            TreeNode treeNode = codec.readTree(jp);


            UUID id = UUID.fromString(((TextNode) treeNode.get("id")).asText());
            String typeStr = ((TextNode) treeNode.get("type")).asText();
            Type type = Type.byName(typeStr);

            TreeNode responseNode = treeNode.get("response");
            StemResponse result = mapper.treeToValue(responseNode, type.clazz);

            Event event = Event.create(type, id);
            event.setResponse(result);
            return event;
        }
    }
}
