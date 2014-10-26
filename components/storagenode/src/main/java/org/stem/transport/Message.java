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

package org.stem.transport;

import io.netty.buffer.ByteBuf;
import org.stem.transport.ops.*;


abstract public class Message {

    public final Type type;
    private Connection connection;
    private volatile int streamId;

    public enum Direction {
        REQUEST, RESPONSE
    }

    public enum Type {
        RESULT(0, Direction.RESPONSE, ResultMessage.codec),
        ERROR(1, Direction.RESPONSE, ErrorMessage.codec),
        READ_BLOB(2, Direction.REQUEST, ReadBlobMessage.codec),
        WRITE_BLOB(3, Direction.REQUEST, WriteBlobMessage.codec),
        DELETE_BLOB(4, Direction.REQUEST, DeleteBlobMessage.codec);


        public final int opcode;
        public final Direction direction;
        public final Codec<?> codec;

        Type(int opcode, Direction direction, Codec codec) {
            this.opcode = opcode;
            this.direction = direction;
            this.codec = codec;
        }

        private static Type[] opcodes;

        static {
            int maxOpcode = -1;
            for (Type type : Type.values())
                maxOpcode = Math.max(type.opcode, maxOpcode);
            opcodes = new Type[maxOpcode + 1];
            for (Type type : Type.values()) {
                opcodes[type.opcode] = type;
            }
        }

        public static Type fromOpcode(int opcode) {
            return opcodes[opcode];
        }
    }

    public interface Codec<O extends Message> {

        ByteBuf encode(O op);

        O decode(ByteBuf buf);
    }

    public static abstract class Request extends Message {

        protected Request(Type type) {
            super(type);
            assert type.direction == Direction.REQUEST;
        }

        abstract public Response execute();
    }

    public static abstract class Response extends Message {

        protected Response(Type type) {
            super(type);
            assert type.direction == Direction.RESPONSE;
        }

        public boolean isFailed() {
            return this instanceof ErrorMessage;
        }

        public boolean isSuccess() {
            return !isFailed();
        }
    }

    protected Message(Type type) {
        this.type = type;
    }

    public void attach(Connection connection) {
        this.connection = connection;
    }

    public Connection connection() {
        return connection;
    }

    public Message setStreamId(int streamId) {
        this.streamId = streamId;
        return this;
    }

    public int getStreamId() {
        return streamId;
    }

    abstract public ByteBuf encode();
}
