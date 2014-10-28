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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

abstract class Message {

    protected static final Logger logger = LoggerFactory.getLogger(Message.class);
    private volatile int streamId;

    protected Message() {
    }

    public Message setStreamId(int streamId) {
        this.streamId = streamId;
        return this;
    }

    public int getStreamId() {
        return streamId;
    }

    public interface Coder<R extends Request> {

        public void encode(R request, ByteBuf dest);
        public int encodedSize(R request);
    }

    public interface Decoder<R extends Response> {

        public R decode(ByteBuf body);
    }

    /**
     *
     */
    public static abstract class Request extends Message {

        public final Type type;

        public enum Type {
            READ_BLOB(2, Requests.ReadBlob.coder),
            WRITE_BLOB(3, Requests.WriteBlob.coder),
            DELETE_BLOB(4, Requests.DeleteBlob.coder);

            public final int opcode;
            public final Coder<?> coder;

            private Type(int opcode, Coder<?> coder) {
                this.opcode = opcode;
                this.coder = coder;
            }
        }

        protected Request(Type type) {
            this.type = type;
        }
    }

    /**
     *
     */
    public static abstract class Response extends Message {

        public final Type type;

        protected Response(Type type) {
            this.type = type;
        }

        public enum Type {
            ERROR(2, Responses.Error.decoder),
            RESULT(2, Responses.Result.decoder);

            public final int opcode;
            public final Decoder<?> decoder;

            private static final Type[] opcodeIdx;

            static {
                int maxOpcode = -1;
                for (Type type : Type.values())
                    maxOpcode = Math.max(maxOpcode, type.opcode);
                opcodeIdx = new Type[maxOpcode + 1];
                for (Type type : Type.values()) {
                    if (opcodeIdx[type.opcode] != null)
                        throw new IllegalStateException("Duplicate opcode");
                    opcodeIdx[type.opcode] = type;
                }
            }

            private Type(int opcode, Decoder<?> decoder) {
                this.opcode = opcode;
                this.decoder = decoder;
            }

            public static Type fromOpcode(int opcode) {
                if (opcode < 0 || opcode >= opcodeIdx.length)
                    throw new ClientInternalError(String.format("Unknown response opcode %d", opcode));
                Type t = opcodeIdx[opcode];
                if (t == null)
                    throw new ClientInternalError(String.format("Unknown response opcode %d", opcode));
                return t;
            }
        }
    }

    public static class ProtocolDecoder extends MessageToMessageDecoder<Frame> {

        @Override
        protected void decode(ChannelHandlerContext ctx, Frame frame, List<Object> out) throws Exception {
            Response response = Response.Type.fromOpcode(frame.header.opcode).decoder.decode(frame.body);
            out.add(response);
            frame.body.release(); // TODO: do we need it?
        }
    }

    public static class ProtocolEncoder extends MessageToMessageEncoder<Message> {

        @Override
        protected void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) throws Exception {
            assert msg instanceof Request : "Expecting request, got " + msg;

            Request request = (Request) msg;
            Coder<Request> coder = (Coder<Request>) request.type.coder;
            ByteBuf body = Unpooled.buffer(coder.encodedSize(request));

            coder.encode(request, body);
            out.add(Frame.create(request.type.opcode, request.getStreamId(), body));
        }
    }
}
