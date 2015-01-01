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
import org.stem.utils.BBUtils;

import java.net.InetSocketAddress;

public class Responses {

    /**
     *
     */
    public static class Error extends Message.Response {

        public static final Decoder<Error> decoder = new Decoder<Error>() {

            @Override
            public Error decode(ByteBuf body) {
                ExceptionCode code = ExceptionCode.fromValue(body.readInt());
                String message = BBUtils.readString(body);
                return new Error(code, message);
            }
        };

        public final ExceptionCode code;
        public final String message;

        public Error(ExceptionCode code, String message) {
            super(Type.ERROR);
            this.code = code;
            this.message = message;
        }

        public ClientException asException(InetSocketAddress host) {
            switch (code) {
                case SERVER_ERROR:
                    return new ClientInternalError(String.format("An unexpected error occurred server side on %s: %s", host, message));
                default:
                    return new ClientInternalError(String.format("Unknown protocol error code %s returned by %s. The error message was: %s", code, host, message));
            }
        }

        @Override
        public String toString() {
            return "ERROR " + code + ": " + message;
        }
    }

    /**
     *
     */
    public static abstract class Result extends Message.Response {

        public static final Message.Decoder<Result> decoder = new Message.Decoder<Result>() {
            @Override
            public Result decode(ByteBuf body) {
                Kind kind = Kind.fromId(body.readInt());
                return kind.subDecoder.decode(body);
            }
        };

        public enum Kind {
            VOID(1, Void.subcodec),
            WRITE_BLOB(2, WriteBlob.subcodec),
            READ_BLOB(3, ReadBlob.subcodec);

            private final int id;
            final Message.Decoder<Result> subDecoder;

            private static final Kind[] ids;

            static {
                int maxId = -1;
                for (Kind k : Kind.values())
                    maxId = Math.max(maxId, k.id);
                ids = new Kind[maxId + 1];
                for (Kind k : Kind.values()) {
                    if (ids[k.id] != null)
                        throw new IllegalStateException("Duplicate kind id");
                    ids[k.id] = k;
                }
            }

            private Kind(int id, Message.Decoder<Result> subDecoder) {
                this.id = id;
                this.subDecoder = subDecoder;
            }

            public static Kind fromId(int id) {
                Kind k = ids[id];
                if (k == null)
                    throw new ClientInternalError(String.format("Unknown kind id %d in RESULT message", id));
                return k;
            }
        }

        public final Kind kind;

        protected Result(Kind kind) {
            super(Type.RESULT);
            this.kind = kind;
        }

        public static class Void extends Result {

            public static final Message.Decoder<Result> subcodec = new Message.Decoder<Result>() {
                @Override
                public Result decode(ByteBuf body) {
                    return new Void();
                }
            };

            protected Void() {
                super(Kind.VOID);
            }

            @Override
            public String toString() {
                return "VOID RESULT";
            }
        }

        public static class ReadBlob extends Result {

            public static final Message.Decoder<Result> subcodec = new Message.Decoder<Result>() {
                @Override
                public Result decode(ByteBuf body) {
                    byte[] data = new byte[body.readableBytes()];
                    body.readBytes(data);
                    return new ReadBlob(data);
                }
            };

            final byte[] data;

            public ReadBlob(byte[] data) {
                super(Kind.READ_BLOB);
                this.data = data;
            }

            @Override
            public String toString() {
                return "READ BLOB (length=" + data.length + ')';
            }
        }

        public static class WriteBlob extends Result {

            public static final Message.Decoder<Result> subcodec = new Message.Decoder<Result>() {

                @Override
                public Result decode(ByteBuf body) {
                    int fatFileIndex = body.readInt();
                    int offset = body.readInt();
                    return new WriteBlob(fatFileIndex, offset);
                }
            };

            final int fatFileIndex;
            final int offset;

            public WriteBlob(int fatFileIndex, int offset) {
                super(Kind.WRITE_BLOB);
                this.fatFileIndex = fatFileIndex;
                this.offset = offset;
            }

            public int getFatFileIndex() {
                return fatFileIndex;
            }

            public int getOffset() {
                return offset;
            }

            @Override
            public String toString() {
                return "WRITE BLOB (ff=" + fatFileIndex + ", offset=" + offset + ')';
            }
        }
    }
}
