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
import org.stem.util.BBUtils;

import java.util.UUID;

public class Requests {

    /**
     *
     */
    public static class ReadBlob extends Message.Request {

        public final UUID diskUuid;  // TODO: encode to bytes, not string
        public final Integer fatFileIndex;
        public final Integer offset;
        public final Integer length;

        public static final Message.Coder<ReadBlob> coder = new Coder<ReadBlob>() {
            @Override
            public void encode(ReadBlob msg, ByteBuf dest) {
                BBUtils.writeString(msg.diskUuid.toString(), dest);
                dest.writeInt(msg.fatFileIndex);
                dest.writeInt(msg.offset);
                dest.writeInt(msg.length);
            }

            @Override
            public int encodedSize(ReadBlob msg) {
                return BBUtils.sizeOfString(msg.diskUuid.toString()) + 12;
            }
        };

        public ReadBlob(UUID diskUuid, int fatFileIndex, int offset, int length) {
            super(Type.READ_BLOB);
            this.diskUuid = diskUuid;
            this.fatFileIndex = fatFileIndex;
            this.offset = offset;
            this.length = length;
        }
    }

    /**
     *
     */
    public static class WriteBlob extends Message.Request {

        public final UUID diskUuid;
        public final byte[] key;
        public final byte[] blob;

        public static final Message.Coder<WriteBlob> coder = new Coder<WriteBlob>() {

            @Override
            public void encode(WriteBlob msg, ByteBuf dest) {
                BBUtils.writeString(msg.diskUuid.toString(), dest);
                dest.writeBytes(msg.key);
                dest.writeInt(msg.blob.length);
                dest.writeBytes(msg.blob);
            }

            @Override
            public int encodedSize(WriteBlob msg) {
                return BBUtils.sizeOfString(msg.diskUuid.toString()) + msg.key.length + 4 + msg.blob.length;
            }
        };

        public WriteBlob(UUID diskUuid, byte[] key, byte[] blob) {
            super(Type.WRITE_BLOB);
            this.diskUuid = diskUuid;
            this.key = key;
            this.blob = blob;
        }
    }

    /**
     *
     */
    public static class DeleteBlob extends Message.Request {

        public final UUID diskUuid;
        public final byte[] key;
        public final byte[] blob;

        public static final Message.Coder<DeleteBlob> coder = new Coder<DeleteBlob>() {

            @Override
            public void encode(DeleteBlob msg, ByteBuf dest) {
                BBUtils.writeString(msg.diskUuid.toString(), dest);
                dest.writeBytes(msg.key);
                dest.writeInt(msg.blob.length);
                dest.writeBytes(msg.blob);
            }

            @Override
            public int encodedSize(DeleteBlob msg) {
                return BBUtils.sizeOfString(msg.diskUuid.toString()) + msg.key.length + 4 + msg.blob.length;
            }
        };

        public DeleteBlob(UUID diskUuid, byte[] key, byte[] blob) {
            super(Type.DELETE_BLOB);
            this.diskUuid = diskUuid;
            this.key = key;
            this.blob = blob;
        }
    }
}
