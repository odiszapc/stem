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

package org.stem.transport.ops;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.stem.db.StorageService;
import org.stem.transport.Message;
import org.stem.util.BBUtils;

import java.util.UUID;

public class ReadBlobMessage extends Message.Request {
    public UUID disk;
    public Integer fatFileIndex;
    public Integer offset;
    public Integer length;

    public static final Codec<ReadBlobMessage> codec = new Codec<ReadBlobMessage>() {

        @Override
        public ByteBuf encode(ReadBlobMessage op) {
            ByteBuf buf = Unpooled.buffer();
            BBUtils.writeString(op.disk.toString(), buf);
            buf.writeInt(op.fatFileIndex);
            buf.writeInt(op.offset);
            buf.writeInt(op.length);
            return buf;
        }

        @Override
        public ReadBlobMessage decode(ByteBuf buf) {
            UUID diskId = UUID.fromString(BBUtils.readString(buf));
            int fatFileIndex = buf.readInt();
            int offset = buf.readInt();
            int length = buf.readInt();

            return new ReadBlobMessage(diskId, fatFileIndex, offset, length);
        }
    };

    public ReadBlobMessage(UUID disk, int fatFileIndex, int offset, int length) {
        super(Type.READ_BLOB);
        this.disk = disk;
        this.fatFileIndex = fatFileIndex;
        this.offset = offset;
        this.length = length;
    }


    @Override
    public ByteBuf encode() {
        return codec.encode(this);
    }

    @Override
    public Response execute() {
        try {
            byte[] data = StorageService.instance.read(this);

            return new ResultMessage.ReadBlob(data);

        }
        catch (Exception e) {
            return ErrorMessage.fromException(e);
        }
    }
}
