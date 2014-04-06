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
import org.stem.domain.BlobDescriptor;
import org.stem.transport.Message;

import java.util.UUID;

public class WriteBlobMessage extends Message.Request
{
    public UUID disk;
    public byte[] key;
    public byte[] blob;


    public static final Codec<WriteBlobMessage> codec = new Codec<WriteBlobMessage>()
    {
        @Override
        public ByteBuf encode(WriteBlobMessage op)
        {
            ByteBuf buf = Unpooled.buffer();
            buf.writeBytes(op.disk.toString().getBytes());
            buf.writeBytes(op.key);
            buf.writeInt(op.blob.length);
            buf.writeBytes(op.blob);
            return buf;
        }

        @Override
        public WriteBlobMessage decode(ByteBuf buf)
        {
            WriteBlobMessage op = new WriteBlobMessage();
            byte[] diskBytes = new byte[36];
            buf.readBytes(diskBytes);
            op.disk = UUID.fromString(new String(diskBytes));

            op.key = new byte[16];
            buf.readBytes(op.key);

            int length = buf.readInt();

            op.blob = new byte[length];
            buf.readBytes(op.blob);

            return op;
        }
    };

    public int getBlobSize()
    {
        return blob.length;
    }

    public WriteBlobMessage(UUID disk, byte[] key, byte[] blob)
    {
        this();
        this.disk = disk;
        this.key = key;
        this.blob = blob;
    }

    public WriteBlobMessage()
    {
        super(Type.WRITE_BLOB);
    }


    @Override
    public ByteBuf encode()
    {
        return codec.encode(this);
    }

    @Override
    public Response execute()
    {
        try
        {
            //Thread.sleep(2000);

            BlobDescriptor desc = StorageService.instance.write(this);

            return new ResultMessage.WriteBlob(
                    desc.getFFIndex(),
                    desc.getBodyOffset());

        }
        catch (Exception e)
        {
            return ErrorMessage.fromException(e);
        }
    }
}
