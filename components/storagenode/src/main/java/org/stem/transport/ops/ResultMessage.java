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
import org.stem.transport.Message;

public abstract class ResultMessage extends Message.Response
{
    public enum Kind
    {
        VOID(1, Void.subcodec),
        WRITE_BLOB(2, WriteBlob.subcodec),
        READ_BLOB(3, ReadBlob.subcodec);


        private int value;
        private Codec<ResultMessage> subcodec;

        private static Kind[] values;

        static
        {
            int maxValue = -1;
            for (Kind type : Kind.values())
                maxValue = Math.max(type.value, maxValue);
            values = new Kind[maxValue + 1];
            for (Kind type : Kind.values())
            {
                values[type.value] = type;
            }
        }

        public static Kind fromValue(int value)
        {
            return values[value];
        }

        Kind(int value, Codec<ResultMessage> codec)
        {
            this.value = value;
            this.subcodec = codec;
        }
    }

    public final Kind kind;

    protected ResultMessage(Kind kind)
    {
        super(Message.Type.RESULT);
        this.kind = kind;
    }

    /**
     * Empty message
     */
    public static class Void extends ResultMessage
    {
        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            @Override
            public ByteBuf encode(ResultMessage op)
            {
                return Unpooled.EMPTY_BUFFER;
            }

            @Override
            public ResultMessage decode(ByteBuf buf)
            {
                return new Void();
            }
        };

        public Void()
        {
            super(Kind.VOID);
        }

        @Override
        public ByteBuf encodeBody()
        {
            return subcodec.encode(this);
        }
    }

    /**
     * Read blob result message
     */
    public static class ReadBlob extends ResultMessage
    {

        final byte[] data;

        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            @Override
            public ByteBuf encode(ResultMessage op)
            {
                ReadBlob readBlobResult = (ReadBlob) op;
                ByteBuf buf = Unpooled.buffer(readBlobResult.data.length);
                buf.writeBytes(readBlobResult.data);
                return buf;
            }

            @Override
            public ResultMessage decode(ByteBuf buf)
            {
                byte[] data = new byte[buf.readableBytes()];
                buf.readBytes(data);
                return new ReadBlob(data);
            }
        };

        public ReadBlob(byte[] data)
        {
            super(Kind.READ_BLOB);
            this.data = data;
        }

        @Override
        public ByteBuf encodeBody()
        {
            return subcodec.encode(this);
        }

        public byte[] getData()
        {
            return data;
        }
    }

    /**
     * Write blob result message
     */
    public static class WriteBlob extends ResultMessage
    {
        final int fatFileIndex;
        final int offset;

        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            @Override
            public ByteBuf encode(ResultMessage op)
            {
                WriteBlob writeBlobResult = (WriteBlob) op;
                ByteBuf buf = Unpooled.buffer(4);
                buf.writeInt(writeBlobResult.fatFileIndex);
                buf.writeInt(writeBlobResult.offset);

                return buf;
            }

            @Override
            public ResultMessage decode(ByteBuf buf)
            {
                int fatFileIndex = buf.readInt();
                int offset = buf.readInt();
                return new WriteBlob(fatFileIndex, offset);
            }
        };

        public WriteBlob(int fatFileIndex, int offset)
        {
            super(Kind.WRITE_BLOB);
            this.fatFileIndex = fatFileIndex;
            this.offset = offset;
        }

        @Override
        public ByteBuf encodeBody()
        {
            return subcodec.encode(this);
        }

        public int getFatFileIndex()
        {
            return fatFileIndex;
        }

        public int getOffset()
        {
            return offset;
        }
    }

    /**
     * Codec
     */
    public static final Codec<ResultMessage> codec = new Codec<ResultMessage>()
    {
        @Override
        public ByteBuf encode(ResultMessage message)
        {
            ByteBuf kbuf = Unpooled.buffer(12);
            kbuf.writeInt(message.kind.value);

            ByteBuf bbuf = message.encodeBody();
            return Unpooled.wrappedBuffer(kbuf, bbuf);
        }

        @Override
        public ResultMessage decode(ByteBuf buf)
        {
            Kind kind = Kind.fromValue(buf.readInt());
            return kind.subcodec.decode(buf);
        }
    };

    @Override
    public ByteBuf encode()
    {
        return codec.encode(this);
    }

    public abstract ByteBuf encodeBody();
}
