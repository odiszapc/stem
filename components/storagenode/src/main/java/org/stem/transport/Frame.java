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

public class Frame // TODO: rename to Packet
{
    public final Header header;
    public final ByteBuf body;
    public Connection connection;

    public Frame(Header header, ByteBuf body, Connection connection)
    {
        this.body = body;
        this.header = header;
        this.connection = connection;
    }

    public static Frame create(ByteBuf in, Connection connection)
    {
        int opcode = in.readByte();
        Message.Type opType = Message.Type.fromOpcode(opcode);
        int streamId = in.readInt();
        Frame.Header header = new Frame.Header(opType, streamId);
        int length = in.readInt();
        assert length == in.readableBytes();

        return new Frame(header, in, connection);
    }

    public static Frame create(Message.Type type, int streamId, ByteBuf body, Connection connection)
    {
        Frame.Header header = new Frame.Header(type, streamId);
        return new Frame(header, body, connection);
    }


    public static class Header
    {
        public static final int LENGTH = 1 + 4 + 4;
        public Message.Type opType;
        int streamId;

        public Header(Message.Type opType, int streamId)
        {
            this.opType = opType;
            this.streamId = streamId;
        }
    }
}
