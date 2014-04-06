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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * Packet format:
 * 1. Header (5 bytes)
 * 2. BODY (64MB of max size)
 * <p/>
 * Header format:
 * 0        8        16       24       32       40
 * +--------+--------+--------+--------+--------+
 * | opcode |               length              |
 * +--------+--------+--------+--------+--------+
 * <p/>
 * BODY is a byte array of `length` bytes
 */
public class PacketDecoder extends LengthFieldBasedFrameDecoder
{
    private static final int MAX_FRAME_SIZE = 64 * 1024 * 1024;
    Connection connection;

    public PacketDecoder()
    {
        super(MAX_FRAME_SIZE, 1 + 4, 4, 0, 0, true);
        connection = new Connection();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        connection.setChannel(ctx.channel());
    }


    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception
    {
        ByteBuf fullBody = (ByteBuf) super.decode(ctx, in);
        if (null == fullBody)
            return null;

        return Frame.create(fullBody, connection);
    }
}
