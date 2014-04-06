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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class PacketEncoder extends MessageToByteEncoder<Frame>
{

    @Override
    protected void encode(ChannelHandlerContext ctx, Frame frame, ByteBuf out) throws Exception
    {
        ByteBuf header = Unpooled.buffer(Frame.Header.LENGTH);
        Message.Type opType = frame.header.opType;

        header.writeByte(opType.opcode);
        header.writeInt(frame.header.streamId);
        header.writeInt(frame.body.readableBytes());

        ByteBuf framePacked = Unpooled.wrappedBuffer(header, frame.body);
        out.writeBytes(framePacked);
        framePacked.release();
    }
}
