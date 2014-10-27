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
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.TooLongFrameException;

public class Frame // TODO: rename to Packet
{

    public final Header header;
    public final ByteBuf body;

    public Frame(Header header, ByteBuf body) {
        this.body = body;
        this.header = header;
    }

    public static Frame create(ByteBuf fullFrame) {
        int opcode = fullFrame.readByte();
        int streamId = fullFrame.readInt();
        int length = fullFrame.readInt();
        assert length == fullFrame.readableBytes();

        Frame.Header header = new Frame.Header(streamId, opcode);
        return new Frame(header, fullFrame);
    }

    public static Frame create(int opcode, int streamId, ByteBuf body) {
        Frame.Header header = new Frame.Header(streamId, opcode);
        return new Frame(header, body);
    }

    public Frame with(ByteBuf newBody) {
        return new Frame(header, newBody);
    }

    /**
     *
     */
    public static class Header {

        public static final int LENGTH = 1 + 4 + 4;

        public final int streamId;
        public final int opcode;

        public Header(int streamId, int opcode) {

            this.streamId = streamId;
            this.opcode = opcode;
        }
    }

    public static final class Decoder extends LengthFieldBasedFrameDecoder {

        private static final int MAX_FRAME_LENGTH = 256 * 1024 * 1024;

        public Decoder() {
            super(MAX_FRAME_LENGTH, 1 + 4, 4, 0, 0, true);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            try {
                if (in.readableBytes() < 4)
                    return null;

                ByteBuf frameBuf = (ByteBuf) super.decode(ctx, in);
                if (null == frameBuf)
                    return null;

                return Frame.create(frameBuf);
            } catch (CorruptedFrameException e) {
                throw new ClientInternalError(String.format("Corrupted frame: %s", e.getMessage()));
            } catch (TooLongFrameException e) {
                throw new ClientInternalError(e.getMessage());
            }
        }
    }

    public static class Encoder extends MessageToByteEncoder<Frame> {

        @Override
        protected void encode(ChannelHandlerContext ctx, Frame frame, ByteBuf out) throws Exception {
            ByteBuf header = Unpooled.buffer(Frame.Header.LENGTH);

            header.writeByte(frame.header.opcode);
            header.writeInt(frame.header.streamId);
            header.writeInt(frame.body.readableBytes());

            ByteBuf framePacked = Unpooled.wrappedBuffer(header, frame.body); // TODO: in a single step?
            out.writeBytes(framePacked);
            framePacked.release();
        }
    }
}
