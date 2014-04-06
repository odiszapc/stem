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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

public class MessageDecoder extends MessageToMessageDecoder<Frame>
{
    @Override
    protected void decode(ChannelHandlerContext ctx, Frame frame, List<Object> out) throws Exception
    {
        boolean isRequest = frame.header.opType.direction == Message.Direction.REQUEST;
        Message message = frame.header.opType.codec.decode(frame.body);
        message.setStreamId(frame.header.streamId);
        frame.body.release();
        if (isRequest)
        {
            message.attach(frame.connection);
        }
        out.add(message);
    }
}
