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
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;
import org.stem.transport.PacketDecoder;

import java.util.UUID;

public class CodecTest {

    @Test
    public void testName() throws Exception {

        Requests.ReadBlob req = new Requests.ReadBlob(UUID.randomUUID(), 123, 456, 789);
        //ByteBuf buf = Unpooled.buffer(Requests.ReadBlob.coder.encodedSize(req));
        //Requests.ReadBlob.coder.encode(req, buf);

        //Frame frame = Frame.create(buf);

        //PacketDecoder decoder = new PacketDecoder();
        //decoder.

        EmbeddedChannel channel = clientChannel();
        //channel.writeInbound(req);
        channel.writeAndFlush(req);

        Object i = channel.readInbound();
        Object o = channel.readOutbound();
        Object o2 = channel.readOutbound();





    }

    private EmbeddedChannel clientChannel() {
        return new EmbeddedChannel(new Message.ProtocolEncoder(), new Frame.Encoder());
    }


}
