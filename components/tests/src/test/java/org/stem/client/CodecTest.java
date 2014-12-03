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
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import org.stem.transport.MessageDecoder;
import org.stem.transport.PacketDecoder;
import org.stem.transport.ops.DeleteBlobMessage;
import org.stem.transport.ops.ReadBlobMessage;
import org.stem.transport.ops.WriteBlobMessage;
import org.stem.utils.TestUtils;

import java.util.UUID;

public class CodecTest {

    @Test
    public void readRequestCodec() throws Exception {
        Requests.ReadBlob original = new Requests.ReadBlob(UUID.randomUUID(), 123, 456, 789);
        ReadBlobMessage decoded = emulatePipeline(original);
        compare(original, decoded);
    }

    @Test
    public void deleteRequestCodec() throws Exception {
        Requests.DeleteBlob original = new Requests.DeleteBlob(UUID.randomUUID(), 123, 456);
        DeleteBlobMessage decoded = emulatePipeline(original);
        compare(original, decoded);
    }

    @Test
    public void writeRequestCodec() throws Exception {
        Requests.WriteBlob original = new Requests.WriteBlob(UUID.randomUUID(), TestUtils.generateRandomBlob(16), TestUtils.generateRandomBlob(100));
        WriteBlobMessage decoded = emulatePipeline(original);
        compare(original, decoded);
    }

    private void compare(Requests.ReadBlob original, ReadBlobMessage decoded) {
        Assert.assertEquals(original.diskUuid, decoded.disk);
        Assert.assertEquals(original.fatFileIndex, decoded.fatFileIndex);
        Assert.assertEquals(original.offset, decoded.offset);
        Assert.assertEquals(original.length, decoded.length);
    }

    private void compare(Requests.DeleteBlob original, DeleteBlobMessage decoded) {
        Assert.assertEquals(original.diskUuid, decoded.disk);
        Assert.assertEquals(original.fatFileIndex, decoded.fatFileIndex);
        Assert.assertEquals(original.offset, decoded.offset);
    }

    private void compare(Requests.WriteBlob original, WriteBlobMessage decoded) {
        Assert.assertEquals(original.diskUuid, decoded.disk);
        Assert.assertArrayEquals(original.key, decoded.key);
        Assert.assertArrayEquals(original.blob, decoded.blob);
    }

    <IN extends org.stem.client.Message.Request, OUT extends org.stem.transport.Message.Request> OUT emulatePipeline(IN message) {
        return decodeMessage(encodeMessage(message));
    }


    <T extends org.stem.client.Message.Request> ByteBuf encodeMessage(T req) {
        EmbeddedChannel out = outbound();
        out.writeOutbound(req);
        return (ByteBuf) out.readOutbound();
    }

    <T extends org.stem.transport.Message.Request> T decodeMessage(ByteBuf bytes) {
        EmbeddedChannel in = inbound();
        in.writeInbound(bytes);
        return (T) in.readInbound();
    }

    private EmbeddedChannel outbound() {
        return new EmbeddedChannel(new Frame.Encoder(), new Message.ProtocolEncoder());
    }

    private EmbeddedChannel inbound() {
        return new EmbeddedChannel(new PacketDecoder(), new MessageDecoder());
    }
}
