/*
 * Copyright 2015 Alexey Plotnik
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

package org.stem.coordination;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.stem.api.REST;
import org.stem.utils.BBUtils;
import org.stem.utils.JsonUtils;
import org.stem.utils.Mappings;

import java.io.IOException;
import java.util.UUID;

public abstract class Codecs {

    public static final ZNode.Codec JSON = new ZNode.Codec() {

        @Override
        public byte[] encode(Object obj) {
            return JsonUtils.encodeBytes(obj);
        }

        @Override
        public <T extends ZNode> T decode(byte[] raw, Class<T> clazz) {
            return JsonUtils.decode(raw, clazz);
        }

    };

    public static final ZNode.Codec TOPOLOGY_SNAPSHOT = new ZNode.Codec() {

        @Override
        public byte[] encode(Object obj) {
            String topologyPacked = JsonUtils.encode(((REST.TopologySnapshot) obj).getTopology());
            byte[] mappingPacked = new Mappings.Encoder(((REST.TopologySnapshot) obj).getMapping()).encode();

            ByteBuf buffer = Unpooled.buffer();
            BBUtils.writeString(topologyPacked, buffer);
            BBUtils.writeBytes(mappingPacked, buffer);

            byte[] result = new byte[buffer.readableBytes()];
            buffer.readBytes(result);
            return result;
        }

        @Override
        public <T extends ZNode> T decode(byte[] raw, Class<T> clazz) {
            ByteBuf buf = Unpooled.wrappedBuffer(raw);
            String topologyPacked = BBUtils.readString(buf);
            REST.Topology topology = JsonUtils.decode(topologyPacked, REST.Topology.class);

            byte[] mappingRaw = new byte[buf.readableBytes()];
            buf.readBytes(mappingRaw);
            REST.Mapping mapping = new Mappings.Decoder(mappingRaw).decode();
            return (T) new REST.TopologySnapshot(topology, mapping);
        }
    };

    public static final ZNode.Codec STREAMING_SESSION = new ZNode.Codec() {

        @Override
        public byte[] encode(Object obj) {
            REST.StreamingSession sess = (REST.StreamingSession) obj;
            return new byte[0];
        }

        @Override
        public <T extends ZNode> T decode(byte[] raw, Class<T> clazz) {
            return null;
        }
    };

    public static class StreamingSessionJsonSerializer extends JsonSerializer<Long[]> {

        @Override
        public void serialize(Long[] partitions, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeStartObject();
            jgen.writeBinaryField(REST.StreamingSession.PARTITIONS_PACKED, packPartitions(partitions));
            jgen.writeEndObject();
        }

        private byte[] packPartitions(Long[] partitions) {
            ByteBuf buf = Unpooled.buffer(partitions.length * 4);
            for (Long p : partitions) {
                buf.writeInt(p.intValue());
            }
            return buf.nioBuffer().array();
        }
    }

    public static class StreamingSessionJsonDeserializer extends JsonDeserializer<Long[]> {

        public StreamingSessionJsonDeserializer() {
        }

        @Override
        public Long[] deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            TreeNode treeNode = jp.getCodec().readTree(jp);
            byte[] partitionPacked = ((TextNode) treeNode.get(REST.StreamingSession.PARTITIONS_PACKED)).binaryValue();
            return decode(partitionPacked);
        }

        private Long[] decode(byte[] bin) {
            ByteBuf buf = Unpooled.wrappedBuffer(bin);

            Long[] result = new Long[buf.readableBytes() / 4];
            int i = 0;
            while (buf.readableBytes() > 0) {
                result[i++] = (long) buf.readInt();
            }
            return result;
        }
    }
}
