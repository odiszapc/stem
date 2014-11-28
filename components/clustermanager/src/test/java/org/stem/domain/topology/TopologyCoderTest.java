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

package org.stem.domain.topology;

import com.fasterxml.jackson.databind.type.ArrayType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;
import org.stem.RestUtils;
import org.stem.api.REST;
import org.stem.utils.BBUtils;
import org.stem.utils.JsonUtils;
import org.stem.utils.Mappings;
import org.stem.utils.TopologyUtils;

import java.net.InetSocketAddress;
import java.util.*;

import static org.stem.utils.Mappings.Encoder.prepareFormat;
import static org.stem.utils.Mappings.NumberFormat.*;

public class TopologyCoderTest {

    @Test
    public void jsonPackAndUnpackEquality() throws Exception {
        DataMapping mapping = prepareMapping(100000, 1);

        REST.Mapping original = RestUtils.packMapping(mapping);
        String encoded = JsonUtils.encode(original);
        REST.Mapping decoded = JsonUtils.decode(encoded, REST.Mapping.class);

        // Assert
        Set<Integer> diskObjIds = new HashSet<>();
        for (Long bucket : decoded.getBuckets()) {
            REST.ReplicaSet originalReplicas = original.getReplicaSet(bucket);
            REST.ReplicaSet decodedReplicas = decoded.getReplicaSet(bucket);
            for (REST.Disk disk : decodedReplicas.getReplicas()) {
                int originalId = System.identityHashCode(disk);
                diskObjIds.add(originalId);
            }

            validateReplicasEquality(originalReplicas, decodedReplicas);
        }

        System.out.println("Length of encoded json: " + encoded.length() + " symbols");
        // Validate we have only 3 unique disk objects
        Assert.assertEquals(3, diskObjIds.size());
    }

    @Test
    public void byteToJson() throws Exception {
        Byte[] original = new Byte[]{1, 2, 3};
        String encoded = JsonUtils.encode(original);

        ArrayType type = JsonUtils.getTypeFactory().constructArrayType(Byte.class);
        Byte[] decoded = (Byte[]) JsonUtils.decode(encoded, type);

        Assert.assertArrayEquals(original, decoded);
    }

    @Test
    public void fitFormatFromBits() throws Exception {
        for (int i = 0; i <= 16; i++)
            Assert.assertEquals("bits=" + i, SHORT, minimalFormat(i));

        for (int i = 17; i <= 32; i++)
            Assert.assertEquals("bits=" + i, INT, minimalFormat(i));

        for (int i = 33; i <= 64; i++)
            Assert.assertEquals("bits=" + i, LONG, minimalFormat(i));
    }

    @Test
    public void prepareFormatFromNumber() throws Exception {
        Assert.assertEquals(SHORT, prepareFormat(1l));
        Assert.assertEquals(SHORT, prepareFormat(65536l - 1));

        Assert.assertEquals(INT, prepareFormat(65536l));
        Assert.assertEquals(INT, prepareFormat(65536l + 1));
        Assert.assertEquals(INT, prepareFormat(4l * 1024 * 1024 * 1024 - 1));

        Assert.assertEquals(LONG, prepareFormat(4l * 1024 * 1024 * 1024 + 1));
    }

    @Test
    public void codeUuid() throws Exception {
        ByteBuf buf = Unpooled.buffer();
        UUID val = UUID.randomUUID();
        BBUtils.writeUuid(val, buf);

        Assert.assertEquals(buf.readableBytes(), 16);
        Assert.assertEquals(val, BBUtils.readUuid(buf));
    }

    @Test
    public void binaryCoderDecoderRfOne() throws Exception {
        REST.Mapping original = RestUtils.packMapping(prepareMapping(100000, 1));
        Mappings.Encoder encoder = new Mappings.Encoder(original);
        byte[] encoded = encoder.encode();

        REST.Mapping decoded = new Mappings.Decoder(encoded).decode();
        assertMappingsEquality(original, decoded);
    }

    @Test
    public void binaryCoderDecoderRfThree() throws Exception {
        REST.Mapping original = RestUtils.packMapping(prepareMapping(100000, 3));
        Mappings.Encoder encoder = new Mappings.Encoder(original);
        byte[] encoded = encoder.encode();

        REST.Mapping decoded = new Mappings.Decoder(encoded).decode();
        assertMappingsEquality(original, decoded);
    }

    @Test
    public void binaryCoderDecoderLarge() throws Exception {
        REST.Mapping original = RestUtils.packMapping(prepareMapping(1000000, 3));
        Mappings.Encoder encoder = new Mappings.Encoder(original);
        byte[] encoded = encoder.encode();

        REST.Mapping decoded = new Mappings.Decoder(encoded).decode();
        assertMappingsEquality(original, decoded);
    }

    @Test
    public void binaryCoderDecoderEmpty() throws Exception {
        REST.Mapping original = new REST.Mapping();

        Mappings.Encoder encoder = new Mappings.Encoder(original);
        byte[] encoded = encoder.encode();
        Assert.assertEquals(0, encoded.length);

        REST.Mapping decoded = new Mappings.Decoder(encoded).decode();
        Assert.assertTrue(decoded.isEmpty());
        assertMappingsEquality(original, decoded);
    }

    @Test
    public void testName() throws Exception {
        Topology topology = createTopology();
        REST.Mapping mapping = RestUtils.packMapping(prepareMapping(topology, 100000, 3));
        REST.TopologySnapshot original = new REST.TopologySnapshot(RestUtils.packTopology(topology), mapping);
        byte[] snapshotEncoded = REST.TopologySnapshot.CODEC.encode(original);

        REST.TopologySnapshot decoded = REST.TopologySnapshot.CODEC.decode(snapshotEncoded, REST.TopologySnapshot.class);
        assertMappingsEquality(original.getMapping(), decoded.getMapping());
        // TODO: compare topologies
    }

    private void assertMappingsEquality(REST.Mapping m1, REST.Mapping m2) {
        Assert.assertEquals(m1.size(), m2.size());

        for (Long b : m1.getBuckets()) {
            Assert.assertEquals(m2.getReplicaSet(b), m1.getReplicaSet(b));
        }

        for (Long b : m2.getBuckets()) {
            Assert.assertEquals(m1.getReplicaSet(b), m2.getReplicaSet(b));
        }
    }

    private void validateReplicasEquality(REST.ReplicaSet original, REST.ReplicaSet decoded) {
        Assert.assertEquals(original.getReplicas(), decoded.getReplicas());
    }

    private DataMapping prepareMapping(int buckets, int rf) {
        return prepareMapping(createTopology(), buckets, rf);
    }

    private DataMapping prepareMapping(Topology topology, int buckets, int rf) {

        Partitioner partitioner = Partitioner.Type.CRUSH.builder.build();
        List<Long> bucketsArray = TopologyUtils.prepareBucketsArray(buckets);
        Map<Long, Topology.ReplicaSet> map = ((CrushAdapter) partitioner.algorithm()).computeMapping(bucketsArray, rf, topology);
        return new DataMapping(map);
    }

    private Topology createTopology() {
        Topology topology = Topology.Factory.create();
        Topology.Datacenter dc = new Topology.Datacenter("DC1");
        topology.addDatacenter(dc);

        Topology.Rack rack1 = new Topology.Rack("RACK1");
        Topology.Rack rack2 = new Topology.Rack("RACK2");
        Topology.Rack rack3 = new Topology.Rack("RACK3");

        dc.addRack(rack1);
        dc.addRack(rack2);
        dc.addRack(rack3);

        Topology.StorageNode node1 = createStorageNode("127.0.0.1", 9997);
        rack1.addStorageNode(node1);
        Topology.StorageNode node2 = createStorageNode("127.0.0.2", 9997);
        rack2.addStorageNode(node2);
        Topology.StorageNode node3 = createStorageNode("127.0.0.3", 9997);
        rack3.addStorageNode(node3);

        Topology.Disk d1 = createDisk("/dev/sda1", 0, 4);
        node1.addDisk(d1);
        Topology.Disk d2 = createDisk("/dev/sda2", 0, 4);
        node2.addDisk(d2);
        Topology.Disk d3 = createDisk("/dev/sda3", 0, 4);
        node3.addDisk(d3);

        return topology;
    }


    private Topology.StorageNode createStorageNode(String host, int port) {
        return new Topology.StorageNode(new InetSocketAddress(host, port));
    }

    private Topology.Disk createDisk(String path, int used, int total) {
        Topology.Disk disk = new Topology.Disk();
        disk.setPath(path);
        disk.setState(Topology.DiskState.RUNNING);
        disk.setUsedBytes(used);
        disk.setTotalBytes(total * 1024 * 1024 * 1024);
        return disk;
    }
}
