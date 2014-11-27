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
import org.junit.Assert;
import org.junit.Test;
import org.stem.RestUtils;
import org.stem.api.REST;
import org.stem.utils.JsonUtils;
import org.stem.utils.TopologyUtils;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TopologyCoderTest {

    @Test
    public void jsonPackAndUnpackEquality() throws Exception {
        DataMapping mapping = prepareMapping(100000);

        REST.Mapping original = RestUtils.packMapping(mapping);
        String encoded = JsonUtils.encode(original);
        REST.Mapping decoded = JsonUtils.decode(encoded, REST.Mapping.class);

        // Assert
        Set<Integer> diskObjIds = new HashSet<>();
        for (Long bucket : decoded.getBuckets()) {
            REST.ReplicaSet originalReplicas = original.getReplicas(bucket);
            REST.ReplicaSet decodedReplicas = decoded.getReplicas(bucket);
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
        Byte[] decoded = (Byte[])JsonUtils.decode(encoded, type);

        Assert.assertArrayEquals(original, decoded);
    }

    public void binaryPackUnpack() throws Exception {

    }

    private void validateReplicasEquality(REST.ReplicaSet original, REST.ReplicaSet decoded) {
        Assert.assertEquals(original.getReplicas(), decoded.getReplicas());
    }

    private DataMapping prepareMapping(int buckets) {
        Topology topology = createTopology();
        Partitioner partitioner = Partitioner.Type.CRUSH.builder.build();
        List<Long> bucketsArray = TopologyUtils.prepareBucketsArray(buckets);
        Map<Long, Topology.ReplicaSet> map = ((CrushAdapter) partitioner.algorithm()).computeMapping(bucketsArray, 1, topology);
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
