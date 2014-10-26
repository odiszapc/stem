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

package org.stem.util;

import com.google.common.collect.Lists;
import com.twitter.crunch.Node;
import com.twitter.crunch.StorageSystemTypes;
import org.stem.coordination.TopoMapping;
import org.stem.domain.StorageNode;
import org.stem.domain.Topology;

import java.util.*;


public class TopologyUtils {

    public static Node createRoot() {
        return createNode(1, "ROOT", 1, StorageSystemTypes.ROOT);
    }

    public static Node createRoot(String name) {
        return createNode(1, name, 1, StorageSystemTypes.ROOT);
    }

    public static Node createDC() {
        return createNode(1, "Datacenter", 1, StorageSystemTypes.DATA_CENTER);
    }

    public static Node createRack(String name) {
        return createNode(null, "RACK-" + name, 1, StorageSystemTypes.RACK);
    }

    public static Node createSingleDCCluster(String name) {
        Node root = createRoot();
        Node dc = createDC();
        root.getChildren().add(dc);
        dc.setParent(root);

        return root;
    }

    public static Node createStorage(String endpoint, List<String> disksIds) {
        Node storage = createNode(null, "STORAGE-" + endpoint, 1, StorageSystemTypes.STORAGE_NODE);
        for (String id : disksIds) {
            Node disk = createDisk(id);
            storage.getChildren().add(disk);
        }
        return storage;
    }


    public static Node createDisk(String id) {
        return createNode(null, "Disk-" + id, 1, StorageSystemTypes.DISK);
    }

    public static Node createNode(Integer id, String name, int weight, int type) {
        Node node = new Node();
        if (null != id)
            node.setId(id);

        node.setName(name);
        node.setWeight(weight);
        node.setSelection(Node.Selection.STRAW);
        node.setType(type);
        node.setChildren(new ArrayList<Node>());
        return node;
    }

    public static List<Long> generateVBucketsIds(int size) {
        List<Long> ranges = Lists.newArrayListWithCapacity(size);

        for (long i = 1; i <= size; i++) {
            ranges.add(i);
        }

        return ranges;
    }

    public static Node createSingleStorageRack(Node storage) {
        Node rack = TopologyUtils.createRack(storage.getName());
        rack.getChildren().add(storage);
        storage.setParent(rack);
        return rack;
    }

    public static void addChildren(Node child, Node parent) {
        parent.getChildren().add(child);
        child.setParent(parent);
    }

    public static TopoMapping buildTopoMap(Topology topology) {
        Map<Long, List<Node>> crushMap = topology.getCRUSHMap();
        Map<UUID, String> diskMap = new HashMap<UUID, String>();

        Map<Long, List<UUID>> bucketMap = new HashMap<Long, List<UUID>>();
        for (Map.Entry<Long, List<Node>> entry : crushMap.entrySet()) {
            // build bucketMap
            Long vBucket = entry.getKey();
            List<Node> disks = entry.getValue();

            List<UUID> disksIds = Lists.newArrayList();
            for (Node disk : disks) {
                String uuidStr = extractDiskUUID(disk.getName());
                UUID uuid = UUID.fromString(uuidStr);
                disksIds.add(uuid);
            }
            bucketMap.put(vBucket, disksIds);

            // build diskMap

            Map<UUID, StorageNode> disksMap = topology.getDisksMap();
            for (Map.Entry<UUID, StorageNode> entry2 : disksMap.entrySet()) {
                UUID diskId = entry2.getKey();
                StorageNode node = entry2.getValue();
                diskMap.put(diskId, node.getEndpoint());
            }
        }

        TopoMapping topoMapping = new TopoMapping(bucketMap, diskMap);

        return topoMapping;
    }

    public static String extractDiskUUID(String name) {
        return name.substring(5);
    }
}
