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

import com.twitter.crunch.*;
import org.stem.exceptions.TopologyException;
import org.stem.utils.TopologyUtils;

import java.util.*;

import static org.stem.utils.TopologyUtils.addChild;

class CrushAdapter implements AlgorithmAdapter<Long, Long, Topology.Node, Node, Topology.ReplicaSet, List<Node>, Topology, Node> {

    @Override
    public Long convertBucket(Long src) {
        return src;
    }

    private List<Long> convertBucket(List<Long> src) {
        List<Long> result = new ArrayList<>(src.size());
        for (Long orig : src) {
            result.add(convertBucket(orig));
        }
        return result;
    }

    @Override
    public Node convertNode(Topology.Node src) {
        Node node = new Node();
        node.setName(src.id.toString());
        node.setWeight(100);
        node.setType(NodeType.fromClass(src.getClass()).code);
        node.setSelection(Node.Selection.STRAW);
        node.setChildren(new ArrayList<Node>());
        return node;
    }

    @Override
    public List<Node> convertReplicaSet(Topology.ReplicaSet src) {
        List<Node> nodes = new ArrayList<>(src.disks.size());
        for (Topology.Disk disk : src.disks) {
            nodes.add(convertNode(disk));
        }
        return nodes;
    }

    @Override
    public Node convertTopology(Topology src) {
        Node root = TopologyUtils.createRoot("ROOT");
        for (Topology.Datacenter datacenter : src.dataCenters()) {
            Node datacenterNode = addChild(convertNode(datacenter), root);
            for (Topology.Rack rack : datacenter.racks()) {
                Node rackNode = addChild(convertNode(rack), datacenterNode);
                for (Topology.StorageNode storageNode : rack.storageNodes()) {
                    Node storageNodeNode = addChild(convertNode(storageNode), rackNode);
                    for (Topology.Disk disk : storageNode.disks()) {
                        addChild(convertNode(disk), storageNodeNode);
                    }
                }
            }
        }
        return root;
    }

    @Override
    public Map<Long, Topology.ReplicaSet> computeMapping(List<Long> buckets, int rf, Topology topology) {
        PlacementRules rules = new RackIsolationPlacementRules();
        MappingFunction function = new SimpleCRUSHMapping(rf, rules);
        Map<Long, List<Node>> crushMapping = function.computeMapping(convertBucket(buckets), convertTopology(topology));

        Map<Long, Topology.ReplicaSet> dataMapping = new HashMap<>(crushMapping.size());
        for (Map.Entry<Long, List<Node>> entry : crushMapping.entrySet()) {
            Long bucket = entry.getKey();
            List<Node> replicas = entry.getValue();

            Topology.ReplicaSet replicaSet = buildReplicaSet(replicas, topology, rf);
            dataMapping.put(bucket, replicaSet); // TODO: convert bucket type from CRUSH to Application level
        }
        return dataMapping;
    }

    private static Topology.ReplicaSet buildReplicaSet(List<Node> disks, Topology topology, int rf) {
        List<Topology.Disk> replicas = new ArrayList<>(disks.size());
        for (Node diskNode : disks) {
            UUID uuid = uuid(diskNode.getName());
            Topology.Disk disk = topology.findDisk(uuid);
            if (null == disk)
                throw new TopologyException("Disk not found: " + uuid);
            replicas.add(disk);
        }
        if (replicas.size() != rf)
            throw new TopologyException(String.format("Inconsistent number of replicas (%s) for RF=%s", replicas.size(), rf));
        return new Topology.ReplicaSet(replicas);
    }

    private static UUID uuid(String uuid) {
        return UUID.fromString(uuid);
    }


    public static final class CrushNode extends Node {  // Stub in favour of Node class which has too generic name

    }

    private static enum NodeType {
        DC(Topology.Datacenter.class, 1),
        RACK(Topology.Rack.class, 2),
        STORAGE(Topology.StorageNode.class, 4),
        DISK(Topology.Disk.class, 5);

        private static Map<Class<? extends Topology.Node>, NodeType> values = new HashMap<>();

        static {
            for (NodeType val : NodeType.values()) {
                values.put(val.clazz, val);
            }
        }

        public static NodeType fromClass(Class<? extends Topology.Node> clazz) {
            NodeType type = values.get(clazz);
            if (null == type)
                throw new IllegalStateException(String.format("Unknown node type for clazz \"%s\"", clazz.getCanonicalName()));
            return type;
        }

        final Class<? extends Topology.Node> clazz;
        final int code;

        NodeType(Class<? extends Topology.Node> clazz, int code) {

            this.clazz = clazz;
            this.code = code;
        }
    }
}
