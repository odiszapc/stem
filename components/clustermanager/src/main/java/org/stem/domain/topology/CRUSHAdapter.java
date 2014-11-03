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

import com.twitter.crunch.Node;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CRUSHAdapter implements AlgorithmAdapter<Long, Long, Topology.Node, Node, ReplicaSet<Topology.Disk>, List<Node>, Topology, Node> {

    @Override
    public Node convertNode(Topology.Node src) {
        Node node = new Node();
        node.setName(src.id.toString());
        node.setType(NodeType.fromClass(src.getClass()).code);
        return node;
    }

    @Override
    public Node convertReplicaSet(Topology.Node src) {
        return null;
    }

    @Override
    public Node convertTopology(Topology src) {
        return null;
    }

    @Override
    public MappingFunction mappingFunction() {
        return null;
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
