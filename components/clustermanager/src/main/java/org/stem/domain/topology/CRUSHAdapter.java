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

public class CRUSHAdapter implements AlgorithmAdapter<Long, Long, Topology.Node, Node, Topology, Node> {

    @Override
    public Node convertTopology(Topology src) {
        return null;
    }

    @Override
    public Node convertNode(Topology.Node src) {
        Node node = new Node();
        node.setName(src.id.toString());
        return node;
    }
}
