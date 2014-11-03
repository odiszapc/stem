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

import org.stem.coordination.ZNodeAbstract;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Topology extends ZNodeAbstract {

    @Override
    public String name() {
        return "topology"; // TODO: extract to constant
    }

    /**
     *
     */
    public static abstract class Node {

        public final UUID id = UUID.randomUUID();
        public final String description = "";




    }

    /**
     *
     */
    public static class Datacenter extends Node {

        public final String name;
        public final List<Rack> racks = new ArrayList<>();

        public Datacenter(String name) {
            super();
            this.name = name;
        }
    }

    /**
     *
     */
    public static class Rack extends Node {

        public final String name;

        public Rack(String name) {
            super();
            this.name = name;
        }
    }

    /**
     *
     */
    public static class StorageNode extends Node {

        public final InetSocketAddress address;

        public StorageNode(InetSocketAddress address) {
            super();
            this.address = address;
        }
    }

    /**
     *
     */
    public static class Disk extends Node {

        public Disk() {
            super();
        }
    }

    public StorageNode getStorageNodeByDiskUUid(UUID uuid) {
        return null;
    }

}
