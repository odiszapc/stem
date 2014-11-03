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

import com.google.common.collect.Lists;
import org.stem.coordination.ZNodeAbstract;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Topology extends ZNodeAbstract {

    private final Map<UUID, Datacenter> datacenters = new HashMap<>();

    @Override
    public String name() {
        return "topology"; // TODO: extract to constant
    }

    public List<Datacenter> datacenters() {
        return Lists.newArrayList(datacenters.values());
    }

    public void addDatacenter(Datacenter dc) {
        datacenters.put(dc.id, dc);
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
        private final Map<UUID, Rack> racks = new HashMap<>();

        public Datacenter(String name) {
            super();
            this.name = name;
        }

        public List<Rack> racks() {
            return Lists.newArrayList(racks.values());
        }

        public void addRack(Rack rack) {
            racks.put(rack.id, rack);
        }
    }

    /**
     *
     */
    public static class Rack extends Node {

        public final String name;
        private final Map<UUID, StorageNode> storageNodes = new HashMap<>();

        public Rack(String name) {
            super();
            this.name = name;
        }

        public List<StorageNode> storageNodes() {
            return Lists.newArrayList(storageNodes.values());
        }

        public void addStorageNode(StorageNode node) {
            storageNodes.put(node.id, node);
        }
    }

    /**
     *
     */
    public static class StorageNode extends Node {

        public final InetSocketAddress address;
        private final Map<UUID, Disk> disks = new HashMap<>();

        public List<Disk> disks() {
            return Lists.newArrayList(disks.values());
        }

        public StorageNode(InetSocketAddress address) {
            super();
            this.address = address;
        }

        public void addDisk(Disk node) {
            disks.put(node.id, node);
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
