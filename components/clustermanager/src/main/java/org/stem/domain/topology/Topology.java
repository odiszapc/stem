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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;
import org.stem.coordination.ZNodeAbstract;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Topology extends ZNodeAbstract {

    private final Map<UUID, Datacenter> dataCenters = new HashMap<>();
    private final Index cache = new Index();


    @Override
    public String name() {
        return "topology"; // TODO: extract to constant
    }

    public List<Datacenter> dataCenters() {
        return Lists.newArrayList(dataCenters.values());
    }

    public void addDatacenter(Datacenter dc) {
        dataCenters.put(dc.id, dc);
    }

    public void removeDatacenter(Datacenter dc) {
        dataCenters.remove(dc.id);
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
            rack.datacenter = this;
        }

        public void removeRack(Rack rack) {
            racks.remove(rack.id);
            rack.datacenter = null;
        }
    }

    /**
     *
     */
    public class Rack extends Node {

        private Datacenter datacenter;
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
            node.rack = this;
        }

        public void removeStorageNode(StorageNode node) {
            storageNodes.remove(node.id);
            node.rack = null;
        }
    }

    /**
     *
     */
    public class StorageNode extends Node {

        private Rack rack;
        public final InetSocketAddress address;
        private final Map<UUID, Disk> disks = new HashMap<>();

        public List<Disk> disks() {
            return Lists.newArrayList(disks.values());
        }

        public StorageNode(InetSocketAddress address) {
            super();
            this.address = address;
        }

        public void addDisk(Disk disk) {
            disks.put(disk.id, disk);
            disk.storageNode = this;
        }

        public void removeDisk(Disk disk) {
            disks.remove(disk.id);
            disk.storageNode = null;
            cache.onDiskRemoved(disk);
        }

        public Datacenter datacenter() {
            return rack.datacenter;
        }
    }

    /**
     *
     */
    public class Disk extends Node {

        private StorageNode storageNode;

        public Disk() {
            super();
        }

        public Datacenter datacenter() {
            return storageNode.rack.datacenter;
        }

        public Rack rack() {
            return storageNode.rack;
        }
    }

    public static class ReplicaSet {

        public final List<Disk> disks;

        public ReplicaSet(List<Disk> disks) {
            this.disks = disks;
        }
    }

    public class DataMapping {

    }

    /**
     *
     */
    private final class Index {

        final Map<UUID, Datacenter> dataCenters = new HashMap<>();
        final Map<UUID, Rack> racks = new HashMap<>();
        final Map<UUID, StorageNode> storageNodes = new HashMap<>();
        final Map<UUID, Disk> disks = new HashMap<>();

        // Events

        void onDatacenterAdded(Datacenter dc) {
            dataCenters.put(dc.id, dc);
        }

        void onDatacenterRemoved(Datacenter dc) {
            dataCenters.remove(dc);
        }

        void onRackAdded(Rack rack) {
            racks.put(rack.id, rack);
        }

        void onRackRemoved(Rack rack) {
            racks.remove(rack.id);
        }

        void onStorageNodeAdded(StorageNode node) {
            storageNodes.put(node.id, node);
        }

        void onStorageNodeRemoved(StorageNode node) {
            storageNodes.remove(node.id);
        }

        void onDiskAdded(Disk disk) {
            disks.put(disk.id, disk);
        }

        void onDiskRemoved(Disk disk) {
            disks.remove(disk.id);
        }

        // Find methods

        public Datacenter findDatacenter(UUID id) {
            return dataCenters.get(id);
        }

        public Rack findRack(UUID id) {
            return racks.get(id);
        }

        public StorageNode findStorageNode(UUID id) {
            return storageNodes.get(id);
        }

        public Disk findDisk(UUID id) {
            return disks.get(id);
        }

        public List<Datacenter> findAllDataCenters() {
            return Lists.newArrayList(dataCenters.values());
        }

        public List<Rack> findAllRacks() {
            return Lists.newArrayList(racks.values());
        }

        public List<StorageNode> findAllStorageNodes() {
            return Lists.newArrayList(storageNodes.values());
        }

        public List<StorageNode> findAllStorageNodesInDatacenter(UUID id) {
            final Datacenter dc = dataCenters.get(id);
            assert null != dc;
            return Lists.newArrayList(CollectionUtils.select(storageNodes.values(), new Predicate<StorageNode>() {
                @Override
                public boolean evaluate(StorageNode node) {
                    return node.datacenter().equals(dc);
                }
            }));
        }

        public List<Disk> findAllDisks() {
            return Lists.newArrayList(disks.values());
        }

        public List<Disk> findAllDisksInDatacenter(UUID id) {
            final Datacenter dc = dataCenters.get(id);
            assert null != dc;
            return Lists.newArrayList(CollectionUtils.select(disks.values(), new Predicate<Disk>() {
                @Override
                public boolean evaluate(Disk disk) {
                    return disk.datacenter().equals(dc);
                }
            }));
        }

        public List<Disk> findAllDisksInRack(UUID id) {
            final Rack rack = racks.get(id);
            assert null != rack;
            return Lists.newArrayList(CollectionUtils.select(disks.values(), new Predicate<Disk>() {
                @Override
                public boolean evaluate(Disk disk) {
                    return disk.rack().equals(rack);
                }
            }));
        }
    }

    public Disk findDisk(UUID id) {
        return cache.findDisk(id);
    }


}
