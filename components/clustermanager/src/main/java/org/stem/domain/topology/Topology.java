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
import java.util.*;

// TODO: add events when topology changes (node added, node failed, node remover, the same for disks, rack, datacenters, etc)
public class Topology extends ZNodeAbstract {

    public static enum NodeState {
        UNAUTHORIZED, RUNNING, UNAVAILABLE
    }

    public static enum DiskState {
        SUSPEND, RUNNING, UNAVAILABLE
    }

    private final Index cache;
    private final EventSubscriber subscriber;
    private final Map<UUID, Datacenter> dataCenters = new HashMap<>();

    public Topology() {
        cache = new Index();
        subscriber = new EventSubscriber(this);
        subscriber.addListener(cache);

        addDatacenter(new Datacenter("default DC"));
    }

    @Override
    public String name() {
        return "topology"; // TODO: extract to constant
    }

    public List<Datacenter> dataCenters() {
        return Lists.newArrayList(dataCenters.values());
    }

    public void addDatacenter(Datacenter dc) {
        dc.attachSubscriber(this.subscriber);
        dataCenters.put(dc.id, dc);
        subscriber.onDatacenterAdded(dc);
    }

    public void removeDatacenter(Datacenter dc) {
        dataCenters.remove(dc.id);
        subscriber.onDatacenterRemoved(dc);
    }

    /**
     * Base class for all objects in topology: Datacenter, Rack, StorageNode or Disk
     */
    public static abstract class Node {

        public UUID id = UUID.randomUUID();
        public String description = "";

        protected EventSubscriber subscriber = new EventSubscriber();

        public UUID getId() {
            return id;
        }

        public void setId(UUID id) {
            this.id = id;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        protected void attachSubscriber(EventSubscriber subscriber) {
            this.subscriber = subscriber;
        }

        // TODO: make sure we adding just single node, not tree or sub-tree
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
            rack.attachSubscriber(this.subscriber);
            racks.put(rack.id, rack);
            rack.datacenter = this;
            subscriber.onRackAdded(rack);
        }

        public void removeRack(Rack rack) {
            racks.remove(rack.id);
            rack.datacenter = null;
            subscriber.onRackRemoved(rack);
        }
    }

    /**
     *
     */
    public static class Rack extends Node {

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
            node.attachSubscriber(this.subscriber);
            storageNodes.put(node.id, node);
            node.rack = this;
            subscriber.onStorageNodeAdded(node);
        }

        public void removeStorageNode(StorageNode node) {
            storageNodes.remove(node.id);
            node.rack = null;
            subscriber.onStorageNodeRemoved(node);
        }
    }

    /**
     *
     */
    public static class StorageNode extends Node {

        private Rack rack;
        public final InetSocketAddress address;
        String hostname;
        long capacity;

        private NodeState state = NodeState.UNAUTHORIZED;

        private final Map<UUID, Disk> disks = new HashMap<>();

        public List<Disk> disks() {
            return Lists.newArrayList(disks.values());
        }

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }

        public InetSocketAddress getAddress() {
            return address;
        }



        public StorageNode(InetSocketAddress address) {
            super();
            this.address = address;
        }

        public void addDisk(Disk disk) {
            disk.attachSubscriber(this.subscriber);
            disks.put(disk.id, disk);
            disk.storageNode = this;
            subscriber.onDiskAdded(disk);
        }

        public void removeDisk(Disk disk) {
            disks.remove(disk.id);
            disk.storageNode = null;
            subscriber.onDiskRemoved(disk);
        }

        public Datacenter datacenter() {
            return rack.datacenter;
        }

        @Override
        protected void attachSubscriber(EventSubscriber subscriber) {
            super.attachSubscriber(subscriber);
            for (Disk disk : disks.values()) {
                disk.attachSubscriber(subscriber);
            }
        }
    }

    /**
     *
     */
    public static class Disk extends Node {

        private StorageNode storageNode;
        String path;
        long usedBytes = 0;
        long totalBytes = 0;

        private DiskState state;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public long getUsedBytes() {
            return usedBytes;
        }

        public void setUsedBytes(long usedBytes) {
            this.usedBytes = usedBytes;
        }

        public long getTotalBytes() {
            return totalBytes;
        }

        public void setTotalBytes(long totalBytes) {
            this.totalBytes = totalBytes;
        }

        public DiskState getState() {
            return state;
        }

        public void setState(DiskState state) {
            this.state = state;
        }

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
    private static final class Index implements TopologyEventListener {

        final Map<UUID, Datacenter> dataCenters = new HashMap<>();
        final Map<String, Datacenter> dataCentersByName = new HashMap<>();
        final Map<UUID, Rack> racks = new HashMap<>();
        final Map<UUID, StorageNode> storageNodes = new HashMap<>();
        final Map<UUID, Disk> disks = new HashMap<>();

        // Events

        @Override
        public void onDatacenterAdded(Datacenter dc) {
            dataCenters.put(dc.id, dc);
            dataCentersByName.put(dc.name, dc);
        }

        @Override
        public void onDatacenterRemoved(Datacenter dc) {
            dataCenters.remove(dc.id);
            dataCentersByName.remove(dc.name);
        }

        @Override
        public void onRackAdded(Rack rack) {
            racks.put(rack.id, rack);
        }

        @Override
        public void onRackRemoved(Rack rack) {
            racks.remove(rack.id);
        }

        @Override
        public void onStorageNodeAdded(StorageNode node) {
            storageNodes.put(node.id, node);
        }

        @Override
        public void onStorageNodeRemoved(StorageNode node) {
            storageNodes.remove(node.id);
        }

        @Override
        public void onDiskAdded(Disk disk) {
            disks.put(disk.id, disk);
        }

        @Override
        public void onDiskRemoved(Disk disk) {
            disks.remove(disk.id);
        }

        // Find methods

        public Datacenter findDatacenter(UUID id) {
            return dataCenters.get(id);
        }

        public Datacenter findDatacenter(String name) {
            return dataCentersByName.get(name);
        }

        public Rack findRack(UUID id) {
            return racks.get(id);
        }

        public Rack findRack(final Datacenter dc, final String name) {
            Collection<Rack> result = CollectionUtils.select(findAllRacksInDatacenter(dc), new Predicate<Rack>() {
                @Override
                public boolean evaluate(Rack rack) {
                    return name == rack.name;
                }
            });

            return result.iterator().hasNext() ? result.iterator().next() : null;
        }

        public Rack findRack(final Datacenter dc, final UUID id) {
            Collection<Rack> result = CollectionUtils.select(findAllRacksInDatacenter(dc), new Predicate<Rack>() {
                @Override
                public boolean evaluate(Rack rack) {
                    return id == rack.id;
                }
            });

            return result.iterator().hasNext() ? result.iterator().next() : null;
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

        public List<Rack> findAllRacksInDatacenter(final Datacenter dc) {
            return Lists.newArrayList(CollectionUtils.select(findAllRacks(), new Predicate<Rack>() {
                @Override
                public boolean evaluate(Rack rack) {
                    return dc == rack.datacenter;
                }
            }));
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

    public Datacenter findDatacenter(UUID id) {
        return cache.findDatacenter(id);
    }

    public Datacenter findDatacenter(String name) {
        return cache.findDatacenter(name);
    }

    public Rack findRack(UUID id) {
        return cache.findRack(id);
    }

    public Rack findRack(Datacenter datacenter, UUID id) {
        return cache.findRack(datacenter, id);
    }

    public Rack findRack(Datacenter datacenter, String name) {
        return cache.findRack(datacenter, name);
    }

    public StorageNode findStorageNode(UUID id) {
        return cache.findStorageNode(id);
    }

    public Disk findDisk(UUID id) {
        return cache.findDisk(id);
    }
}
