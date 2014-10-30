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

package org.stem.domain;

import org.stem.coordination.*;
import org.stem.exceptions.StemException;
import org.stem.streaming.StreamSession;
import org.stem.utils.TopologyUtils;

import java.util.Collection;
import java.util.List;

public class Cluster {

    protected static Cluster instance = null;

    final Descriptor descriptor;
    Topology topology;
    private ZookeeperClient client;

    public static Cluster getInstance() {
        if (!initialized())
            throw new StemException("Cluster has not been initialized yet.");
        return instance;
    }

    public static Cluster load(String zookeeperEndpoint) {
        if (initialized())
            throw new StemException("Cluster is already initialized");

        try {
            instance = new Cluster(zookeeperEndpoint);
        } catch (Exception e) {
            throw new StemException("Error while loading Cluster configuration from Zookeeper", e);
        }
        return instance; // TODO: load cluster topology from zookeeper
    }

    public static Cluster init(String name, int vBuckets, int rf, String zookeeperEndpoint) {
        if (initialized())
            throw new StemException("Cluster is already initialized");

        try {
            instance = new Cluster(name, vBuckets, rf, zookeeperEndpoint);
        } catch (Exception e) {
            throw new StemException("Error while initializing cluster", e);
        }

        return instance;
    }


    protected Cluster(String zookeeperEndpoint) throws Exception {
        tryStartZookeeperClient(zookeeperEndpoint);
        Descriptor descriptor = client.readZNodeData(ZooConstants.CLUSTER_DESCRIPTOR_PATH, Descriptor.class);
        assert null != descriptor : "Cluster descriptor is null";

        validate(descriptor.name, descriptor.vBuckets, descriptor.rf, zookeeperEndpoint);
        this.descriptor = descriptor;
        this.topology = new Topology(descriptor.name, descriptor.rf);
        initZookeeperPaths();
    }

    protected Cluster(String name, int vBuckets, int rf, String zookeeperEndpoint) throws Exception {
        validate(name, vBuckets, rf, zookeeperEndpoint);
        this.descriptor = new Descriptor(name, vBuckets, rf, zookeeperEndpoint);
        this.topology = new Topology(name, rf);
        initZookeeperPaths();
    }

    private void tryStartZookeeperClient(String endpoint) throws ZooException {
        if (null == client) {
            client = ZookeeperClientFactory.newClient(endpoint);
        }

        if (!client.isUninitialized()) {
            client.close();
            client = ZookeeperClientFactory.newClient(endpoint);
        }
    }

    private void validate(String name, int vBuckets, int rf, String zookeeperEndpoint) {
        if (null == name)
            throw new StemException("Cluster name is null");

        if (name.length() > 100)
            throw new StemException("Cluster name must be less than 100 symbols");

        if (vBuckets <= 0)
            throw new StemException("Number of virtual buckets must be greater than zero");

        if (rf <= 0)
            throw new StemException("Replication factor must be greater than zero");

        if (null == zookeeperEndpoint || zookeeperEndpoint.isEmpty())
            throw new StemException("Replication factor must be greater than zero");
    }

    public String getName() {
        return descriptor.name();
    }

    public int getvBuckets() {
        return descriptor.vBuckets;
    }

    public int getRf() {
        return descriptor.rf;
    }

    public void save()  // TODO: make static
    {
        try {
            tryStartZookeeperClient(descriptor.zookeeperEndpoint);
            client.createNode(ZooConstants.CLUSTER, descriptor);
        } catch (Exception e) {
            throw new StemException("Can't save cluster configuration", e);
        }
    }

    public void destroy() {
        if (null != instance) {
            client.close();
            instance = null;
        }
    }

    private void initZookeeperPaths() throws Exception {
        //client.createIfNotExists(ZooConstants.TOPOLOGY + "/" + ZooConstants.TOPO_MAP);
        client.createNodeIfNotExists(ZooConstants.TOPOLOGY, new TopoMapping());
        client.createIfNotExists(ZooConstants.OUT_SESSIONS);
    }

    public static boolean initialized() {
        return null != instance;
    }

    public synchronized void addStorageIfNotExist(StorageNode storage)  // replace synchronized with Lock
    {
        if (!topology.storageExists(storage)) {
            topology.addStorage(storage);

            // TODO: StorageNode vs. StorageStat vs. JoinRequest = combine ?

            StorageStat nodeStat = new StorageStat(storage.getIpAddress(), storage.getPort());
            for (Disk disk : storage.getDisks()) {
                DiskStat diskStat = new DiskStat(disk.getId());
                diskStat.setPath(disk.getPath());
                diskStat.setTotalBytes(disk.getTotalBytes());
                diskStat.setUsedBytes(disk.getUsedBytes());
                nodeStat.getDisks().add(diskStat);
            }

            try {
                client.createNodeIfNotExists(ZooConstants.CLUSTER, nodeStat);
            } catch (Exception e) {
                throw new StemException(e);
            }
        }
        // TODO: 1. Handle the situation when storage already exists but new disk were added
        // TODO: 2. Handle the situation when storage is new but its disks are already attached to another storage
        // TODO:    (maybe disk was moved)
    }

    public Collection<StorageNode> getStorageNodes() {
        return topology.getStorages();
    }

    public long getUsedBytes() {
        long sum = 0;
        for (StorageNode node : getStorageNodes()) {
            sum += node.getUsedBytes();
        }
        return sum;
    }

    public long getTotalBytes() {
        long sum = 0;
        for (StorageNode node : getStorageNodes()) {
            sum += node.getTotalBytes();
        }
        return sum;
    }

    public synchronized void computeMapping() // TODO: synchronized is BAD
    {
        try {
            topology.computeMappings(descriptor.vBuckets);
            TopoMapping topoMap = TopologyUtils.buildTopoMap(topology);
            client.updateNode(ZooConstants.TOPOLOGY, topoMap);

            List<StreamSession> sessions = topology.computeStreamingSessions();

            // TODO: Anything below is not a part og this method, it should be passed to somewhere like SessionManager
            for (StreamSession s : sessions) {
                client.createNodeIfNotExists(ZooConstants.OUT_SESSIONS, s);
            }

        } catch (Exception e) {
            throw new StemException("Can't compute mapping. Reason: " + e.getMessage());
        }
    }

    public void updateStat(StorageStat stat) {
        if (topology.storageExists(stat.getEndpoint())) {
            StorageNode node = topology.getStorage(stat.getEndpoint());
            node.setDisks(stat.getDisks()); // TODO: Check disks existence
        }
    }

    /**
     *
     */
    public static class Descriptor extends ZNodeAbstract {

        final String name;
        final int vBuckets;
        final int rf;
        final String zookeeperEndpoint;

        public String getName() {
            return name;
        }

        public int getvBuckets() {
            return vBuckets;
        }

        public int getRf() {
            return rf;
        }

        public Descriptor(String name, int vBuckets, int rf, String zookeeperEndpoint) {
            this.name = name;
            this.vBuckets = vBuckets;
            this.rf = rf;
            this.zookeeperEndpoint = zookeeperEndpoint;
        }

        @Override
        public String name() {
            return ZooConstants.CLUSTER_DESCRIPTOR_NAME;
        }
    }
}
