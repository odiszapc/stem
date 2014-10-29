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
import org.stem.util.TopologyUtils;

import java.util.Collection;
import java.util.List;

public class Cluster {

    protected static Cluster instance = null;
    private final ClusterDescriptor descriptor;
    private static ZookeeperClient client;

    public static Cluster getInstance() {
        if (!initialized())
            throw new StemException("Cluster has not been initialized yet.");
        return instance;
    }

    Topology topology;

    protected Cluster(String name, int vBuckets, int rf) {
        this.descriptor = new ClusterDescriptor(name, vBuckets, rf);
        topology = new Topology(name, rf);
    }


    public String getName() {
        return descriptor.getName();
    }

    public int getvBuckets() {
        return descriptor.getvBuckets();
    }

    public int getRf() {
        return descriptor.getRf();
    }

    public static Cluster init() {
        try {
            zookeeperClientSafeStart();
            ClusterDescriptor descriptor = client.readZNodeData(ZooConstants.CLUSTER_DESCRIPTOR_PATH, ClusterDescriptor.class);
            if (null != descriptor) {
                init(descriptor.getName(), descriptor.getvBuckets(), descriptor.getRf());
            }
            int a = 1;
        } catch (Exception e) {
            throw new StemException("Can't load cluster configuration", e);
        }
        return null; // TODO: load cluster topology from zookeeper
    }

    public void save()  // TODO: make static
    {
        try {
            zookeeperClientSafeStart();
            client.createNode(ZooConstants.CLUSTER, descriptor);
        } catch (Exception e) {
            throw new StemException("Can't save cluster configuration", e);
        }
    }

    private static void zookeeperClientSafeStart() throws ZooException {
        if (null == client) {
            client = ZookeeperClientFactory.newClient();
        }

        if (client.isUninitialized())
            client.start();
        else {
            client.close();
            client = ZookeeperClientFactory.newClient();
            client.start();
        }
    }

    public static Cluster init(String name, int vBuckets, int rf) {
        if (null == name) {
            throw new StemException("Cluster name can not be null");
        }

        if (name.length() > 50) {
            throw new StemException("Cluster name must be less than 50 symbols");
        }

        if (vBuckets <= 0) {
            throw new StemException("Number of virtual buckets must be greater than zero");
        }

        if (rf <= 0) {
            throw new StemException("Replication factor must be greater than zero");
        }

        if (initialized()) {
            throw new StemException("Cluster is already initialized");
        }

        instance = new Cluster(name, vBuckets, rf);

        try {
            instance.initZookeeper();
        } catch (Exception e) {
            throw new StemException("Error while initializing cluster", e);
        }

        return instance;
    }

    public static void destroy() {
        if (null != instance) {
            client.close();
            instance = null;
        }
    }

    private void initZookeeper() throws Exception {
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
            topology.computeMappings(descriptor.getvBuckets());
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
}
