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

public class Cluster
{
    protected static Cluster instance = new Cluster();
    private ZookeeperClient client = new ZookeeperClient();

    public static Cluster getInstance()
    {
        if (!initialized())
            throw new StemException("Cluster has not been initialized yet.");
        return instance;
    }

    String name;
    int vBuckets;
    int rf;

    Topology topology;

    protected Cluster(String name, int vBuckets, int rf)
    {
        this();
        this.name = name;
        this.vBuckets = vBuckets;
        this.rf = rf;
        topology = new Topology(name, rf);
    }

    public Cluster()
    {
        client.start();
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public int getvBuckets()
    {
        return vBuckets;
    }

    public void setvBuckets(int vBuckets)
    {
        this.vBuckets = vBuckets;
    }

    public int getRf()
    {
        return rf;
    }

    public void setRf(int rf)
    {
        this.rf = rf;
    }

    public static Cluster load()
    {
        return null; // TODO: load cluster topology from zookeeper
    }

    public static Cluster save()
    {
        return null; // TODO: save cluster topology to zookeeper
    }

    public static Cluster init(String name, int vBuckets, int rf)
    {
        if (null == name)
        {
            throw new StemException("Cluster name can not be null");
        }

        if (name.length() > 50)
        {
            throw new StemException("Cluster name must be less than 50 symbols");
        }

        if (vBuckets <= 0)
        {
            throw new StemException("Number of virtual buckets must be greater than zero");
        }

        if (rf <= 0)
        {
            throw new StemException("Replication factor must be greater than zero");
        }

        if (initialized())
        {
            throw new StemException("Cluster is already initialized");
        }

        instance = new Cluster(name, vBuckets, rf);

        try
        {
            instance.initZookeeper();
        }
        catch (Exception e)
        {
            throw new StemException("Error while initializing cluster", e);
        }

        return instance;
    }

    public static void destroy()
    {
        instance.name = null;
        instance.vBuckets = 0;
        instance.rf = 0;
        instance.client.close();
    }

    private void initZookeeper() throws Exception
    {
        //client.createIfNotExists(StemZooConstants.TOPOLOGY + "/" + StemZooConstants.TOPO_MAP);
        client.createNodeIfNotExists(StemZooConstants.TOPOLOGY, new TopoMapping());
        client.createIfNotExists(StemZooConstants.OUT_SESSIONS);
    }

    public static boolean initialized()
    {
        return null != instance.name && 0 != instance.vBuckets;
    }

    public synchronized void addStorageIfNotExist(StorageNode storage)  // replace synchronized with Lock
    {
        if (!topology.storageExists(storage))
        {
            topology.addStorage(storage);

            // TODO: StorageNode vs. StorageStat vs. JoinRequest = combine ?

            StorageStat nodeStat = new StorageStat(storage.getIpAddress(), storage.getPort());
            for (Disk disk : storage.getDisks())
            {
                DiskStat diskStat = new DiskStat(disk.getId());
                diskStat.setPath(disk.getPath());
                diskStat.setTotalBytes(disk.getTotalBytes());
                diskStat.setUsedBytes(disk.getUsedBytes());
                nodeStat.getDisks().add(diskStat);
            }

            try
            {
                client.createNodeIfNotExists(StemZooConstants.CLUSTER, nodeStat);
            }
            catch (Exception e)
            {
                throw new StemException(e);
            }
        }
        // TODO: 1. Handle the situation when storage already exists but new disk were added
        // TODO: 2. Handle the situation when storage is new but its disks are already attached to another storage
        // TODO:    (maybe disk was moved)
    }

    public Collection<StorageNode> getStorageNodes()
    {
        return topology.getStorages();
    }

    public long getUsedBytes()
    {
        long sum = 0;
        for (StorageNode node : getStorageNodes())
        {
            sum += node.getUsedBytes();
        }
        return sum;
    }

    public long getTotalBytes()
    {
        long sum = 0;
        for (StorageNode node : getStorageNodes())
        {
            sum += node.getTotalBytes();
        }
        return sum;
    }

    public synchronized void computeMapping() // TODO: synchronized is BAD
    {
        try
        {
            topology.computeMappings(vBuckets);
            TopoMapping topoMap = TopologyUtils.buildTopoMap(topology);
            client.updateNode(StemZooConstants.TOPOLOGY, topoMap);

            List<StreamSession> sessions = topology.computeStreamingSessions();

            // TODO: Anything below is not a part og this method, it should be passed to somewhere like SessionManager
            for (StreamSession s : sessions)
            {
                client.createNodeIfNotExists(StemZooConstants.OUT_SESSIONS, s);
            }

        }
        catch (Exception e)
        {
            throw new StemException("Can't compute mapping. Reason: " + e.getMessage());
        }
    }

    public void updateStat(StorageStat stat)
    {
        if (topology.storageExists(stat.getEndpoint()))
        {
            StorageNode node = topology.getStorage(stat.getEndpoint());
            node.setDisks(stat.getDisks()); // TODO: Check disks existence
        }
    }
}
