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

package org.stem.client;

import com.google.common.base.Throwables;
import org.stem.api.REST;
import org.stem.coordination.ZookeeperClient;
import org.stem.coordination.ZookeeperEventListener;
import org.stem.coordination.ZookeeperPaths;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class ClusterDescriber {

    final StemCluster.Manager cluster;
    final ZookeeperEventListener<REST.Topology> topologyListener; // TODO: topology and mapping are changes synchronously
    final ZookeeperEventListener<REST.Mapping> mappingListener;

    private volatile boolean isShutdown;

    public ClusterDescriber(StemCluster.Manager cluster) {
        this.cluster = cluster;
        topologyListener = new ZookeeperEventListener<REST.Topology>() {
            @Override
            public Class<REST.Topology> getBaseClass() {
                return REST.Topology.class;
            }

            @Override
            protected void onNodeUpdated(REST.Topology topology) {
                onTopologyChanged(topology);
            }
        };

        mappingListener = new ZookeeperEventListener<REST.Mapping>() {
            @Override
            public Class<? extends REST.Mapping> getBaseClass() {
                return REST.Mapping.class;
            }

            @Override
            protected void onNodeUpdated(REST.Mapping mapping) {
                onMappingChanged(mapping);
            }
        };
    }

    private void onTopologyChanged(REST.Topology topology) {
        refreshNodeList(cluster, topology, false);
    }

    private void onMappingChanged(REST.Mapping mapping) {
        cluster.metadata.setMapping(mapping);
    }

    void start() {
        if (isShutdown)
            return;

        try {
            REST.Topology topology = tryReadTopology();
            refreshNodeList(cluster, topology, true);
            // TODO: refreshBucketMap(cluster, true);

            cluster.coordinationClient.listenForZNode(ZookeeperPaths.topologyPath(), topologyListener);
        } catch (Exception e) {
            Throwables.propagate(e);

        }
    }

    private void refreshNodeList(StemCluster.Manager cluster, REST.Topology topology, boolean isInitialAttempt) {
        List<InetSocketAddress> foundHosts = new ArrayList<InetSocketAddress>();
        List<String> dcs = new ArrayList<String>(); // TODO: implement extraction of dc name
        List<String> racks = new ArrayList<String>(); // TODO: implement extraction of rack name

        for (REST.StorageNode nodeInfo : topology.nodes()) {
            InetSocketAddress addr = nodeInfo.socketAddress();
            foundHosts.add(addr);
        }


        for (InetSocketAddress addr : foundHosts) {
            Host host = cluster.metadata.getHost(addr);
            boolean isNew = false;
            if (null == host) {
                host = cluster.metadata.add(addr);
                isNew = true;
            }

            if (isNew && !isInitialAttempt)
                cluster.triggerOnAdd(host);
        }

        cluster.metadata.setTopology(topology);
    }

    private void refreshNodeList() {
        refreshNodeList(cluster, cluster.metadata.getTopology(), false);
    }

    private REST.Topology tryReadTopology() {
        return readTopology();
    }

    private REST.Topology readTopology() {
        try {
            return zookeeperClient().readZNodeData(ZookeeperPaths.topologyPath(), REST.Topology.class);

        } catch (Exception e) {
            Throwables.propagate(e);
        }
        return null;
    }

    private ZookeeperClient zookeeperClient() {
        return cluster.coordinationClient;
    }

    public CloseFuture closeAsync() {
        isShutdown = true;

        return CloseFuture.immediateFuture();
    }

    public void onUp(Host host) {
    }

    public void onDown(Host host) {

    }

    public void onRemove(Host host) {
        refreshNodeList();
    }

    public void onSuspected(Host host) {

    }

    public void refreshNodeInfo(Host host) {
        // TODO: should or not we to implement it?
    }
}
