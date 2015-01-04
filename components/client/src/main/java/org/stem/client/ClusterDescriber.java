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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.api.REST;
import org.stem.coordination.*;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ClusterDescriber {

    private static final Logger logger = LoggerFactory.getLogger(ClusterDescriber.class);

    final StemCluster.Manager cluster;
    final StateNotifier notifier;


    private volatile boolean isShutdown;

    private class StateNotifier {

        final ZookeeperEventListener<REST.TopologySnapshot> stateWatcher;

        public StateNotifier() {

            stateWatcher = new ZookeeperEventListener<REST.TopologySnapshot>() {
                @Override
                public Class<? extends REST.TopologySnapshot> getBaseClass() {
                    return REST.TopologySnapshot.class;
                }

                @Override
                protected ZNode.Codec codec() {
                    return Codecs.TOPOLOGY_SNAPSHOT;
                }

                @Override
                protected synchronized void onNodeUpdated(REST.TopologySnapshot object) {
                    logger.info("Updated topology response received");
                    onTopologyChanged(object);
                }

                @Override
                protected void onError(Throwable t) {
                    super.onError(t);
                }
            };
        }

        void start() throws Exception {
            cluster.coordinationClient.listenForZNode(ZookeeperPaths.topologySnapshotPath(), stateWatcher);
        }

        // TODO: stop() {}
    }

    public ClusterDescriber(StemCluster.Manager cluster) {
        this.cluster = cluster;
        this.notifier = new StateNotifier();
    }

    private void onTopologyChanged(REST.TopologySnapshot state) {
        refreshNodeList(cluster, state);
        //refreshNodeList(cluster, state.getTopology(), false);
        //updateMapping(state.getMapping());
        // TODO: !!!!!!!!!!!
    }

    void start() {
        if (isShutdown)
            return;

        try {
            REST.TopologySnapshot state = tryReadState();
            refreshNodeList(cluster, state);
            //refreshNodeList(cluster, topology, true);
            // TODO: refreshBucketMap(cluster, true);

            notifier.start();
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    private void refreshNodeList(StemCluster.Manager cluster, REST.TopologySnapshot state) {
        List<InetSocketAddress> foundHosts = new ArrayList<>();
        REST.Topology topology = state.getTopology();
        for (REST.StorageNode nodeInfo : topology.nodes()) {
            InetSocketAddress addr = nodeInfo.getSocketAddress();
            foundHosts.add(addr);
        }

        for (InetSocketAddress addr : foundHosts) {
            Host host = cluster.metadata.getHost(addr);
            boolean isNew = false;
            if (null == host) {
                host = cluster.metadata.add(addr);
                isNew = true;
            }

            if (isNew)
                cluster.triggerOnAdd(host);
        }

        cluster.metadata.updateRouting(state.getTopology(), state.getMapping()); // TODO: merge both parameters into single instance (REST.TopologySnapshot)
    }

    @Deprecated
    private void refreshNodeList(StemCluster.Manager cluster, REST.Topology topology, boolean isInitialAttempt) {
        List<InetSocketAddress> foundHosts = new ArrayList<InetSocketAddress>();
        List<String> dcs = new ArrayList<String>(); // TODO: implement extraction of dc name
        List<String> racks = new ArrayList<String>(); // TODO: implement extraction of rack name

        for (REST.StorageNode nodeInfo : topology.nodes()) {
            InetSocketAddress addr = nodeInfo.getSocketAddress();
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
                cluster.triggerOnAdd(host); // Create connection pools in non-blocking mode

            // TODO: remove hosts
        }

        // TODO: wait until all pools will be ready
        cluster.metadata.setTopology(topology);
    }

    private void refreshNodeList() {
        refreshNodeList(cluster, cluster.metadata.getTopology(), false);
    }

    private REST.TopologySnapshot tryReadState() {
        return readState();
    }

    private REST.TopologySnapshot readState() {
        try {
            return zookeeperClient().readZNodeData(ZookeeperPaths.topologySnapshotPath(), REST.TopologySnapshot.class, Codecs.TOPOLOGY_SNAPSHOT);

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
