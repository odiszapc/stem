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

public class ClusterTopologyMappingDescriber {

    final StemCluster.Manager manager;
    final ZookeeperEventListener<REST.Topology> topologyListener;

    public ClusterTopologyMappingDescriber(StemCluster.Manager manager) {
        this.manager = manager;

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
    }

    private void onTopologyChanged(REST.Topology topology) {
        for (REST.Datacenter datacenter : topology.getDataCenters()) {
            for (REST.Rack rack : datacenter.getRacks()) {
                for (REST.StorageNode storageNode : rack.getNodes()) {
                    //ClusterTopologyMappingDescriber.this.manager.
                }
            }
        }
    }

    void start() {
        tryReadTopology();



        manager.executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    zookeeperClient().listenForZNode(ZookeeperPaths.CLUSTER_TOPOLOGY_PATH, );
                } catch (Exception e) {
                    Throwables.propagate(e);
                }
            }
        });
    }

    private void tryReadTopology() {
        try {
            REST.Topology topology = zookeeperClient().readZNodeData(ZookeeperPaths.topologyPath(), REST.Topology.class);

        } catch (Exception e) {
            Throwables.propagate(e);
        }

    }



    private ZookeeperClient zookeeperClient() {
        return manager.coordinationClient;
    }


}
