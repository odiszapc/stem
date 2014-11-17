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

package org.stem.service;


import org.stem.api.ClusterManagerClient;
import org.stem.api.REST;
import org.stem.api.request.JoinRequest;
import org.stem.api.response.ClusterResponse;
import org.stem.coordination.ZooException;
import org.stem.coordination.ZookeeperClient;
import org.stem.coordination.ZookeeperClientFactory;
import org.stem.db.Layout;
import org.stem.db.MountPoint;
import org.stem.db.StorageNodeDescriptor;
import org.stem.utils.Utils;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class ClusterService {

    public static ClusterService instance; // TODO: make private

//    static {
//        instance = new ClusterService();
//    }

    private static ClusterManagerClient client = ClusterManagerClient.create(StorageNodeDescriptor.getClusterManagerEndpoint());
    private Executor periodicTasksExecutor = Executors.newFixedThreadPool(5);
    public final ZookeeperClient zookeeperClient;

    public ClusterService() {
        String endpoint = StorageNodeDescriptor.cluster().getZookeeperEndpoint();
        try {
            zookeeperClient = ZookeeperClientFactory.newClient(endpoint);
        } catch (ZooException e) {
            throw new RuntimeException("Fail to initialize cluster service", e);
        }
    }

    public ClusterService(ClusterResponse.Cluster cluster) {
        String endpoint = cluster.getZookeeperEndpoint();
        try {
            zookeeperClient = ZookeeperClientFactory.newClient(endpoint);
        } catch (ZooException e) {
            throw new RuntimeException("Fail to initialize cluster service", e);
        }
    }

    public void join() {
        client.join(prepareJoinRequest(), zookeeperClient);
    }

    private static JoinRequest prepareJoinRequest() {
        List<InetAddress> ipAddresses = Utils.getIpAddresses();
        Map<UUID, MountPoint> mountPoints = Layout.getInstance().getMountPoints();

        JoinRequest req = new JoinRequest();
        REST.StorageNode node = req.getNode();
        node.setId(StorageNodeDescriptor.id);
        node.setHostname(Utils.getMachineHostname());
        node.setListen(StorageNodeDescriptor.getNodeListenAddress() + ':' + StorageNodeDescriptor.getNodeListenPort());

        for (InetAddress ipAddress : ipAddresses) {
            node.getIpAddresses().add(ipAddress.toString());
        }

        long capacity = 0;
        for (MountPoint mp : mountPoints.values()) {
            REST.Disk disk = new REST.Disk(
                    mp.uuid.toString(),
                    mp.getPath(),
                    mp.getTotalSizeInBytes(),
                    mp.getAllocatedSizeInBytes());
            node.getDisks().add(disk);
            capacity += disk.getTotal();
        }
        node.setCapacity(capacity);
        return req;
    }

    public static ClusterResponse.Cluster describeAndInit() {
        ClusterResponse resp = client.describeCluster();
        instance = new ClusterService(resp.getCluster());
        return resp.getCluster();
    }

//    public ClusterResponse.Cluster describeCluster() {
//        ClusterResponse resp = client.describeCluster();
//        return resp.getCluster();
//    }


    public void startDataNotificator() throws Exception {
        periodicTasksExecutor.execute(new DataClusterNotificator());
    }
}
