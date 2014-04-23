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
import org.stem.api.request.JoinRequest;
import org.stem.api.response.ClusterResponse;
import org.stem.db.Layout;
import org.stem.db.MountPoint;
import org.stem.db.StorageNodeDescriptor;
import org.stem.util.Utils;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class ClusterService
{
    public static final ClusterService instance = new ClusterService();
    private ClusterManagerClient client = ClusterManagerClient.create(StorageNodeDescriptor.getBlobManagerEndpoint());
    private Executor periodicTasksExecutor = Executors.newFixedThreadPool(5);


    public void join()
    {
        List<InetAddress> ipAddresses = Utils.getIpAddresses();
        Map<UUID, MountPoint> mountPoints = Layout.getInstance().getMountPoints();

        JoinRequest req = new JoinRequest();
        req.setHost(StorageNodeDescriptor.getNodeListen());
        req.setPort(StorageNodeDescriptor.getNodePort());
        for (InetAddress ipAddress : ipAddresses)
        {
            req.getIpAddresses().add(ipAddress.toString());
        }

        for (MountPoint mp : mountPoints.values())
        {
            JoinRequest.Disk disk = new JoinRequest.Disk(
                    mp.uuid.toString(),
                    mp.getPath(),
                    mp.getTotalSizeInBytes(),
                    mp.getAllocatedSizeInBytes());
            req.getDisks().add(disk);
        }

        client.join(req);
    }

    public ClusterResponse.Cluster describeCluster()
    {
        ClusterResponse resp = client.describeCluster();
        return resp.getCluster();
    }


    public void startDataNotificator()
    {
        periodicTasksExecutor.execute(new DataClusterNotificator());
    }
}
