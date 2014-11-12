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

package org.stem;

import org.stem.api.DiskTransient;
import org.stem.api.StorageNodeTransient;
import org.stem.api.response.ClusterResponse;
import org.stem.api.response.ListNodesResponse;
import org.stem.api.response.StemResponse;
import org.stem.domain.Cluster;
import org.stem.domain.Disk;
import org.stem.domain.StorageNode;
import org.stem.domain.topology.Topology;
import org.stem.utils.Utils;

import javax.ws.rs.core.Response;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;

public class RestUtils {

    public static Response ok() {

        return ok(new StemResponse());
    }

    public static Response ok(StemResponse response) {
        return Response.status(Response.Status.OK).entity(response).build();
    }

    public static ClusterResponse buildClusterResponse(Cluster cluster) {
        return buildClusterResponse(cluster, false);
    }

    public static ClusterResponse buildClusterResponse(Cluster cluster, boolean attachDiskStat) {
        ClusterResponse response = new ClusterResponse();
        Cluster.Descriptor descriptor = cluster.descriptor();
        response.getCluster().setName(descriptor.getName());
        response.getCluster().setvBucketsNum(descriptor.getvBuckets());
        response.getCluster().setPartitioner(descriptor.getPartitioner().getName());
        response.getCluster().setRf(descriptor.getRf());
        response.getCluster().setZookeeperEndpoint(descriptor.getZookeeperEndpoint());
        response.getCluster().setUsedBytes(cluster.getUsedBytes());
        response.getCluster().setTotalBytes(cluster.getTotalBytes());

        for (StorageNode node : cluster.getStorageNodes()) {
            ClusterResponse.Storage storageREST = new ClusterResponse.Storage(
                    node.getIpAddress(),
                    node.getPort(),
                    node.getUsedBytes(),
                    node.getTotalBytes()
            );

            if (attachDiskStat) {
                for (Disk disk : node.getDisks()) {
                    ClusterResponse.Disk diskREST = new ClusterResponse.Disk();
                    diskREST.setId(disk.getId());
                    diskREST.setPath(disk.getPath());
                    diskREST.setUsedBytes(disk.getUsedBytes());
                    diskREST.setTotalBytes(disk.getTotalBytes());
                    storageREST.getDisks().add(diskREST);
                }
            }

            response.getCluster().getNodes().add(storageREST);
        }
        return response;
    }

    public static ListNodesResponse buildUnauthorizedListResponse(List<Topology.StorageNode> list) {
        return null;
    }

    public static Topology.Disk extractDisk(DiskTransient diskTransient) {
        Topology.Disk disk = new Topology.Disk();
        disk.setId(UUID.fromString(diskTransient.getId()));
        disk.setPath(diskTransient.getPath());
        disk.setUsedBytes(diskTransient.getUsed());
        disk.setTotalBytes(diskTransient.getTotal());
        disk.setState(Topology.DiskState.SUSPEND);
        return disk;
    }

    public static Topology.StorageNode extractNode(StorageNodeTransient nodeTransient) {
        InetSocketAddress address = new InetSocketAddress(
                Utils.getHost(nodeTransient.getListen()),
                Utils.getPort(nodeTransient.getListen()));
        Topology.StorageNode node = new Topology.StorageNode(address);
        node.setId(nodeTransient.getId());
        for (DiskTransient diskTransient : nodeTransient.getDisks()) {
            Topology.Disk disk = extractDisk(diskTransient);
            node.addDisk(disk);
        }

        return node;
    }
}
