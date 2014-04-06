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

import org.stem.api.response.ClusterResponse;
import org.stem.api.response.StemResponse;
import org.stem.domain.Cluster;
import org.stem.domain.Disk;
import org.stem.domain.StorageNode;

import javax.ws.rs.core.Response;

public class RestUtils
{
    public static Response ok()
    {

        return ok(new StemResponse());
    }

    public static Response ok(StemResponse response)
    {
        return Response.status(Response.Status.OK).entity(response).build();
    }

    public static ClusterResponse buildClusterResponse(Cluster cluster)
    {
        return buildClusterResponse(cluster, false);
    }

    public static ClusterResponse buildClusterResponse(Cluster cluster, boolean attachDiskStat)
    {
        ClusterResponse response = new ClusterResponse();

        response.getCluster().setName(cluster.getName());
        response.getCluster().setvBucketsNum(cluster.getvBuckets());
        response.getCluster().setRf(cluster.getRf());
        response.getCluster().setUsedBytes(cluster.getUsedBytes());
        response.getCluster().setTotalBytes(cluster.getTotalBytes());

        for (StorageNode node : cluster.getStorageNodes())
        {
            ClusterResponse.Storage storageREST = new ClusterResponse.Storage(
                    node.getIpAddress(),
                    node.getPort(),
                    node.getUsedBytes(),
                    node.getTotalBytes()
            );

            if (attachDiskStat)
            {
                for (Disk disk : node.getDisks())
                {
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
}
