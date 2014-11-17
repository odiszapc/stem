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

import org.stem.api.REST;
import org.stem.api.response.ClusterResponse;
import org.stem.api.response.ListNodesResponse;
import org.stem.api.response.StemResponse;
import org.stem.api.response.TopologyResponse;
import org.stem.domain.Cluster;
import org.stem.domain.topology.Topology;
import org.stem.utils.Utils;

import javax.ws.rs.core.Response;
import java.net.InetSocketAddress;
import java.util.ArrayList;
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


        for (org.stem.domain.topology.Topology.StorageNode node : cluster.getStorageNodes()) {
            REST.StorageNode storageREST = packNode(node);

            if (!attachDiskStat)
                storageREST.getDisks().clear();

            response.getCluster().getNodes().add(storageREST);
        }
        return response;
    }

    public static ListNodesResponse buildUnauthorizedListResponse(List<Topology.StorageNode> nodes) {
        ListNodesResponse result = new ListNodesResponse();
        for (Topology.StorageNode node : nodes) {
            result.getNodes().add(packNode(node));
        }

        return result;
    }

    public static REST.Topology packTopology(Topology topology) {
        REST.Topology rest = new REST.Topology();
        for (Topology.Datacenter datacenter : topology.dataCenters()) {
            rest.getDataCenters().add(packDatacenter(datacenter));
        }
        return rest;
    }

    public static REST.Datacenter packDatacenter(Topology.Datacenter datacenter) {
        REST.Datacenter dcRest = new REST.Datacenter(datacenter.getId(), datacenter.getName());
        for (Topology.Rack rack : datacenter.racks()) {
            dcRest.getRacks().add(packRack(rack));
        }
        return dcRest;
    }

    public static REST.Rack packRack(Topology.Rack rack) {
        REST.Rack rackRest = new REST.Rack(rack.getId(), rack.getName());
        for (Topology.StorageNode node : rack.storageNodes()) {
            rackRest.getNodes().add(packNode(node));
        }
        return rackRest;
    }

    public static REST.StorageNode packNode(Topology.StorageNode node) {
        REST.StorageNode result = new REST.StorageNode(node.getId(), node.getHostname(), node.getAddress().toString(), 0l);

        long total = 0;
        for (Topology.Disk disk : node.disks()) {
            total += disk.getTotalBytes();
            result.getDisks().add(packDisk(disk));
        }
        result.setCapacity(total);
        return result;
    }

    public static REST.Disk packDisk(Topology.Disk disk) {
        return new REST.Disk(disk.getId().toString(), disk.getPath(), disk.getUsedBytes(), disk.getTotalBytes());
    }

    public static List<Topology.Datacenter> extractDataCenters(REST.Topology topologyTransient) {
        List<Topology.Datacenter> result = new ArrayList<>();
        for (REST.Datacenter dcTransient : topologyTransient.getDataCenters()) {
            result.add(extractDatacenter(dcTransient));
        }
        return result;
    }

    public static Topology.Datacenter extractDatacenter(REST.Datacenter dcTransient) {
        Topology.Datacenter datacenter = new Topology.Datacenter(dcTransient.getName());

        for (REST.Rack rackTransient : dcTransient.getRacks()) {
            datacenter.addRack(extractRack(rackTransient));
        }
        return datacenter;
    }

    public static Topology.Rack extractRack(REST.Rack rackTransient) {
        Topology.Rack rack = new Topology.Rack(rackTransient.getName());
        rack.setId(rackTransient.getId());

        for (REST.StorageNode nodeTransient : rackTransient.getNodes()) {
            rack.addStorageNode(extractNode(nodeTransient));
        }
        return rack;
    }

    public static Topology.StorageNode extractNode(REST.StorageNode nodeTransient) {
        InetSocketAddress address = new InetSocketAddress(
                Utils.getHost(nodeTransient.getListen()),
                Utils.getPort(nodeTransient.getListen()));
        Topology.StorageNode node = new Topology.StorageNode(address);
        node.setId(nodeTransient.getId());
        node.setHostname(nodeTransient.getHostname());
        for (REST.Disk diskTransient : nodeTransient.getDisks()) {
            Topology.Disk disk = extractDisk(diskTransient);
            node.addDisk(disk);
        }

        return node;
    }

    public static Topology.Disk extractDisk(REST.Disk diskTransient) {
        Topology.Disk disk = new Topology.Disk();
        disk.setId(UUID.fromString(diskTransient.getId()));
        disk.setPath(diskTransient.getPath());
        disk.setUsedBytes(diskTransient.getUsed());
        disk.setTotalBytes(diskTransient.getTotal());
        disk.setState(Topology.DiskState.SUSPEND);
        return disk;
    }

    public static TopologyResponse buildTopologyResponse(org.stem.domain.topology.Topology topology) {
        return new TopologyResponse(packTopology(topology));
    }
}
