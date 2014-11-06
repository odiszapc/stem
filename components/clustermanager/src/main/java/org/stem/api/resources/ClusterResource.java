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

package org.stem.api.resources;

import org.stem.RestUtils;
import org.stem.api.RESTConstants;
import org.stem.api.request.InitClusterRequest;
import org.stem.api.request.JoinRequest;
import org.stem.api.response.ClusterResponse;
import org.stem.api.response.JoinResponse;
import org.stem.coordination.Event;
import org.stem.coordination.EventManager;
import org.stem.domain.Cluster;
import org.stem.domain.StorageNode;
import org.stem.domain.topology.Topology;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.net.InetSocketAddress;
import java.util.UUID;

@Path(RESTConstants.Api.Cluster.URI)
public class ClusterResource {

    /**
     * Init cluster
     *
     * @param req
     * @return
     */
    @PUT
    @Path(RESTConstants.Api.Cluster.Init.BASE)
    public Response init(InitClusterRequest req) {
        Cluster.instance.initialize(req.getName(), req.getvBuckets(), req.getRf(), req.getPartitioner());
        Cluster.instance().save();

        return RestUtils.ok();
    }

    /**
     * Describe cluster topology
     *
     * @return
     */
    @GET
    @Path(RESTConstants.Api.Cluster.Get.BASE)
    public Response get() {
        Cluster cluster = Cluster.instance;
        ClusterResponse response = RestUtils.buildClusterResponse(cluster, true);
        return RestUtils.ok(response);
    }

    /**
     * Join cluster
     *
     * @param request
     * @return
     * @deprecated use #join2(request)
     */
    @Deprecated
    @PUT
    @Path(RESTConstants.Api.Cluster.Join.BASE)
    public Response join(JoinRequest request) {
        StorageNode storage = new StorageNode(request);
        Cluster.instance.addStorageIfNotExist(storage);
        return RestUtils.ok();
    }

    @PUT
    @Path(RESTConstants.Api.Cluster.Join2.BASE)
    public Response join2(JoinRequest request) throws Exception {
        InetSocketAddress address = new InetSocketAddress(request.getHost(), request.getPort());
        Topology.StorageNode node = new Topology.StorageNode(address);

//        StorageNode storage = new StorageNode(request);
//        Cluster.instance.addStorageIfNotExist(storage);

        // Cluster.instance.addPendingNode(node, trackId);
        UUID trackId = EventManager.randomId();
        EventManager.instance.createSubscription(Event.Type.JOIN, trackId);
        //EventManager.instance.fire(trackId, StemResponse)

        return RestUtils.ok(new JoinResponse(trackId));
    }
}
