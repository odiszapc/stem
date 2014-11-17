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
import org.stem.api.request.AuthorizeNodeRequest;
import org.stem.api.request.InitClusterRequest;
import org.stem.api.request.JoinRequest;
import org.stem.api.request.ListUnauthorizedNodesRequest;
import org.stem.api.response.ClusterResponse;
import org.stem.api.response.JoinResponse;
import org.stem.api.response.ListNodesResponse;
import org.stem.coordination.Event;
import org.stem.coordination.EventFuture;
import org.stem.coordination.EventManager;
import org.stem.domain.Cluster;
import org.stem.domain.topology.Topology;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.util.List;

@Path(RESTConstants.Api.Cluster.URI)
public class ClusterResource {

    /**
     * Initialize cluster
     *
     * @param req
     * @return
     */
    @POST
    @Path(RESTConstants.Api.Cluster.Init.BASE)
    public Response create(InitClusterRequest req) {
        Cluster.instance.initialize(req.getName(), req.getvBuckets(), req.getRf(), req.getPartitioner());
        Cluster.instance().save();

        return RestUtils.ok();
    }

    /**
     * Show common information about cluster
     *
     * @return
     */
    @GET
    @Path(RESTConstants.Api.Cluster.Get.BASE)
    public Response get() {
        Cluster cluster = Cluster.instance().ensureInitialized();

        ClusterResponse response = RestUtils.buildClusterResponse(cluster, true);
        return RestUtils.ok(response);
    }

    /**
     * Node wants join the cluster. STEP 1
     *
     * @param request
     * @return
     * @throws Exception
     */
    @POST
    @Path(RESTConstants.Api.Cluster.Join.BASE) // TODO: when node restarts between join and accept events
    public synchronized Response join(JoinRequest request) throws Exception {
        Topology.StorageNode node = RestUtils.extractNode(request.getNode());
        // TODO: delete unused async requests
        Cluster cluster = Cluster.instance().ensureInitialized();
        EventFuture future = EventManager.instance.createSubscription(Event.Type.JOIN);

        Topology.StorageNode existing = cluster.topology().findStorageNode(node.getId());
        if (null == existing) {
            cluster.unauthorized().add(node, future);
        } else {
            // TODO: check node status
            cluster.approve(future.eventId(), node.getId());
        }

        return RestUtils.ok(new JoinResponse(future.eventId()));
    }

    // TODO: save/load unauthorized nodes
    // TODO: save/load subscriptions
    @GET
    @Path(RESTConstants.Api.Cluster.Unauthorized.BASE)
    public Response unauthorized(ListUnauthorizedNodesRequest request) throws Exception {
        Cluster cluster = Cluster.instance().ensureInitialized();
        List<Topology.StorageNode> list = cluster.unauthorized().list();
        ListNodesResponse resp = RestUtils.buildUnauthorizedListResponse(list);

        return RestUtils.ok(resp);
    }

    @POST
    @Path(RESTConstants.Api.Cluster.Approve.BASE)
    public Response approveUnauthorized(AuthorizeNodeRequest req) throws Exception {
        Cluster cluster = Cluster.instance().ensureInitialized();

        String datacenter = req.getDatacenter();
        String rack = req.getRack();

        Event.Join response = cluster.approve(req.getNodeId(), datacenter, rack);
        return RestUtils.ok(response); // TODO: return an empty result on success as we usual do?
    }

    @POST
    @Path(RESTConstants.Api.Cluster.Refuse.BASE)
    public Response deny(AuthorizeNodeRequest req) throws Exception {
        Cluster cluster = Cluster.instance().ensureInitialized();
        Event.Join response = cluster.unauthorized().deny(req.getNodeId());
        return RestUtils.ok(response); // TODO: return an empty result on success as we usual do?
    }
}
