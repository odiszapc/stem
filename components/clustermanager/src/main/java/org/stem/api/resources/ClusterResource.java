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
import org.stem.api.UserMessages;
import org.stem.api.request.ApproveNodeRequest;
import org.stem.api.request.CreateClusterRequest;
import org.stem.api.request.JoinRequest;
import org.stem.api.response.ClusterResponse;
import org.stem.api.response.JoinResponse;
import org.stem.api.response.ListNodesResponse;
import org.stem.coordination.Event;
import org.stem.coordination.EventFuture;
import org.stem.domain.Cluster;
import org.stem.domain.topology.Topology;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path(RESTConstants.Api.Cluster.URI)
@Produces(MediaType.APPLICATION_JSON)
public class ClusterResource {

    /**
     * Initialize cluster
     *
     * @param req
     * @return
     */
    @POST
    @Path(RESTConstants.Api.Cluster.Init.BASE)
    public Response create(CreateClusterRequest req) {

        Cluster.instance.initialize(req.getName(), req.getvBuckets(), req.getRf(), req.getPartitioner(),
                req.getMetaStoreConfiguration(), req.getConfiguration());

        return RestUtils.ok(UserMessages.CLUSTER_CREATED);
    }

    /**
     * Show common information about cluster
     *
     * @return
     */
    @GET
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

        EventFuture future = cluster.tryJoinAsync(node);
        return RestUtils.ok(new JoinResponse(future.eventId()), UserMessages.NODE_WAITING);
    }

    // TODO: save/load unauthorized nodes
    // TODO: save/load subscriptions
    @GET
    @Path(RESTConstants.Api.Cluster.Unauthorized.BASE)
    public Response unauthorized() throws Exception {
        Cluster cluster = Cluster.instance().ensureInitialized();
        List<Topology.StorageNode> list = cluster.unauthorizedPool().list();
        ListNodesResponse resp = RestUtils.buildUnauthorizedListResponse(list);

        return RestUtils.ok(resp);
    }

    /**
     * Admin approve pending node that wants join cluster, STEP 2
     *
     * @param request
     * @return
     * @throws Exception
     */
    @POST
    @Path(RESTConstants.Api.Cluster.Approve.BASE)
    public Response approveUnauthorized(ApproveNodeRequest request) throws Exception {
        Cluster cluster = Cluster.instance().ensureInitialized();

        Event.Join response = cluster.approve(request.getNodeId(), request.getDatacenter(), request.getRack());
        return RestUtils.ok(response, UserMessages.NODE_JOINED); // TODO: return an empty result on success as we usual do?
    }

    @POST
    @Path(RESTConstants.Api.Cluster.Refuse.BASE)
    public Response deny(ApproveNodeRequest req) throws Exception {
        Cluster cluster = Cluster.instance().ensureInitialized();
        Event.Join response = cluster.unauthorizedPool().deny(req.getNodeId());
        return RestUtils.ok(response); // TODO: return an empty result on success as we usual do?
    }
}
