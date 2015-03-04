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

package org.stem.frontend.api.resources;

import org.stem.frontend.RestUtils;
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
//import org.stem.domain.Cluster;
//import org.stem.domain.topology.Topology;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

//@Path(RESTConstants.Api.Cluster.URI)
@Path("/frontend")
@Produces(MediaType.APPLICATION_JSON)
public class FrontendResource {

    /**
     * Show common information about cluster
     *
     * @return
     */
    @GET
    @Path("/get")
    @Produces(MediaType.TEXT_HTML)
    public Response get() {
//        Cluster cluster = Cluster.instance().ensureInitialized();

//        ClusterResponse response = RestUtils.buildClusterResponse(cluster, true);
//        return RestUtils.ok();
        return Response.status(200).entity("Hello world").build();
    }
}
