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
import org.stem.domain.Cluster;
import org.stem.domain.StorageNode;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Path(RESTConstants.Api.Cluster.URI)
public class ClusterResource {
    /**
     * Init cluster
     *
     * @param request
     * @return
     */
    @PUT
    @Path(RESTConstants.Api.Cluster.Init.BASE)
    public Response init(InitClusterRequest request) {
        Cluster.init(request.getName(), request.getvBuckets(), request.getRf());
        Cluster.getInstance().save();

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
        Cluster cluster = Cluster.getInstance();
        ClusterResponse response = RestUtils.buildClusterResponse(cluster, true);
        return RestUtils.ok(response);
    }

    /**
     * Join cluster
     *
     * @param request
     * @return
     */
    @PUT
    @Path(RESTConstants.Api.Cluster.Join.BASE)
    public Response join(JoinRequest request) {
        StorageNode storage = new StorageNode(request);
        Cluster.getInstance().addStorageIfNotExist(storage);
        return RestUtils.ok();
    }
}
