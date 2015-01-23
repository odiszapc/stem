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

package org.stem.api;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.api.request.ClusterConfiguration;
import org.stem.api.request.CreateClusterRequest;
import org.stem.api.request.JoinRequest;
import org.stem.api.request.MetaStoreConfiguration;
import org.stem.api.response.ClusterResponse;
import org.stem.api.response.JoinResponse;
import org.stem.api.response.StemResponse;
import org.stem.coordination.Event;
import org.stem.coordination.ZookeeperClient;

import java.net.URI;

public class ClusterManagerClient extends BaseHttpClient {

    private static final Logger logger = LoggerFactory.getLogger(ClusterManagerClient.class);

    public static ClusterManagerClient create(String uri) {
        return new ClusterManagerClient(uri);
    }

    public ClusterManagerClient(String uri) {
        super(uri);
    }

    public void join(JoinRequest message, ZookeeperClient client) {
        try {
            URI uri = getURI(RESTConstants.Api.Cluster.Join.URI);
            JoinResponse response = send(new HttpPost(uri), message, JoinResponse.class);

            assert null != response.requestId;
            logger.info("Waiting for join approval from cluster manager...");
            Event.Join joinResult = (Event.Join) Event.Listener.waitFor(response.requestId, Event.Type.JOIN, client);

            // TODO: What if node has to go down until user approve it?
            logger.info("Joining response received");
            if (!joinResult.isSuccess())
                throw new Exception("Cluster manager refused the request: " + joinResult.getMessage());

            // TODO: what should we do immediately after joining cluster?
        } catch (Exception e) {
            logger.error("Can not join cluster", e);
            throw new RuntimeException("Can't join cluster: " + e.getMessage());
        }
    }

    public ClusterResponse describeCluster() {
        URI uri = getURI(RESTConstants.Api.Cluster.Get.URI);
        HttpGet request = new HttpGet(uri);

        return send(request, ClusterResponse.class);
    }

    public StemResponse info() {
        URI uri = getURI(RESTConstants.Api.BASE);
        HttpGet request = new HttpGet(uri);

        return send(request, StemResponse.class);
    }


    public StemResponse initCluster(String clusterName, int vBuckets, int rf, String partitioner, boolean autoApprove) {
        URI uri = getURI(RESTConstants.Api.Cluster.Init.URI);
        HttpPost request = new HttpPost(uri);

        // Initialize with local Cassandra cluster, RF = 1
        CreateClusterRequest initRequest = new CreateClusterRequest(clusterName, vBuckets, rf, partitioner,
                new MetaStoreConfiguration(), new ClusterConfiguration(autoApprove));

        return send(request, initRequest, StemResponse.class);
    }


    public StemResponse computeMapping() {
        URI uri = getURI(RESTConstants.Api.Topology.Build.URI);
        return send(new HttpPost(uri), StemResponse.class);
    }

}
