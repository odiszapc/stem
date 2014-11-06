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

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.stem.api.request.InitClusterRequest;
import org.stem.api.request.JoinRequest;
import org.stem.api.response.ClusterResponse;
import org.stem.api.response.JoinResponse;
import org.stem.api.response.StemResponse;
import org.stem.coordination.Event;
import org.stem.coordination.EventResultFuture;
import org.stem.coordination.ZookeeperClient;

import java.net.URI;

public class ClusterManagerClient extends BaseHttpClient {

    public static ClusterManagerClient create(String uri) {
        return new ClusterManagerClient(uri);
    }

    public ClusterManagerClient(String uri) {
        super(uri);
    }

    public void join(JoinRequest msg) {
        try {
            URI uri = getURI(RESTConstants.Api.Cluster.Join.URI);
            HttpPut request = new HttpPut(uri);

            StemResponse send = send(request, msg, StemResponse.class);

        } catch (Exception e) {
            throw new RuntimeException("Can't join cluster, ClusterManager response: " + e.getMessage());
        }
    }

    public void join2(ZookeeperClient client, JoinRequest message) {
        try {
            URI uri = getURI(RESTConstants.Api.Cluster.Join.URI);
            JoinResponse response = send(new HttpPut(uri), message, JoinResponse.class);

            assert null != response.requestId;

            EventResultFuture future = new EventResultFuture();
            Event.Factory
                    .newHandler(response.requestId, Event.Type.JOIN, future, client)
                    .start();
            StemResponse delayedResponse = Uninterruptibles.getUninterruptibly(future);

        } catch (Exception e) {
            throw new RuntimeException("Can't join cluster, ClusterManager response: " + e.getMessage());
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


    public StemResponse initCluster(String clusterName, int vBuckets, int rf, String partitioner) {
        URI uri = getURI(RESTConstants.Api.Cluster.Init.URI);
        HttpPut request = new HttpPut(uri);

        InitClusterRequest initRequest = new InitClusterRequest(clusterName, vBuckets, rf, partitioner);

        return send(request, initRequest, StemResponse.class);
    }


    public StemResponse computeMapping() {
        URI uri = getURI(RESTConstants.Api.Topology.Build.URI);
        return send(new HttpPost(uri), StemResponse.class);
    }

}
