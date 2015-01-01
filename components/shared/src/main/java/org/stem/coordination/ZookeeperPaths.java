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

package org.stem.coordination;

/**
 * ZNodes hierarchy:
 * <p/>
 * stem
 * ..clustermanager
 * ....cluster
 * ......descriptor  (name, rf, buckets, zookeeper_endpoint)
 * ......topology
 * ........current   (cluster topology tree: DC -> RACK -> NODE -> DISK)
 * ........mapping_previous
 * ........mapping_current
 * ........snapshot
 * ......stat        (nodes post their state to child ZNodes)
 * ....mapping       (data mapping)
 * ....streaming
 * ......out
 * ......in
 * ....async_requests
 */
public class ZookeeperPaths {

    // TODO: think about ZNodes paths
    private static final String STEM_ROOT = "/stem";
    public static final String CLUSTER_MANAGER = STEM_ROOT + "/clustermanager";
    public static final String CLUSTER = CLUSTER_MANAGER + "/cluster";
    public static final String CLUSTER_DESCRIPTOR_NAME = "descriptor";
    public static final String CLUSTER_DESCRIPTOR_PATH = CLUSTER + "/" + CLUSTER_DESCRIPTOR_NAME;

    public static final String CLUSTER_TOPOLOGY = "topology";
    public static final String CLUSTER_TOPOLOGY_PATH = CLUSTER + '/' + CLUSTER_TOPOLOGY;
    public static final String CURRENT_TOPOLOGY = "current";
    public static final String CURRENT_TOPOLOGY_PATH = CLUSTER_TOPOLOGY_PATH + '/' + CURRENT_TOPOLOGY;
    public static final String CURRENT_MAPPING = "mapping_current";
    public static final String CURRENT_MAPPING_PATH = CLUSTER_TOPOLOGY_PATH + '/' + CURRENT_MAPPING;
    public static final String PREVIOUS_MAPPING = "mapping_previous";
    public static final String PREVIOUS_MAPPING_PATH = CLUSTER_TOPOLOGY_PATH + '/' + PREVIOUS_MAPPING;
    public static final String TOPOLOGY_SNAPSHOT = "topology_snapshot";
    public static final String TOPOLOGY_SNAPSHOT_PATH = CLUSTER_TOPOLOGY_PATH + '/' + TOPOLOGY_SNAPSHOT;

    public static final String STAT = CLUSTER + "/stat";

    @Deprecated
    public static final String MAPPING = CLUSTER_MANAGER + "/mapping";
    public static final String OUT_SESSIONS = CLUSTER_MANAGER + "/streaming/out";
    public static final String IN_SESSIONS = CLUSTER_MANAGER + "/streaming/in"; // TODO: do we really need this? It's a pseudo session
    public static final String ASYNC_REQUESTS = CLUSTER_MANAGER + "/async_requests";

    public static String topologyPath() {
        return CURRENT_TOPOLOGY_PATH;
    }

    public static String currentMappingPath() {
        return CURRENT_MAPPING_PATH;
    }

    public static String previousMappingPath() {
        return PREVIOUS_MAPPING_PATH;
    }

    public static String topologySnapshotPath() {
        return TOPOLOGY_SNAPSHOT_PATH;
    }

    public static final String TOPO_MAP = "mapping";

    // TODO: recursive creation of znodes
    public static String[] containerNodes() { // will be created on startup
        return new String[]{ASYNC_REQUESTS, CLUSTER, STAT, OUT_SESSIONS, IN_SESSIONS, CLUSTER_TOPOLOGY_PATH, topologyPath()};
    }

    public static class Containers {

        public static String cluster() {
            return CLUSTER;
        }

        public static String asyncRequests() {
            return ASYNC_REQUESTS;
        }

        public static String topology() {
            return CLUSTER_TOPOLOGY_PATH;
        }
    }
}
