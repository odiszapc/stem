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

import java.util.Arrays;

/**
 * ZNodes hierarchy:
 *
 * stem
 * ..clustermanager
 * ....cluster
 * ......descriptor  (name, rf, buckets, zookeeper_endpoint)
 * ......topology    (cluster topology tree: DC -> RACK -> NODE -> DISK)
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
    public static final String STAT = CLUSTER + "/stat";
    public static final String MAPPING = CLUSTER_MANAGER + "/mapping"; // TODO: rename to '/mappings/
    public static final String OUT_SESSIONS = CLUSTER_MANAGER + "/streaming/out";
    public static final String IN_SESSIONS = CLUSTER_MANAGER + "/streaming/in"; // TODO: do we really need this? It's pseudo session
    public static final String ASYNC_REQUESTS = CLUSTER_MANAGER + "/async_requests";

    public static final String TOPO_MAP = "mapping";

    public static String[] containerNodes() {
        return new String[]{ASYNC_REQUESTS, CLUSTER, CLUSTER_TOPOLOGY_PATH};
    }
}
