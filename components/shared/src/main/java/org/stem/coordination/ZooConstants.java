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

public class ZooConstants {
    // TODO: think about ZNodes paths
    private static final String STEM_ROOT = "/stem";
    private static final String CLUSTER_MANAGER = STEM_ROOT + "/clustermanager";
    public static final String CLUSTER = CLUSTER_MANAGER + "/cluster";
    public static final String CLUSTER_DESCRIPTOR_NAME = "descriptor";
    public static final String CLUSTER_DESCRIPTOR_PATH = CLUSTER + "/" + CLUSTER_DESCRIPTOR_NAME;
    public static final String TOPOLOGY = CLUSTER_MANAGER + "/topology";
    public static final String OUT_SESSIONS = CLUSTER_MANAGER + "/streaming/out";
    public static final String IN_SESSIONS = CLUSTER_MANAGER + "/streaming/in"; // TODO: do we really need this? It's pseudo session

    public static final String TOPO_MAP = "mapping";
}
