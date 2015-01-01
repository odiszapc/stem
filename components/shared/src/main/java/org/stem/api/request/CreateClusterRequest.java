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

package org.stem.api.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateClusterRequest extends ClusterManagerRequest {

    String name;
    int vBuckets;
    int rf;
    String partitioner;

    @JsonProperty(value = "meta")
    MetaStoreConfiguration metaStoreConfiguration = new MetaStoreConfiguration();

    @JsonProperty(value = "configuration")
    ClusterConfiguration configuration = new ClusterConfiguration();

    public String getName() {
        return name;
    }

    public int getvBuckets() {
        return vBuckets;
    }

    public int getRf() {
        return rf;
    }

    public String getPartitioner() {
        return partitioner;
    }

    public MetaStoreConfiguration getMetaStoreConfiguration() {
        return metaStoreConfiguration;
    }

    public ClusterConfiguration getConfiguration() {
        return configuration;
    }

    public CreateClusterRequest() {
    }

    public CreateClusterRequest(String name,
                                int vBuckets,
                                int rf,
                                String partitioner,
                                MetaStoreConfiguration metaStoreConfiguration,
                                ClusterConfiguration configuration) {
        this.name = name;
        this.vBuckets = vBuckets;
        this.rf = rf;
        this.partitioner = partitioner;
        this.metaStoreConfiguration = metaStoreConfiguration;
        this.configuration = configuration;
    }
}
