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

import org.stem.api.REST;

public class JoinRequest extends ClusterManagerRequest { // TODO: apply @lombok magic

    REST.StorageNode node = new REST.StorageNode();

    //String datacenter; /** Is used when {@link org.stem.api.request.ClusterConfiguration#isAutoApproval()} */
    //String rack;       /** is used when {@link org.stem.api.request.ClusterConfiguration#isAutoApproval()} */

    public REST.StorageNode getNode() {
        return node;
    }

//    public String getDatacenter() {
//        return datacenter;
//    }
//
//    public void setDatacenter(String datacenter) {
//        this.datacenter = datacenter;
//    }
//
//    public String getRack() {
//        return rack;
//    }
//
//    public void setRack(String rack) {
//        this.rack = rack;
//    }

    public JoinRequest() {
    }
}
