/*
 * Copyright 2015 Alexey Plotnik
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


public class ClusterConfiguration {

    private static final boolean AUTO_APPROVAL_DEFAULT = false;
    private static final String DATACENTER_DEFAULT = "DC1";
    private static final String RACK_DEFAULT = "RACK1";


    private boolean autoApproval = AUTO_APPROVAL_DEFAULT;


    public ClusterConfiguration() {
        this(AUTO_APPROVAL_DEFAULT);
    }

    public ClusterConfiguration(boolean autoApproval) {
        this.autoApproval = autoApproval;
    }

    public ClusterConfiguration setAutoApproval(boolean autoApproval) {
        this.autoApproval = autoApproval;
        return this;
    }

    public boolean isAutoApproval() {
        return autoApproval;
    }

    @Override
    public String toString() {
        return "ClusterConfiguration{" +
                "autoApproval=" + autoApproval +
                '}';
    }
}
