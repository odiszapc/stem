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

package org.stem.config;

public class Config {

    public String cluster_manager_endpoint;
    public Boolean auto_allocate = false;
    public String[] blob_mount_points;
    public String node_listen = "0.0.0.0";
    public Integer node_port = 9998;
    public Integer fat_file_size_in_mb = 256;
    public Boolean mark_on_allocate = true;
    public Integer max_space_allocation_in_mb = 0;
    public Float compaction_threshold = 0.3f;
}
