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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class StorageNodeTransient {
    UUID id;
    String hostname;
    String listen;
    Long capacity;
    List<DiskTransient> disks = new ArrayList<>();
    private List<String> ipAddresses = new ArrayList<String>();

    public UUID getId() {
        return id;
    }

    public String getHostname() {
        return hostname;
    }

    public String getListen() {
        return listen;
    }

    public Long getCapacity() {
        return capacity;
    }

    public List<DiskTransient> getDisks() {
        return disks;
    }

    public List<String> getIpAddresses() {
        return ipAddresses;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public void setListen(String listen) {
        this.listen = listen;
    }

    public void setListen(String host, int port) {
        this.listen = host + ':' + port;
    }

    public void setCapacity(Long capacity) {
        this.capacity = capacity;
    }

    public StorageNodeTransient() {
    }

    public StorageNodeTransient(UUID id, String hostname, String listen, Long capacity) {
        this.id = id;
        this.hostname = hostname;
        this.listen = listen;
        this.capacity = capacity;
    }
}
