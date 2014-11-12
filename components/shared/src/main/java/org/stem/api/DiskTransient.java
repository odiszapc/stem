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

public class DiskTransient {
    private String id; // TODO: use UUID type
    private String path;
    private long total;
    private long used;

    public String getId() {
        return id;
    }

    public String getPath() {
        return path;
    }

    public long getTotal() {
        return total;
    }

    public long getUsed() {
        return used;
    }

    public DiskTransient() {
    }

    public DiskTransient(String id, String path, long used, long total) {
        this.id = id;
        this.path = path;
        this.used = used;
        this.total = total;
    }
}
