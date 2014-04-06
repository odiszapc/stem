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

import java.util.List;

public class AddStorageNodeRequest extends BlobManagerRequest
{
    String ipAddress;
    int port;
    List<Disk> disks;

    public AddStorageNodeRequest()
    {
    }

    public String getIpAddress()
    {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress)
    {
        this.ipAddress = ipAddress;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public List<Disk> getDisks()
    {
        return disks;
    }

    public void setDisks(List<Disk> disks)
    {
        this.disks = disks;
    }

    public static class Disk
    {
        String id;
        long usedBytes;
        long totalBytes;

        public Disk()
        {
        }

        public String getId()
        {
            return id;
        }

        public void setId(String id)
        {
            this.id = id;
        }

        public long getUsedBytes()
        {
            return usedBytes;
        }

        public void setUsedBytes(long usedBytes)
        {
            this.usedBytes = usedBytes;
        }

        public long getTotalBytes()
        {
            return totalBytes;
        }

        public void setTotalBytes(long totalBytes)
        {
            this.totalBytes = totalBytes;
        }
    }
}
