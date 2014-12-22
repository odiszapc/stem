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

package org.stem.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.api.REST;
import org.stem.coordination.ZooException;
import org.stem.coordination.ZookeeperClient;
import org.stem.coordination.ZookeeperClientFactory;
import org.stem.coordination.ZookeeperPaths;
import org.stem.db.Layout;
import org.stem.db.MountPoint;
import org.stem.db.StorageNodeDescriptor;
import org.stem.utils.Utils;

import java.util.Collection;

import static org.stem.db.StorageNodeDescriptor.*;

public class DataClusterNotificator implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataClusterNotificator.class);
    private static final int DELAY_MS = 1000;

    ZookeeperClient client; // TODO: the client instance must be a singleton ???

    public DataClusterNotificator() throws ZooException {
        client = ZookeeperClientFactory.newClient(StorageNodeDescriptor.cluster().getZookeeperEndpoint());
    }

    @Override
    public void run() {
        while (true) {
            try {
                doWork();
            } catch (Exception e) {
                logger.warn("Error occurred during notify Zookeeper", e);
            }

            sleep();
        }
    }

    private void doWork() throws Exception {
        // Stub to test progress bars oscillating values
//        int from = 60;
//        int to = 80;
//        double ratio = Math.random() * (to-from) + from;
//        stat.getDisks().get(0).setUsedBytes(Math.round(stat.getDisks().get(0).getUsedBytes() * ratio/100));
        // Stub
        REST.StorageNode stat = packNode(Layout.getInstance().getMountPoints().values());
        client.updateNode(ZookeeperPaths.STAT, stat);
    }

    private static REST.StorageNode packNode(Collection<MountPoint> disks) {
        REST.StorageNode result = new REST.StorageNode(id, Utils.getMachineHostname(),
                getNodeListenAddress() + ':' + getNodeListenPort(), 0l);

        long total = 0;
        for (MountPoint mp : disks) {
            total += mp.getAllocatedSizeInBytes();
            REST.Disk disk = new REST.Disk(mp.getId(), mp.getPath(),
                    mp.getTotalSizeInBytes(), mp.getAllocatedSizeInBytes()); // TODO: think about TotalSizeInBytes... it should be ...used...
            result.getDisks().add(disk);
        }
        result.setCapacity(total);
        return result;
    }

    private void sleep() {
        try {
            Thread.sleep(DELAY_MS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
