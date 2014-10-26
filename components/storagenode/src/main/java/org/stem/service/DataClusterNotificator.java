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
import org.stem.coordination.*;
import org.stem.db.Layout;
import org.stem.db.MountPoint;
import org.stem.db.StorageNodeDescriptor;

public class DataClusterNotificator implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataClusterNotificator.class);

    ZookeeperClient client; // TODO: the client instance must be a singleton ???


    public DataClusterNotificator() {
        client = ZookeeperClientFactory.create();
        client.start();
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
        StorageStat stat = new StorageStat(StorageNodeDescriptor.getNodeListen(), StorageNodeDescriptor.getNodePort());

        for (MountPoint mp : Layout.getInstance().getMountPoints().values()) {
            DiskStat disk = new DiskStat(mp.uuid.toString());
            disk.setPath(mp.getPath());
            disk.setTotalBytes(mp.getAllocatedSizeInBytes());
            disk.setUsedBytes(mp.getTotalSizeInBytes());
            stat.getDisks().add(disk);
        }

        // Stub to test progress bars oscillating values
//        int from = 60;
//        int to = 80;
//        double ratio = Math.random() * (to-from) + from;
//        stat.getDisks().get(0).setUsedBytes(Math.round(stat.getDisks().get(0).getUsedBytes() * ratio/100));
        // Stub

        client.updateNode(ZooConstants.CLUSTER, stat);
    }

    private void sleep() {
        try {


            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
