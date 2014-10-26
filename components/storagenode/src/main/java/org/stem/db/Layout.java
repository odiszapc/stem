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

package org.stem.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

// TODO: remove synchronized
public class Layout {

    private static final Logger logger = LoggerFactory.getLogger(Layout.class);
    public static Layout instance;

    public static synchronized Layout getInstance() {
        if (null == instance) {
            instance = new Layout();
        }
        return instance;
    }

    private Layout() {
    }

    public synchronized Map<UUID, MountPoint> getMountPoints() {
        return mountPoints;
    }

    public synchronized MountPoint getMountPoint(UUID id) {
        return mountPoints.get(id);
    }

    private Map<UUID, MountPoint> mountPoints = new ConcurrentHashMap<UUID, MountPoint>();

    // TODO: check for UUID duplicates
    public void load(String[] dirPaths, int vBuckets) throws IOException {
        for (String path : dirPaths) {
            try {
                logger.debug("Opening mount point {}", path);

                DataTracker dataTracker = new DataTracker(vBuckets);
                MountPoint mp = MountPoint.open(path, dataTracker, true);

                logger.info("Mount point {} opened, total blobs: {} ({} bytes), live blobs: {} ({} bytes)",
                        path,
                        dataTracker.getTotalBlobs(),
                        dataTracker.getTotalSizeInBytes(),
                        dataTracker.getLiveBlobs(),
                        dataTracker.getLiveSizeInBytes());

                mountPoints.put(mp.uuid, mp);

            } catch (IOException e) {
                logger.error("Can't load mount point {}", path);
                throw e;
            }
        }
    }

    // TODO: summaries numbers provided by DataTracker for each mount point

    public void detach() {
        for (MountPoint mountPoint : mountPoints.values()) {
            mountPoints.remove(mountPoint.uuid);
            mountPoint.close();
        }
    }
}
