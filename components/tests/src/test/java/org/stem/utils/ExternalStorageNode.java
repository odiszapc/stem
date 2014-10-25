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

package org.stem.utils;

import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class ExternalStorageNode {
    public static ExternalStorageNode create(File nodeDir, YamlConfigurator configurator) {
        return new ExternalStorageNode(nodeDir, configurator);
    }

    private File nodeDir;
    private YamlConfigurator configurator;
    private int mountPointsNumber = 1;

    public ExternalStorageNode(File nodeDir, YamlConfigurator configurator) {
        this.nodeDir = nodeDir;
        this.configurator = configurator;
    }

    public ExternalStorageNode setMounPointsNumber(int num) {
        if (num < 1 || num > 254) {
            throw new RuntimeException("number of mount points should be in range (1, 254)");
        }

        this.mountPointsNumber = num;
        return this;
    }

    public void start() {
        newCommandLine(nodeDir);
    }

    private CommandLine newCommandLine(File nodeDir) {
        File bin = new File(nodeDir, "bin");
        File conf = new File(nodeDir, "conf");
        File data = new File(nodeDir, "data");
        createDirsClean(Arrays.asList(bin, conf, data));

        List<File> mpDirs = newMountPointDirs(data);
        createDirsClean(mpDirs);
        newConfigYaml(conf, mpDirs);

        File jarFile = new File(bin, "storagenode.jar");
        createStorageNodeJar(jarFile, StorageNodeMonitor.class.getName(), nodeDir);
        return null;
    }

    private void createStorageNodeJar(File jarFile, String name, File nodeDir) {

    }

    private void newConfigYaml(File conf, List<File> mpDirs) {
        String[] mpPaths = getMountPointPaths(mpDirs).toArray(new String[mpDirs.size()]);
        configurator.setBlobMountPoints(mpPaths);

        configurator.save(new File(conf, "stem.yaml"));
    }

    private void createDirsClean(List<File> dirs) {
        for (File dir : dirs) {
            if (dir.isFile())
                dir.delete();

            if (!dir.isDirectory())
                dir.mkdirs();
        }
    }

    private List<File> newMountPointDirs(File conf) {
        List<File> dirs = Lists.newArrayList();
        for (int i = 1; i <= mountPointsNumber; i++) {
            File mp = new File(conf, "mountpoint" + (i > 1 ? i : ""));
            dirs.add(mp);
        }
        return dirs;
    }

    private List<String> getMountPointPaths(List<File> files) {
        List<String> list = Lists.newArrayList();
        for (File file : files) {
            list.add(file.getAbsolutePath());
        }
        return list;
    }

}
