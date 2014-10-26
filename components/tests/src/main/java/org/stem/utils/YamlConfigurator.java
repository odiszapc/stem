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


import org.stem.config.Config;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Random;

/**
 * YamlConfigurator.open("stem.yaml")
 */
public class YamlConfigurator {

    public static YamlConfigurator open(URL configUrl) {
        return new YamlConfigurator(configUrl);
    }

    public static YamlConfigurator open(String configPath) {
        try {
            return new YamlConfigurator(configPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Random random = new Random();
    private URL url;
    private Config config;

    public URI getURI() {
        try {
            return url.toURI();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private YamlConfigurator(URL configUrl) {
        try {
            if (null == configUrl)
                throw new NullPointerException("URL parameter is null");

            url = configUrl;
            Constructor constructor = new Constructor(Config.class);
            Yaml yaml = new Yaml(constructor);
            InputStream stream = url.openStream();
            config = yaml.loadAs(stream, Config.class);
            stream.close(); // TODO; utilize the power of Guava
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private YamlConfigurator(String configPath) {
        this(convertPathToURL(configPath));
    }

    public YamlConfigurator setAutoAllocate(Boolean value) {
        config.auto_allocate = value;
        return this;
    }

    public YamlConfigurator setBlobMountPoints(String... value) {
        config.blob_mount_points = value;
        return this;
    }

    public YamlConfigurator setBlobMountPoint(String value) {
        config.blob_mount_points = new String[]{value};
        return this;
    }

    public YamlConfigurator setBlobManagerEndpoint(String value) {
        config.cluster_manager_endpoint = value;
        return this;
    }

    public YamlConfigurator setNodeListen(String value) {
        config.node_listen = value;
        return this;
    }

    public YamlConfigurator setFatFileSizeInMb(Integer value) {
        config.fat_file_size_in_mb = value;
        return this;
    }

    public YamlConfigurator setMarkOnAllocate(Boolean value) {
        config.mark_on_allocate = value;
        return this;
    }

    public YamlConfigurator setMaxSpaceAllocationInMb(Integer value) {
        config.max_space_allocation_in_mb = value;
        return this;
    }

    public YamlConfigurator setCompactionThreshold(Float value) {
        config.compaction_threshold = value;
        return this;
    }

    public String save() {
        try {
            File file = new File(url.toURI());
            int index = Math.abs(random.nextInt());
            String newPath = file.getParentFile().getAbsolutePath() + File.separator + index + ".yaml";
            save(newPath);
            return newPath;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String saveTo(String dir) {
        try {
            int index = Math.abs(random.nextInt());
            String path = dir + File.separator + index + ".yaml";
            save(path);
            return path;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void save(String configPath) {
        File newFile = new File(configPath);
        save(newFile);
    }

    public void save(File newFile) {
        try {
            Yaml yamlDumper = new Yaml();
            FileWriter writer = new FileWriter(newFile);
            yamlDumper.dump(config, writer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static URL convertPathToURL(String configPath) {
        try {
            URL url = Thread.currentThread().getContextClassLoader().getResource(configPath);
            if (null == url)
                throw new Exception(configPath + " can not be found");
            return url;
        } catch (Exception e) {
            throw new RuntimeException("Can not convert String path to URL object", e);
        }

    }
}
