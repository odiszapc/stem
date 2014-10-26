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

package org.stem;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.http.HttpBaseFilter;
import org.glassfish.grizzly.http.HttpContent;
import org.glassfish.grizzly.http.HttpRequestPacket;
import org.glassfish.grizzly.http.server.*;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;
import org.glassfish.jersey.server.ContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.stem.coordination.StorageStatListener;
import org.stem.coordination.ZooConstants;
import org.stem.coordination.ZookeeperClient;
import org.stem.coordination.ZookeeperClientFactory;
import org.stem.domain.Cluster;
import org.stem.exceptions.DefaultExceptionMapper;
import org.stem.exceptions.StemExceptionMapper;
import org.stem.net.CLStaticByPassHttpHandler;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class ClusterManagerLauncher {
    HttpServer server;

    public static void main(String[] args) throws InterruptedException {
        ClusterManagerLauncher launcher = new ClusterManagerLauncher();
        launcher.start();
        Thread.currentThread().join();
    }

    public void start() {
        try {
            initZookeeperPaths();
            loadClusterConfiguration();
            configureWebServer();
            startWebServer();
        } catch (Exception e) {
            throw new RuntimeException("Error while starting Cluster Manager web server", e);
        }
    }

    private void loadClusterConfiguration() {
        Cluster.init();
    }

    private void configureWebServer() throws URISyntaxException, IOException {
        URI uri = new URI("http://0.0.0.0:9997");
        ResourceConfig resourceCfg = new ResourceConfig();
        setupJsonSerialization(resourceCfg);
        resourceCfg.packages("org.stem.api.resources");
        resourceCfg.registerClasses(StemExceptionMapper.class);
        resourceCfg.registerClasses(DefaultExceptionMapper.class);
        server = createServer(uri, resourceCfg);
        configureStaticResources(server, resourceCfg);
    }

    private void startWebServer() throws IOException {
        server.start();
        server.getListener("grizzly").getFileCache().setEnabled(false);
        server.getListener("grizzly").getFilterChain().add(2, new HttpBaseFilter() // TODO: indexOfType
        {
            @Override
            public NextAction handleRead(FilterChainContext ctx) throws IOException {
                HttpContent message = ctx.getMessage();
                HttpRequestPacket httpHeader = (HttpRequestPacket) message.getHttpHeader();
                String uri = httpHeader.getRequestURI();
                if (uri.equals("/")) {
                    uri = "/admin";
                    httpHeader.getRequestURIRef().init(uri.getBytes(), 0, uri.getBytes().length);
                }

                return super.handleRead(ctx);
            }
        });
    }

    public void stop() {
        if (null != server) // TODO: why this may happen
            server.shutdownNow();

        Cluster.destroy();
    }

    private void configureStaticResources(HttpServer server, ResourceConfig resourceCfg) {
        HttpHandler handler = getStaticHandler();
        server.getServerConfiguration().addHttpHandler(handler, "/admin", "/static");
    }

    private void initZookeeperPaths() {
        ZookeeperClient client = ZookeeperClientFactory.create();
        client.start();
        try {
            client.createIfNotExists(ZooConstants.CLUSTER);
            client.listenForChildren(ZooConstants.CLUSTER, new StorageStatListener());
            //client.createIfNotExists(ZooConstants.CLUSTER_DESCRIPTOR_PATH);
        } catch (Exception e) {
            throw new RuntimeException("Can't init Zookeeper", e);
        }
    }

    private HttpServer createServer(URI uri, ResourceConfig resourceCfg) {
        final String host = uri.getHost();
        final int port = uri.getPort();
        final HttpServer server = new HttpServer();
        final NetworkListener listener = new NetworkListener("grizzly", host, port);
        listener.getTransport().setWorkerThreadPoolConfig(adjustThreadPool());
        server.addListener(listener);

        final ServerConfiguration config = server.getServerConfiguration();
        final HttpHandler handler = ContainerFactory.createContainer(HttpHandler.class, resourceCfg);

        if (handler != null) {
            config.addHttpHandler(handler, uri.getPath());
        }
        return server;
    }

    private void setupJsonSerialization(ResourceConfig cfg) {
        cfg.registerInstances(getJsonProvider());
    }

    public static JacksonJaxbJsonProvider getJsonProvider() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        //mapper.getSerializationConfig().addMixInAnnotations(File.class, MixIn_File.class);
        JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider(mapper, JacksonJaxbJsonProvider.DEFAULT_ANNOTATIONS);
        provider.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, Boolean.TRUE);
        provider.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, Boolean.FALSE);
        return provider;
    }

    private ThreadPoolConfig adjustThreadPool() {
        Integer corePoolSize = 2;
        Integer maxPoolSize = 8;
        ThreadPoolConfig workerPoolConfig = ThreadPoolConfig.defaultConfig();
        workerPoolConfig.setCorePoolSize(corePoolSize);
        workerPoolConfig.setMaxPoolSize(maxPoolSize);


        workerPoolConfig.setPoolName("Worker");
        return workerPoolConfig;
    }

    public HttpHandler getStaticHandler() {
        if (null != System.getProperty("development")) {
            return new StaticHttpHandler("src/main/resources/static/");
        } else {
            CLStaticByPassHttpHandler handler = new CLStaticByPassHttpHandler(this.getClass().getClassLoader(), "static/");
            handler.setFileCacheEnabled(false);
            return handler;
        }
    }
}
