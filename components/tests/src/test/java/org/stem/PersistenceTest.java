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

import org.fest.assertions.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.stem.api.ClusterManagerClient;
import org.stem.api.response.ClusterResponse;

import java.io.IOException;

public class PersistenceTest extends IntegrationTestBase
{

    @Override
    @Before
    public void setUp() throws IOException
    {
        TestUtil.cleanupTempDir();
        TestUtil.createTempDir();
        startZookeeperEmbedded();
        startClusterManagerEmbedded();
        waitForClusterManager();
        clusterManagerClient = ClusterManagerClient
                        .create("http://localhost:9997");
    }

    @After
    public void tearDown() throws InterruptedException
    {
        stopClusterManagerEmbedded();
        shoutDownZookeeperClients();
        stopZookeeperEmbedded();
    }

    @Test
    public void clusterStateShouldBeSavedBetweenRestarts()
    {
        initCluster();

        ClusterResponse clusterResponse = clusterManagerClient.describeCluster();
        assertClusterState(clusterResponse);

        restartClusterManagerEmbedded();
        waitForClusterManager();

        clusterResponse = clusterManagerClient.describeCluster();
        assertClusterState(clusterResponse);
    }

    private void assertClusterState(ClusterResponse clusterResponse)
    {
        Assertions.assertThat(clusterResponse.getCluster().getName()).isEqualTo(getClusterName());
        Assertions.assertThat(clusterResponse.getCluster().getRf()).isEqualTo(getRF());
        Assertions.assertThat(clusterResponse.getCluster().getvBucketsNum()).isEqualTo(getvBucketsNum());
    }
}
