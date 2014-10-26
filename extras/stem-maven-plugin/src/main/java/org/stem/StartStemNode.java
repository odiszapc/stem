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

import org.apache.maven.artifact.Artifact;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;
import org.stem.utils.YamlConfigurator;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Starts Stem Storage node instance
 *
 * @goal start
 * @phase pre-integration-test
 */
public class StartStemNode
        extends AbstractMojo {

    /**
     * Location of the file.
     *
     * @parameter expression="${project.build.directory}/stem"
     * @required
     */
    private File stemDir;

    /**
     * The enclosing project.
     *
     * @parameter default-value="${project}"
     * @required
     * @readonly
     */
    protected MavenProject project;

    /**
     * @parameter expression="${project.build.outputDirectory}"
     * @required
     */
    private File classesDir;

    /**
     * @parameter default-value="${plugin.artifacts}"
     * @readonly
     */
    private List<Artifact> pluginDependencies;

    /**
     * @parameter default-value="${plugin.pluginArtifact}"
     * @readonly
     */
    private Artifact pluginArtifact;

    /**
     * The current build session instance. This is used for toolchain manager API calls.
     *
     * @parameter default-value="${session}"
     * @required
     * @readonly
     */
    protected MavenSession session;

    public void execute()
            throws MojoExecutionException {
        try {
            MavenContext mvnContext = new MavenContext(project, classesDir, pluginDependencies, pluginArtifact, session);

            File nodeDir = new File(stemDir, "node1");
            YamlConfigurator configurator = YamlConfigurator.open("stem.yaml");
            ExternalNode node = new ExternalNode(nodeDir, configurator, mvnContext, getLog());

            node.start();
        } catch (IOException e) {
            throw new MojoExecutionException(e.getLocalizedMessage(), e);
        }

    }
}
