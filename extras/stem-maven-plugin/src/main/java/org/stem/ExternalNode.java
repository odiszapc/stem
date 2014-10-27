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

import com.google.common.collect.Lists;
import org.apache.commons.exec.*;
import org.apache.commons.exec.util.StringUtils;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.ArtifactUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.toolchain.Toolchain;
import org.apache.maven.toolchain.ToolchainManager;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.codehaus.plexus.util.IOUtil;
import org.codehaus.plexus.util.cli.CommandLineUtils;
import org.stem.utils.YamlConfigurator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class ExternalNode {

    private MavenContext mvnContext;
    private Log log;

    private final File nodeDir;
    private final YamlConfigurator configurator;
    private int mountPointsNumber = 1;

    public ExternalNode(File nodeDir, YamlConfigurator configurator, MavenContext mvnContext, Log log) {
        this.nodeDir = nodeDir;
        this.configurator = configurator;
        this.mvnContext = mvnContext;
        this.log = log;
    }

    public void start() throws IOException, MojoExecutionException {
        CommandLine commandLine = newCommandLine(nodeDir);
        startStemProcess(commandLine, createEnvironmentVars(nodeDir));
    }

    private Map<String, String> createEnvironmentVars(File nodeDir) {
        Map<String, String> env = new HashMap<String, String>();

        try {
            Properties properties = CommandLineUtils.getSystemEnvVars();
            for (Map.Entry prop : properties.entrySet()) {
                env.put((String) prop.getKey(), (String) prop.getValue());
            }
        } catch (IOException e) {
            log.error("Could not assign default system environment variables.", e);
        }
        env.put("stem.config", new File(new File(nodeDir, "conf"), "stem.yaml").getAbsolutePath());
        return env;
    }

    private DefaultExecuteResultHandler startStemProcess(CommandLine commandLine, Map env) throws MojoExecutionException {
        try {
            DefaultExecutor exec = new DefaultExecutor();
            DefaultExecuteResultHandler execHandler = new DefaultExecuteResultHandler();
            exec.setWorkingDirectory(nodeDir);
            exec.setProcessDestroyer(new ShutdownHookProcessDestroyer());

            LogOutputStream stdout = new MavenLogOutputStream(log);
            LogOutputStream stderr = new MavenLogOutputStream(log);

            log.debug("Executing command line: " + commandLine);

            PumpStreamHandler streamHandler = new PumpStreamHandler(stdout, stderr);
            streamHandler.start();
            exec.setStreamHandler(streamHandler);

            exec.execute(commandLine, env, execHandler);
//            try
//            {
//                execHandler.waitFor();
//            }
//            catch (InterruptedException e)
//            {
//                e.printStackTrace();
//            }
            return execHandler;
        } catch (IOException e) {
            throw new MojoExecutionException("Command execution failed.", e);
        }
    }

    private CommandLine newCommandLine(File nodeDir) throws IOException {
        createStemHome(nodeDir);
        CommandLine cmd = newJavaCommandLine();
        // TODO: add -Xmx, -Dlog4j.configuration, JMX configuration

        cmd.addArgument("-Dstem.config=" + new File(new File(nodeDir, "conf"), "stem.yaml").getAbsolutePath());
        cmd.addArgument("-jar");
        cmd.addArgument(new File(new File(nodeDir, "bin"), "storagenode.jar").getAbsolutePath(), false);

        return cmd;
    }

    private CommandLine newJavaCommandLine() {
        String javaExecPath = null;

        Toolchain toolchain = getToolchain();

        if (toolchain != null) {
            log.info("Toolchain: " + toolchain);
            javaExecPath = toolchain.findTool("java");

        } else if (OS.isFamilyWindows()) {
            String exec = "java.exe";
            String path = System.getenv("PATH");
            if (path != null) {
                for (String elem : StringUtils.split(path, File.pathSeparator)) {
                    File file = new File(elem, exec);
                    if (file.exists()) {
                        javaExecPath = file.getAbsolutePath();
                        break;
                    }
                }
            }

        }

        if (null == javaExecPath) {
            javaExecPath = "java";
        }

        return new CommandLine(javaExecPath);
    }

    private Toolchain getToolchain() {
        Toolchain toolchain = null;

        try {
            ToolchainManager toolchainManager =
                    (ToolchainManager) mvnContext.session.getContainer().lookup(ToolchainManager.ROLE);
            if (toolchainManager != null) {
                toolchain = toolchainManager.getToolchainFromBuildContext("jdk", mvnContext.session);
            }
        } catch (ComponentLookupException e) {
            //
        }

        return toolchain;
    }

    private void createStemHome(File nodeDir) throws IOException {
        File bin = new File(nodeDir, "bin");
        File conf = new File(nodeDir, "conf");
        File data = new File(nodeDir, "data");
        createDirsClean(Arrays.asList(bin, conf, data));

        List<File> mpDirs = newMountPointDirs(data);
        createDirsClean(mpDirs);
        newConfigYaml(conf, mpDirs);

        File jarFile = new File(bin, "storagenode.jar");
        createNodeJar(jarFile, NodeSupervisor.class.getName(), nodeDir);
    }


    private void createNodeJar(File jarFile, String mainClass, File nodeDir) throws IOException {
        File conf = new File(nodeDir, "conf");

        FileOutputStream fos = null;
        JarOutputStream jos = null;

        try {
            fos = new FileOutputStream(jarFile);
            jos = new JarOutputStream(fos);
            jos.setLevel(JarOutputStream.STORED);
            jos.putNextEntry(new JarEntry("META-INF/MANIFEST.MF"));

            Manifest man = new Manifest();

            StringBuilder cp = new StringBuilder();
            cp.append(new URL(conf.toURI().toASCIIString()).toExternalForm());
            cp.append(' ');

            log.debug("Adding plugin artifact: " + ArtifactUtils.versionlessKey(mvnContext.pluginArtifact) +
                    " to the classpath");
            cp.append(new URL(mvnContext.pluginArtifact.getFile().toURI().toASCIIString()).toExternalForm());
            cp.append(' ');

            log.debug("Adding: " + mvnContext.classesDir + " to the classpath");
            cp.append(new URL(mvnContext.classesDir.toURI().toASCIIString()).toExternalForm());
            cp.append(' ');

            for (Artifact artifact : mvnContext.pluginDependencies) {
                log.info("Adding plugin dependency artifact: " + ArtifactUtils.versionlessKey(artifact) +
                        " to the classpath");
                // NOTE: if File points to a directory, this entry MUST end in '/'.
                cp.append(new URL(artifact.getFile().toURI().toASCIIString()).toExternalForm());
                cp.append(' ');
            }

            man.getMainAttributes().putValue("Manifest-Version", "1.0");
            man.getMainAttributes().putValue("Class-Path", cp.toString().trim());
            man.getMainAttributes().putValue("Main-Class", mainClass);

            man.write(jos);

        } finally {
            IOUtil.close(jos);
            IOUtil.close(fos);
        }
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

    public ExternalNode setMounPointsNumber(int num) {
        if (num < 1 || num > 254) {
            throw new RuntimeException("number of mount points should be in range (1, 254)");
        }

        this.mountPointsNumber = num;
        return this;
    }
}
