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

package org.stem.tools.cli;

import org.apache.commons.cli.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.stem.client.Blob;
import org.stem.client.ClientInternalError;
import org.stem.client.Session;
import org.stem.client.StemCluster;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StemCli {

    private static final int INTERACTIVE_MODE = 1;
    private static final int MAX_FILE_SIZE = 100; //Max size of file is 100MB
    private static final int MIN_QUANTITY_ARGS = 2; //Min quantity of args in commands from file
    private static final Options options;
    private static final CommandLineParser parser = new PosixParser();
    private static Session session = null;

    static {
        options = buildOptions();
    }

    @SuppressWarnings("all")
    private static Options buildOptions() {
        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("data")
                .hasArg()
                .withArgName("DATA")
                .create());
        options.addOption(OptionBuilder.withLongOpt("src")
                .hasArg()
                .withArgName("FILE")
                .create());
        options.addOption(OptionBuilder.withLongOpt("dst")
                .hasArg()
                .withArgName("FILE")
                .create());
        options.addOption(OptionBuilder.withLongOpt("manager")
                .hasArg()
                .withArgName("--manager=<URL>")
//                .isRequired()
                .create());
        options.addOption(OptionBuilder.withLongOpt("file")
                .hasArg()
                .withArgName("FILE")
                .create());
        options.addOption(OptionBuilder.withLongOpt("help")
                .create());
        return options;
    }

    public static void main(String[] args) {
        StemCli cli = new StemCli(args);
        cli.run();
        System.exit(0);
    }

    private enum Mode {
        INTERACTIVE, BATCH, SINGLE
    }

    CommandLine cmd;
    private String[] args;
    private Mode mode;

    public StemCli(String[] args) {
        this.args = args;
        if (args.length == 0) {
            usage();
            System.exit(1);
        }

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            usage();
            System.exit(1);
        }

        if (cmd.hasOption("help")) {
            usage();
            System.exit(0);
        }

        if (args.length == INTERACTIVE_MODE)
            mode = Mode.INTERACTIVE;
        else if (cmd.hasOption("file"))
            mode = Mode.BATCH;
        else
            mode = Mode.SINGLE;
    }

    private void run() {
        try {
            connect(cmd.getOptionValue("manager"));
        } catch (Exception ex) {
            System.err.println("Unable to connect to cluster manager" + ex.getMessage());
            System.exit(1);
        }

        switch (mode) {
            case INTERACTIVE:
                interactiveMode();
                break;
            case BATCH:
                try {
                    parsingFile(cmd.getOptionValue("file"));
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                }
                break;
            default:
                try {
                    processing(cmd, args);
                } catch (Exception ex) {
                    System.err.println(ex.getMessage());
                }
        }
    }

    /**
     * Establish a session with org.stem.client.StemCluster
     */
    private static void connect(String url) {
        StemCluster cluster = new StemCluster.Builder()
                .withClusterManagerUrl(url)
                .build();

        session = cluster.connect();
    }

    private static void usage() {
        System.out.println("Usage (batch mode):");
        new HelpFormatter().printHelp("stem-cli [<COMMAND>] [<KEY>] [--data <DATA>] [--dst <FILE>] [--src <FILE>] [--file <FILE>] --manager=<URL>", options);
        System.out.println("Usage (interactive mode):");
        new HelpFormatter().printHelp("<COMMAND> <KEY> [--data <DATA>] [--dst <FILE>] [--src <FILE>] [--file <FILE>]", options);
    }

    private static void interactiveMode() {
        Console input = System.console();
        if (input == null) {
            System.err.println("There is no console.");
            System.exit(1);
        }
        System.out.println("Enter 'quit' to quit");

        String inputString;
        CommandLine cmd;

        while (true) {
            try {
                inputString = input.readLine("> ");
            } catch (IOError e) {
                System.err.println(e.getMessage());
                continue;
            }

            inputString = inputString.trim();

            if (inputString.isEmpty())
                continue;

            if (inputString.equals("quit"))
                return;

            try {
                cmd = parsingArgs(inputString, true);
            } catch (ParseException pe) {
                System.err.println(pe.getMessage());
                continue;
            }
            try {
                processing(cmd, inputString.split(" "));
            } catch (Exception ex) {
                System.err.println(ex.getMessage());
            }
        }
    }

    /**
     * Parsing commands from file line-by-line like in interaction mode
     *
     * @param fileName file to read commands from
     * @throws IOException
     * @throws FileNotFoundException
     */
    private static void parsingFile(String fileName) throws IOException {
        BufferedReader reader;
        reader = new BufferedReader(new FileReader(fileName));
        String line;
        int lineNumber = 0;
        CommandLine cmd;
        String[] args;

        while ((line = reader.readLine()) != null) {
            line = line.trim();
            ++lineNumber;

            if (line.isEmpty() || line.startsWith("#"))
                continue;

            try {
                cmd = parsingArgs(line, true);
            } catch (ParseException pe) {
                System.out.println("Error occurred in the line " + lineNumber + " of file.");
                System.out.println(pe.getMessage());
                continue;
            }
            try {
                args = line.split(" ");
                processing(cmd, args);
            } catch (Exception ile) {
                System.err.println(ile.getMessage());
            }
        }
    }

    private static CommandLine parsingArgs(String inputString, boolean interactiveMode) throws ParseException {
        String[] inputArgs = inputString.split(" ");

        CommandLine cmd = parser.parse(options, inputArgs);
        if (!interactiveMode && !cmd.hasOption("manager") && !cmd.hasOption("help"))
            throw new ParseException("There is no '--manager' option!");

        if (inputArgs.length < MIN_QUANTITY_ARGS)
            throw new ParseException("Too few arguments");

        return cmd;
    }

    /**
     * Processing commands
     *
     * @param cmd command line object
     * @param args arguments
     * @throws IllegalArgumentException
     * @throws IOException
     * @throws ClientInternalError
     */
    private static void processing(CommandLine cmd, String[] args) throws IOException {
        long startTime = System.nanoTime();
        if (args[1].startsWith("--")) {
            throw new IllegalArgumentException(String.format("Unknown argument '%s'", args[1]));
        }
        byte[] data;
        switch (args[0]) {
            case "put":
                if (cmd.hasOption("data")) {
                    data = cmd.getOptionValue("data").getBytes();
                } else {
                    data = dataFromFile(cmd.getOptionValue("src"));
                }
                Blob blob = Blob.create(DigestUtils.md5(args[1].replace("\"", "").getBytes()), data);
                session.put(blob);
                break;
            case "get":
                Blob stored = session.get(DigestUtils.md5(args[1].replace("\"", "").getBytes()));
                if (stored == null)
                    return;
                if (cmd.hasOption("dsc")) {
                    dataToFile(stored.body, cmd.getOptionValue("dsc"));
                } else {
                    for (int i = 0; i < stored.getBlobSize(); i++) {
                        System.out.print((char) stored.body[i]);
                    }
                    System.out.println("");
                }
                break;
            case "delete":
                session.delete(DigestUtils.md5(args[1].replace("\"", "").getBytes()));
                break;
            default:
                throw new IllegalArgumentException("Method " + args[0] + " is not allowed");
        }
        elapsedTime(startTime, args[0]);
    }

    /**
     * Get data from file
     *
     * @param fileName file name to read commands from
     * @return byte[] binary data
     * @throws IOException
     */
    private static byte[] dataFromFile(String fileName) throws IOException {
        FileInputStream fis = null;
        Path filePath = Paths.get(fileName);
        if (!Files.exists(filePath) && !Files.isRegularFile(filePath)) {
            throw new FileNotFoundException("There is no file or it is not regular file.");
        }

        if (Files.size(filePath) > MAX_FILE_SIZE) {
            throw new IOException("File is too big");
        }
        byte[] blob = new byte[(int) Files.size(filePath)];

        try {
            fis = new FileInputStream(fileName);
            return blob;
        } catch (IOException ex) {
            throw new IOException(ex.getMessage());
        } finally {
            if (fis != null)
                fis.close();
        }

    }

    /**
     * Save data to file
     *
     * @param blob binary data
     * @param fileName file to save result to
     * @throws IOException
     */
    private static void dataToFile(byte[] blob, String fileName) throws IOException {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(fileName);
            fos.write(blob);
            fos.flush();
        } catch (IOException ex) {
            throw new IOException(ex.getMessage());
        } finally {
            if (fos != null)
                fos.close();
        }
    }

    /**
     * Print elapsed time. Print 2 fraction digits if eta is under 10 ms.
     *
     * @param startTime starting time in nanoseconds
     */
    private static void elapsedTime(long startTime, String commandName) {
        long eta = System.nanoTime() - startTime;
        System.out.printf("Elapsed time for command %s: ", commandName);
        if (eta < 10000000) {
            System.out.print(Math.round(eta / 10000.0) / 100.0);
        } else {
            System.out.print(Math.round(eta / 1000000.0));
        }
        System.out.println(" msec(s).");
    }
}
