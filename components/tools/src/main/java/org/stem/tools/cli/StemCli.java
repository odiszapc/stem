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
import org.stem.api.REST;
import org.stem.client.*;
import org.stem.utils.JsonUtils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import static org.stem.tools.cli.Utils.newOpt;
import static org.stem.tools.cli.Utils.printLine;

public class StemCli {

    private static final int INTERACTIVE_MODE_ARG_LENGTH = 1;
    private static final int MAX_FILE_SIZE = 100; //Max size of file is 100MB
    private static final int MIN_QUANTITY_ARGS = 2; //Min quantity of args in commands from file
    private static final int URL = 1;
    private static final int CONSISTENCY = 2; //Interactive mode with consistency argument

    public static final String CONNECT = "connect";
    private static final String PUT = "put";
    private static final String GET = "get";
    private static final String DELETE = "delete";
    private static final String DESCRIBE = "describe";
    private static final String HELP = "help";

    private static final String ONE = "one";
    private static final String TWO = "two";
    private static final String THREE = "three";
    private static final String QUORUM = "quorum";
    private static final String ALL = "all";

    private enum Mode {
        INTERACTIVE, BATCH, SINGLE
    }

    private enum Argument {
        COMMAND, KEY, DATA
    }

    public static void main(String[] args) {
        StemCli cli = new StemCli(args);
        cli.run();
        System.exit(0);
    }

    private final CommandLineParser parser = new PosixParser();
    private final Options options;
    private final Scanner console = new Scanner(System.in);
    private final String[] args;

    private CommandLine cmd;
    private Mode mode;

    private StemCluster cluster;
    private Session session = null;

    @SuppressWarnings("all")
    private Options buildOptions() {
        Options options = new Options();
        options.addOption(newOpt("data", "DATA"))
                .addOption(newOpt("src", "FILE"))
                .addOption(newOpt("dst", "FILE"))
                .addOption(newOpt("manager", "URL"))
                .addOption(newOpt("file", "FILE"))
                .addOption(newOpt("consistency", "NAME"))
                .addOption(newOpt("help", null));
        return options;
    }

    public StemCli(String[] args) {
        options = buildOptions();
        this.args = args;

        if (args.length > 0) {
            try {
                cmd = parser.parse(options, args);
            } catch (ParseException e) {
                printLine(e.getMessage());
                usage();
                System.exit(1);
            }
            if (cmd.hasOption("help")) {
                usage();
                System.exit(0);
            }
        }

        if (args.length <= INTERACTIVE_MODE_ARG_LENGTH ||
                (args.length <= INTERACTIVE_MODE_ARG_LENGTH + CONSISTENCY && this.cmd.hasOption("consistency")))
            mode = Mode.INTERACTIVE;
        else if (cmd.hasOption("file"))
            mode = Mode.BATCH;
        else
            mode = Mode.SINGLE;
    }

    private void run() {
        if (cmd != null && cmd.hasOption("manager")) {
            try {
                connect(cmd.getOptionValue("manager"), cmd);
            } catch (Exception ex) {
                printLine(ex.getMessage());
            }
        }
        switch (mode) {
            case INTERACTIVE:
                interactiveMode();
                break;
            case BATCH:
                if (session != null) {
                    try {
                        parsingFile(cmd.getOptionValue("file"));
                    } catch (IOException e) {
                        printLine(e.getMessage());
                    }
                } else {
                    printLine("There is no connection to cluster manager.");
                }
                break;
            default:
                try {
                    processing(cmd, args);
                } catch (Exception ex) {
                    printLine(ex.getMessage());
                }
        }
    }

    /**
     * Establish connection to cluster
     * @param url
     * @param cmd
     */
    private void connect(String url, CommandLine cmd) {

        if (cmd != null && cmd.hasOption("consistency")) {
            this.cluster = buildCluster(url, getConsistency(cmd.getOptionValue("consistency")));
        } else {
            this.cluster = buildCluster(url);
        }
        session = cluster.connect();

        printLine(String.format("Connected to \"%s\" on %s", cluster.getName(), url));
    }

    private StemCluster buildCluster(String url) {
        return new StemCluster.Builder()
                .withClusterManagerUrl(url)
                .build();
    }

    private StemCluster buildCluster(String url, Consistency.Level level) {
        return new StemCluster.Builder()
                .withClusterManagerUrl(url)
                .withQueryOpts(new QueryOpts().setConsistency(level))
                .build();
    }

    private void usage() {
        printLine("Usage (batch mode):");
        new HelpFormatter().printHelp("stem-cli [<COMMAND>] [<KEY>] [--data <DATA>] [--dst <FILE>] [--src <FILE>] [--file <FILE>] [--manager=<URL>] [--consistency <NAME>]", options);
        printLine();
        printLine("Usage (interactive mode):");
        new HelpFormatter().printHelp("<COMMAND> <KEY> [--data <DATA>] [--dst <FILE>] [--src <FILE>] [--file <FILE>] [--consistency <NAME>]", options);
    }

    private void usageInteractiveMode() {
        printLine();
        printLine("Usage commands: ");
        printLine("connect <URL> [--consistency <NAME>] - Connect to cluster, consistency=<ONE,TWO,THREE,QUORUM,ALL>");
        printLine("put <KEY> [<DATA> or --data <DATA>] [--src <FILE>]  - Put data to storage");
        printLine("get <KEY> [--dst <FILE>] - Show saved data or save it in to file");
        printLine("delete <KEY> - Delete data from storage");
        printLine("describe - Show information about cluster.");
    }

    private void interactiveMode() {
        if (console == null) {
            System.err.println("There is no console.");
            System.exit(1);
        }
        printLine("Enter 'quit' to exit interactive mode");

        String inputString;
        CommandLine cmd;

        while (true) {
            try {
                inputString = readUserInput().trim();

                if (inputString.isEmpty())
                    continue;

                if (inputString.equals("quit"))
                    return;

                cmd = parsingArgs(inputString, true);

                processing(cmd, inputString.split(" "));
            } catch (Exception ex) {
                printLine(ex.getMessage());
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
    private void parsingFile(String fileName) throws IOException {
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
                printLine("Error occurred in the line " + lineNumber + " of file.");
                printLine(pe.getMessage());
                continue;
            }
            try {
                args = line.split(" ");
                processing(cmd, args);
            } catch (Exception ile) {
                printLine(ile.getMessage());
            }
        }
    }

    private CommandLine parsingArgs(String inputString, boolean interactiveMode) throws ParseException {
        String[] args = inputString.split(" ");

        CommandLine cmd = parser.parse(options, args);
        if (!interactiveMode && !cmd.hasOption("manager") && !cmd.hasOption("help"))
            throw new ParseException("There is no '--manager' option!");

        if (args.length == 0 && !args[Argument.COMMAND.ordinal()].equals("help") && args.length < MIN_QUANTITY_ARGS)
            throw new ParseException("Too few arguments");

        return cmd;
    }

    /**
     * Processing commands
     *
     * @param cmd  command line object
     * @param args arguments
     * @throws IllegalArgumentException
     * @throws IOException
     * @throws ClientInternalError
     */
    private void processing(CommandLine cmd, String[] args) throws IOException, ParseException, ClientException {
        long startTime = System.nanoTime();

        if (session == null && (!args[Argument.COMMAND.ordinal()].equals("connect") &&
                !args[Argument.COMMAND.ordinal()].equals("help"))) {
            return;
        }

        byte[] data;
        switch (args[Argument.COMMAND.ordinal()]) {
            case CONNECT:
                try {
                    connect(args[URL], cmd);
                } catch (Exception ex) {
                    printLine(ex.getMessage());
                }
                break;
            case PUT:
                if (args.length <= MIN_QUANTITY_ARGS)
                    throw new ParseException("Too few arguments for put command");

                if (mode == Mode.INTERACTIVE && !cmd.hasOption("data") && !cmd.hasOption("src")) {
                    if (args[Argument.DATA.ordinal()].startsWith("--")) {
                        throw new IllegalArgumentException(String.format("Wrong argument '%s'", args[Argument.DATA.ordinal()]));
                    }
                    data = args[Argument.DATA.ordinal()].replace("\"", "").getBytes();
                } else if (cmd.hasOption("data")) {
                    data = cmd.getOptionValue("data").replace("\"", "").getBytes();
                } else {
                    data = Utils.readFromFile(cmd.getOptionValue("src"), MAX_FILE_SIZE);
                }

                Blob blob = Blob.create(DigestUtils.md5(args[Argument.KEY.ordinal()].replace("\"", "").getBytes()), data);
                session.put(blob);
                break;
            case GET:
                Blob stored = session.get(DigestUtils.md5(args[Argument.KEY.ordinal()].replace("\"", "").getBytes()));

                if (stored == null)
                    break;

                if (cmd.hasOption("dsc")) {
                    Utils.writeToFile(stored.body, cmd.getOptionValue("dsc"));
                } else {
                    for (int i = 0; i < stored.getBlobSize(); i++) {
                        System.out.print((char) stored.body[i]);
                    }
                    printLine();
                }
                break;
            case DELETE:
                session.delete(DigestUtils.md5(args[Argument.KEY.ordinal()].replace("\"", "").getBytes()));
                break;
            case HELP:
                usageInteractiveMode();
                break;
            case DESCRIBE:
                REST.Cluster clusterDescriptor = cluster.getMetadata().getDescriptor();
                clusterDescriptor.setNodes(null);
                printLine(JsonUtils.encodeFormatted(clusterDescriptor));
                break;
            default:
                throw new IllegalArgumentException("Method " + args[Argument.COMMAND.ordinal()] + " is not allowed");
        }
        elapsedTime(startTime, args[Argument.COMMAND.ordinal()]);
    }

    /**
     * Print elapsed time. Print 2 fraction digits if eta is under 10 ms.
     *
     * @param startTime starting time in nanoseconds
     */
    private void elapsedTime(long startTime, String commandName) {
        long eta = System.nanoTime() - startTime;
        double durationMs = eta < 10000000 ? Math.round(eta / 10000.0) / 100.0 : Math.round(eta / 1000000.0);
        printLine("Elapsed time: %s ms", durationMs);

    }

    private String readUserInput() {
        String prompt = session != null ? String.format("[%s] ", cluster.getName()) : String.format("[disconnected] ");
        return readLine(prompt);
    }

    private String readLine(String message) {
        System.out.print(message);
        return console.nextLine();
    }

    private Consistency.Level getConsistency(String consistency) {

        switch (consistency.toLowerCase()) {
            case ONE:
                return Consistency.Level.ONE;
            case TWO:
                return Consistency.Level.TWO;
            case THREE:
                return Consistency.Level.THREE;
            case QUORUM:
                return Consistency.Level.QUORUM;
            case ALL:
                return Consistency.Level.ALL;
            default:
                printLine(String.format("There is no consistency %s. Default consistency is used.", consistency));
                return QueryOpts.DEFAULT_CONSISTENCY;
        }
    }
}
