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

import static org.stem.tools.cli.Utils.printLine;

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

    private enum Argument {
        COMMAND, KEY, DATA
    }

    CommandLine cmd;
    private String[] args;
    private Mode mode;
    private final Scanner console = new Scanner(System.in);
    private StemCluster cluster;

    public StemCli(String[] args) {
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
        } else if (args.length <= INTERACTIVE_MODE)
            mode = Mode.INTERACTIVE;
        else if (cmd.hasOption("file"))
            mode = Mode.BATCH;
        else
            mode = Mode.SINGLE;
    }

    private void run() {
        if (cmd != null && cmd.hasOption("manager")) {
            try {
                connect(cmd.getOptionValue("manager"));
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
     */
    private void connect(String url) {
        this.cluster = new StemCluster.Builder()
                .withClusterManagerUrl(url)
                .build();

        session = cluster.connect();

        printLine(String.format("Connected to \"%s\" on %s", cluster.getName(), cmd.getOptionValue("manager")));
    }

    private static void usage() {
        printLine("Usage (batch mode):");
        new HelpFormatter().printHelp("stem-cli [<COMMAND>] [<KEY>] [--data <DATA>] [--dst <FILE>] [--src <FILE>] [--file <FILE>] [--manager=<URL>]", options);
        printLine();
        printLine("Usage (interactive mode):");
        new HelpFormatter().printHelp("<COMMAND> <KEY> [--data <DATA>] [--dst <FILE>] [--src <FILE>] [--file <FILE>]", options);
    }

    private void usageInteractiveMode() {
        printLine();
        printLine("Usage commands: ");
        printLine("connect <URL> - Connect to cluster");
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
            } catch (ClientException ce) {
                printLine(ce.getMessage());
            } catch (Exception ile) {
                printLine(ile.getMessage());
            }
        }
    }

    private CommandLine parsingArgs(String inputString, boolean interactiveMode) throws ParseException {
        String[] inputArgs = inputString.split(" ");

        CommandLine cmd = parser.parse(options, inputArgs);
        if (!interactiveMode && !cmd.hasOption("manager") && !cmd.hasOption("help"))
            throw new ParseException("There is no '--manager' option!");

        if (inputArgs.length == 0 && !inputArgs[Argument.COMMAND.ordinal()].equals("help") && inputArgs.length < MIN_QUANTITY_ARGS)
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
            case "connect":
                try {
                    connect(args[1]);
                } catch (Exception ex) {
                    printLine(ex.getMessage());
                }
                break;
            case "put":
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
            case "get":
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
            case "delete":
                session.delete(DigestUtils.md5(args[Argument.KEY.ordinal()].replace("\"", "").getBytes()));
                break;
            case "help":
                usageInteractiveMode();
                break;
            case "describe":
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
        printLine(String.format("Elapsed time for command %s: ", commandName));
        if (eta < 10000000) {
            System.out.print(Math.round(eta / 10000.0) / 100.0);
        } else {
            System.out.print(Math.round(eta / 1000000.0));
        }
        printLine(" ms");
    }

    private String readUserInput() {
        String promt = null;
        if (session != null) {
            promt = String.format("[%s] ", cluster.getName());
        } else {
            promt = String.format("[disconnected] ");
        }
        return readLine(promt);
    }

    private String readLine(String message) {
        System.out.print(message);
        return console.nextLine();
    }
}
