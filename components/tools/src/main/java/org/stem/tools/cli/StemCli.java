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

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.commons.cli.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.stem.api.REST;
import org.stem.client.*;
import org.stem.utils.JsonUtils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static org.stem.tools.cli.Utils.newOpt;
import static org.stem.tools.cli.Utils.printLine;

public class StemCli {

    private static final int MAX_FILE_SIZE = 100; //Max size of file is 100MB.
    private static final int MIN_QUANTITY_ARGS = 2; //Min quantity of args in commands from file.
    private static final int INFO_COMMAND_QUANTITY_ARGS = 1;
    private static final int URL = 1; //Position of URL in the arguments array.
    private static final boolean ENABLE_CALCULATION = true; //Calculation of elapsed time
    private static final boolean DISABLE_CALCULATION = false;

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
    private String[] args;

    private CommandLine cmd;
    private Mode mode;

    private StemCluster cluster;
    private Session session = null;
    private Consistency.Level consistency = QueryOpts.DEFAULT_CONSISTENCY;

    private enum Command {
        CONNECT("connect", DISABLE_CALCULATION, MIN_QUANTITY_ARGS) {
            public void execute(StemCli stemCli) {
                stemCli.connect(stemCli.args[URL]);
            }
        },
        PUT("put", ENABLE_CALCULATION, MIN_QUANTITY_ARGS + 1) {
            public void execute(StemCli stemCli) throws ParseException, IOException {
                if (stemCli.args.length <= MIN_QUANTITY_ARGS)
                    throw new ParseException("Too few arguments for put command");

                byte[] data;
                if (stemCli.mode == Mode.INTERACTIVE && !stemCli.cmd.hasOption("data") && !stemCli.cmd.hasOption("src")) {
                    if (stemCli.args[Argument.DATA.ordinal()].startsWith("--")) {
                        throw new IllegalArgumentException(String.format("Wrong argument '%s'", stemCli.args[Argument.DATA.ordinal()]));
                    }
                    data = stemCli.args[Argument.DATA.ordinal()].replace("\"", "").getBytes();
                } else if (stemCli.cmd.hasOption("data")) {
                    data = stemCli.cmd.getOptionValue("data").replace("\"", "").getBytes();
                } else {
                    data = Utils.readFromFile(stemCli.cmd.getOptionValue("src"), MAX_FILE_SIZE);
                }

                Blob blob = Blob.create(DigestUtils.md5(stemCli.args[Argument.KEY.ordinal()].replace("\"", "").getBytes()), data);
                stemCli.session.put(blob);
            }
        },
        GET("get", ENABLE_CALCULATION, MIN_QUANTITY_ARGS) {
            public void execute(StemCli stemCli) {
                Blob stored = stemCli.session.get(DigestUtils.md5(stemCli.args[Argument.KEY.ordinal()].replace("\"", "").getBytes()));

                if (stored == null)
                    return;

                if (stemCli.cmd.hasOption("dst")) {
                    try {
                        Utils.writeToFile(stored.body, stemCli.cmd.getOptionValue("dst"));
                    } catch (IOException ioe) {
                        printLine(ioe.getMessage());
                    }
                } else {
                    for (int i = 0; i < stored.getBlobSize(); i++) {
                        System.out.print((char) stored.body[i]);
                    }
                    printLine();
                }
            }
        },
        DELETE("delete", ENABLE_CALCULATION, MIN_QUANTITY_ARGS) {
            public void execute(StemCli stemCli) {
                stemCli.session.delete(DigestUtils.md5(stemCli.args[Argument.KEY.ordinal()].replace("\"", "").getBytes()));
            }
        },
        DESCRIBE("describe", DISABLE_CALCULATION, INFO_COMMAND_QUANTITY_ARGS) {
            public void execute(StemCli stemCli) {
                REST.Cluster clusterDescriptor = stemCli.cluster.getMetadata().getDescriptor();
                clusterDescriptor.setNodes(null);
                printLine(JsonUtils.encodeFormatted(clusterDescriptor));
            }
        },
        CONSISTENCYLEVEL("consistencylevel", DISABLE_CALCULATION, INFO_COMMAND_QUANTITY_ARGS) {
            public void execute(StemCli stemCli) {
                printLine(String.format("Current consistency level: %s", stemCli.consistency));
            }
        },
        HELP("help", DISABLE_CALCULATION, INFO_COMMAND_QUANTITY_ARGS) {
            public void execute(StemCli stemCli) {
                stemCli.usageInteractiveMode();
            }
        };

        private final String name;
        private final boolean measure;
        private int minQuantityArgs;
        private static long startTime;

        /**
         *
         * @param name This is the name of command to execute.
         * @param measure It is used to calculate elapsed time of execution.
         * @param minQuantityArgs This is the minimum quantity of arguments for current command.
         */
        Command(String name, boolean measure, int minQuantityArgs) {
            this.name = name;
            this.measure = measure;
            this.minQuantityArgs = minQuantityArgs;
        }

        private static Map<String, Command> values = new HashMap<>();

        static {
            for (Command val : Command.values()) {
                values.put(val.name, val);
            }
        }

        private static Command byName(String name) {
            startTime = System.nanoTime();
            Command command = values.get(name);

            if (null == command)
                throw new IllegalStateException("Command " + name + " is not allowed");

            startTime = System.nanoTime();
            return command;
        }

        private void getElapsedTime() {
            if (measure) {
                long eta = System.nanoTime() - startTime;
                double durationMs = eta < 10000000 ? Math.round(eta / 10000.0) / 100.0 : Math.round(eta / 1000000.0);
                printLine("Elapsed time: %s ms", durationMs);
            }
        }

        private boolean isArgsValid(int argsLength) {
            return argsLength >= minQuantityArgs;
        }

        abstract void execute(StemCli stemCli) throws ParseException, IOException;
    }

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

        try {
            cmd = parser.parse(options, args);

            consistency = readConsistencyLevel(cmd.getOptionValue("consistency"));

            if (cmd.hasOption("file")) {
                mode = Mode.BATCH;
                ensureClusterManagerUrl();
            } else if (hasArg("put") || hasArg("get") || hasArg("delete")) {
                mode = Mode.SINGLE;
                ensureClusterManagerUrl();
            } else
                mode = Mode.INTERACTIVE;
        } catch (ParseException e) {
            printLine(e.getMessage());
            usage();
            System.exit(1);
        } catch (Exception e) {
            printLine(e.getMessage());
            System.exit(1);
        }

        if (cmd.hasOption("help")) {
            usage();
            System.exit(0);
        }
    }

    private boolean hasArg(String arg) {
        return cmd.getArgList().contains(arg);
    }

    private void ensureClusterManagerUrl() {
        if (!cmd.hasOption("manager")) {
            printLine("Manager URL not set");
            System.exit(1);
        }
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
                    processing();
                } catch (Exception ex) {
                    printLine(ex.getMessage());
                }
        }
    }

    /**
     * Establish connection to cluster
     *
     * @param url
     */
    private void connect(String url) {
        this.cluster = buildCluster(url, consistency);
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
        printLine("consistencylevel - show current Consistency level.");
    }

    private void interactiveMode() {
        if (console == null) {
            printLine("There is no console.");
            System.exit(1);
        }
        printLine("Enter 'quit' to exit interactive mode");

        String inputString;

        while (true) {
            try {
                inputString = readUserInput().trim();

                if (inputString.isEmpty())
                    continue;

                if (inputString.equals("quit"))
                    return;

                parsingArgs(inputString, true);
                processing();
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

        while ((line = reader.readLine()) != null) {
            line = line.trim();
            ++lineNumber;

            if (line.isEmpty() || line.startsWith("#"))
                continue;

            try {
                parsingArgs(line, true);
              } catch (ParseException pe) {
                printLine("Error occurred in the line " + lineNumber + " of file.");
                printLine(pe.getMessage());
                continue;
            }
            try {
                processing();
            } catch (Exception ile) {
                printLine(ile.getMessage());
            }
        }
    }

    /**
     * Parsing args from lines which is received from file or console
     * @param inputString - string from file or console
     * @param interactiveMode - current mode
     * @throws ParseException
     */
    private void parsingArgs(String inputString, boolean interactiveMode) throws ParseException {
        this.args = inputString.split(" ");

        this.cmd = parser.parse(options, this.args);
        if (!interactiveMode && !this.cmd.hasOption("manager") && !this.cmd.hasOption("help"))
            throw new ParseException("There is no '--manager' option!");

//        if (!hasArg("help") && !hasArg("describe") && !hasArg("consistencylevel") && this.args.length < MIN_QUANTITY_ARGS)
//            throw new ParseException("Too few arguments");
    }

    /**
     * Processing commands
     * @throws IllegalArgumentException
     * @throws IOException
     * @throws ClientInternalError
     */
    private void processing() throws IOException, ParseException, ClientException {
        if (session == null && !hasArg("connect") && !hasArg("help")) {
            return;
        }

        Command command = Command.byName(this.args[Argument.COMMAND.ordinal()].toLowerCase());
        if (!command.isArgsValid(this.args.length))
            throw new ParseException(String.format("Too few arguments for %s command",command.name));
        command.execute(this);
        command.getElapsedTime();
    }

    /**
     * Print elapsed time. Print 2 fraction digits if eta is under 10 ms.
     *
     * @param startTime starting time in nanoseconds
     */
    private void elapsedTime(long startTime) {
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

    private Consistency.Level readConsistencyLevel(String consistency) {
        if (null == consistency || consistency.isEmpty())
            return QueryOpts.DEFAULT_CONSISTENCY;

        try {
            return Consistency.Level.valueOf(consistency.toUpperCase());
        } catch (IllegalArgumentException e) {
            return QueryOpts.DEFAULT_CONSISTENCY;
        }
    }
}
