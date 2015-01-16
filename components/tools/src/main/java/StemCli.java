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

import java.io.Console;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Exception;
import java.lang.IllegalArgumentException;
import java.lang.String;
import java.lang.System;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.*;
import org.apache.commons.codec.digest.DigestUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.stem.client.Blob;
import org.stem.client.ClientInternalError;
import org.stem.client.Session;
import org.stem.client.StemCluster;

public class StemCli {

    private static final Logger logger = LoggerFactory.getLogger(StemCli.class);
    private static final int INTERACTIVE_MODE = 1;
    private static final int MAX_FILE_SIZE = 100; //Max size of file is 100MB
    private static final int MIN_QUANTITY_ARGS = 2; //Min quantity of args in commands from file
    private static final Options options;
    private static final CommandLineParser parser = new PosixParser();
    private static Session session = null;

    static {
        options = new Options();
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
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("There are no arguments");
        }

        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        }catch(ParseException e){
            System.out.println(e.getMessage());
            usage();
            System.exit(1);
        }

        if (cmd.hasOption("help")) {
            usage();
            System.exit(0);
        }

        try {
            connect(cmd.getOptionValue("manager"));
        } catch (Exception ex) {
//            System.err.println("Connection is not established");
            System.err.println(ex.getMessage());
            System.exit(1);
        }

        if (args.length == INTERACTIVE_MODE) {
            interactiveMode();
            System.exit(0);
        } else if (cmd.hasOption("file")) {
            try {
                parsingFile(cmd.getOptionValue("file"));
            } catch (FileNotFoundException fne) {
                System.err.println(fne.getMessage());
            }catch (IOException ioe) {
                System.err.println(ioe.getMessage());
            }
        } else {
            try {
                processing(cmd, args);
            } catch (IllegalArgumentException ile) {
                System.err.println(ile.getMessage());
            } catch (IOException ioe) {
                System.err.println(ioe.getMessage());
            } catch (Exception ex) {
                System.err.println(ex.getMessage());
            }
        }

        System.exit(0);
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
        new HelpFormatter().printHelp("stem-cli [<COMMAND>] [<KEY>] [--data <DATA>] [--dst <FILE>] [--src <FILE>] [--file <FILE>] --manager=<URL>",options);
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
        CommandLine cmd = null;
        
        while (true) {
            try
            {
                inputString = input.readLine("> ");
            }
            catch (IOError e)
            {
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
            } catch (IllegalArgumentException ile) {
                System.err.println(ile.getMessage());
            } catch (IOException ioe) {
                System.err.println(ioe.getMessage());
            } catch (Exception ex) {
                System.err.println(ex.getMessage());
            }
        }
    }

    /**
     * Parsing commands from file line-by-line like in interaction mode
     * @param fileName
     * @throws IOException
     * @throws FileNotFoundException
     */
    private static void parsingFile(String fileName) throws IOException, FileNotFoundException {
        BufferedReader reader;
        reader = new BufferedReader(new FileReader(fileName));
        String line = "";
        int lineNumber = 0;
        CommandLine cmd = null;
        String[] args;

        while((line = reader.readLine()) != null) {
            line.trim();
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
            } catch (IllegalArgumentException ile) {
                System.err.println(ile.getMessage());
                continue;
            } catch (ClientInternalError cie) {
                System.err.println(cie.getMessage());
            } catch (IOException ioe) {
                System.err.println(ioe.getMessage());
                continue;
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
     * @param cmd
     * @param inputArgs
     * @throws IllegalArgumentException
     * @throws IOException
     * @throws ClientInternalError
     */
    private static void processing(CommandLine cmd, String[] inputArgs) throws IllegalArgumentException, IOException, ClientInternalError {
        long startTime = System.nanoTime();
        if (inputArgs[1].startsWith("--")) {
            throw new IllegalArgumentException(String.format("Unknown argument '%s'", inputArgs[1]));
        }
        byte[] data;
        if (inputArgs[0].equals("put")) {
            if (cmd.hasOption("data")) {
                data = cmd.getOptionValue("data").getBytes();
            } else {
                data = dataFromFile(cmd.getOptionValue("src"));
            }
            Blob blob = Blob.create(DigestUtils.md5(inputArgs[1].replace("\"","").getBytes()), data);
            session.put(blob);
        } else if (inputArgs[0].equals("get")) {
            Blob stored = session.get(DigestUtils.md5(inputArgs[1].replace("\"", "").getBytes()));
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
        } else if (inputArgs[0].equals("delete")) {
            session.delete(DigestUtils.md5(inputArgs[1].replace("\"", "").getBytes()));
        } else {
            throw new IllegalArgumentException("Method " + inputArgs[0].toString() + " is not allowed");
        }
        elapsedTime(startTime, inputArgs[0]);
    }

    /**
     * Get data from file
     * @param fileName
     * @return
     * @throws IOException
     */
    private static byte[] dataFromFile(String fileName) throws IOException {
        FileInputStream fis = null;
        int i = 0;
        Path filePath = Paths.get(fileName);
        if (!Files.exists(filePath) && !Files.isRegularFile(filePath)) {
            throw new FileNotFoundException("There is no file or it is not regular file.");
        }

        if (Files.size(filePath) > MAX_FILE_SIZE ) {
            throw new IOException("File is too big");
        }
        byte[] blob = new byte[(int) Files.size(filePath)];

        try{
            fis = new FileInputStream(fileName);
            i = fis.read(blob);
            return blob;
        } catch (IOException ex){
            throw new IOException(ex.getMessage());
        } finally {
            if (fis != null)
                fis.close();
        }

    }

    /**
     * Save data to file
     * @param blob
     * @param fileName
     * @throws IOException
     */
    private static void dataToFile(byte[] blob, String fileName) throws IOException {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(fileName);
            fos.write(blob);
            fos.flush();
        } catch (IOException ex){
            throw new IOException(ex.getMessage());
        } finally {
            if (fos != null)
                fos.close();
        }
    }

    /**
     * Print elapsed time. Print 2 fraction digits if eta is under 10 ms.
     * @param startTime starting time in nanoseconds
     */
    private static void elapsedTime(long startTime, String commandName)
    {
        long eta = System.nanoTime() - startTime;
        System.out.printf("Elapsed time for command %s: ", commandName);
        if (eta < 10000000)
        {
            System.out.print(Math.round(eta/10000.0)/100.0);
        }
        else
        {
            System.out.print(Math.round(eta/1000000.0));
        }
        System.out.println(" msec(s).");
    }
}
