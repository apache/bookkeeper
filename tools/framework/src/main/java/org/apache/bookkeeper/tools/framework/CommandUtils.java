/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.tools.framework;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.WrappedParameter;
import com.google.common.collect.Lists;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;
import org.apache.commons.lang.StringUtils;

/**
 * Utils to process a commander.
 */
public class CommandUtils {

    private static final int MAX_COLUMN_SIZE = 79;
    private static final int DEFAULT_INDENT = 4;
    private static final String USAGE_HEADER = "Usage:";

    private static final Comparator<? super ParameterDescription> PD_COMPARATOR =
        (Comparator<ParameterDescription>) (p0, p1) -> p0.getLongestName().compareTo(p1.getLongestName());

    private static void printIndent(PrintStream printer, int indent) {
        IntStream.range(0, indent).forEach(ignored -> printer.print(" "));
    }

    public static void printUsage(PrintStream printer, String usage) {
        final int indent = ((USAGE_HEADER.length() / DEFAULT_INDENT) + 1) * DEFAULT_INDENT;
        final int firstIndent = indent - USAGE_HEADER.length();
        printer.print(USAGE_HEADER);
        printDescription(
            printer,
            firstIndent,
            indent,
            usage);
        printer.println();
    }

    /**
     * Print the available flags in <tt>commander</tt>.
     *
     * @param commander commander
     * @param printer printer
     */
    public static void printAvailableFlags(JCommander commander, PrintStream printer) {
        List<ParameterDescription> sorted = Lists.newArrayList();
        List<ParameterDescription> pds = commander.getParameters();

        // Align the descriptions at the `longestName`
        int longestName = 0;
        for (ParameterDescription pd : pds) {
           if (pd.getParameter().hidden()) {
               continue;
           }

           sorted.add(pd);
           int length = pd.getNames().length() + 2;
           if (length > longestName) {
               longestName = length;
           }
        }

        if (sorted.isEmpty()) {
            return;
        }

        // Sorted the flags
        Collections.sort(sorted, PD_COMPARATOR);

        // Display the flags
        printer.println("Flags:");
        printer.println();

        ParameterDescription helpPd = null;

        for (ParameterDescription pd : sorted) {
            if ("--help".equals(pd.getLongestName())) {
                helpPd = pd;
                continue;
            }

            printFlag(pd, DEFAULT_INDENT, printer);
            printer.println();
        }

        if (null != helpPd) {
            printer.println();
            printFlag(helpPd, DEFAULT_INDENT, printer);
            printer.println();
        }

    }

    private static void printFlag(ParameterDescription pd, int indent, PrintStream printer) {
        WrappedParameter parameter = pd.getParameter();
        // print flag
        printIndent(printer, indent);
        printer.print(pd.getNames());
        printer.print(parameter.required() ? " (*)" : "");
        printer.println();
        // print flag description
        int descIndent = 2 * indent;
        printDescription(printer, descIndent, descIndent, pd.getDescription());
    }

    public static void printDescription(PrintStream printer,
                                        int firstLineIndent,
                                        int indent,
                                        String description) {
        int max = MAX_COLUMN_SIZE;
        String[] words = description.split(" ");
        int current = indent;
        int i = 0;
        printIndent(printer, firstLineIndent);
        while (i < words.length) {
            String word = words[i];
            if (word.length() > max || current + word.length() <= max) {
                if (i != 0) {
                    printer.print(" ");
                }
                printer.print(word);
                current += (word.length() + 1);
            } else {
                printer.println();
                printIndent(printer, indent);
                printer.print(word);
                current = indent;
            }
            i++;
        }
        printer.println();
    }

    /**
     * Print the available commands in <tt>commander</tt>.
     *
     * @param commands commands
     * @param printer printer
     */
    public static void printAvailableCommands(Map<String, Command> commands,
                                              PrintStream printer) {
        if (commands.isEmpty()) {
            return;
        }

        int longestCommandName = commands
            .keySet()
            .stream()
            .mapToInt(name -> name.length())
            .max()
            .orElse(0);

        // group the commands by category
        Map<String, Map<String, Command>> categorizedCommands = new TreeMap<>();
        for (Map.Entry<String, Command> commandEntry : commands.entrySet()) {
            if ("help".equals(commandEntry.getKey())) {
                // don't add help message along with other commands
                continue;
            }
            String category = commandEntry.getValue().category();
            Map<String, Command> subCommands = categorizedCommands.get(category);
            if (null == subCommands) {
                subCommands = new TreeMap<>();
                categorizedCommands.put(category, subCommands);
            }
            subCommands.put(commandEntry.getKey(), commandEntry.getValue());
        }

        // there is only one category, print all of them under `Commands`.
        if (categorizedCommands.size() <= 1) {
            printer.println("Commands:");
            printer.println();
        }

        for (Map.Entry<String, Map<String, Command>> categoryEntry : categorizedCommands.entrySet()) {
            String category = categoryEntry.getKey();
            printCategoryHeader(printer, category);
            Map<String, Command> subCommands = categoryEntry.getValue();
            for (Map.Entry<String, Command> commandEntry : subCommands.entrySet()) {
                printCommand(printer, commandEntry.getKey(), commandEntry.getValue(), longestCommandName);
            }
            printer.println();
        }

        Command helpCmd = commands.get("help");
        if (null != helpCmd) {
            if (categorizedCommands.size() > 1) {
                // if commands has been categorized, put help
                printCategoryHeader(printer, "Other commands");
            }
            printCommand(printer, "help", helpCmd, longestCommandName);
        }

        printer.println();
    }

    private static void printCommand(PrintStream printer,
                                     String name,
                                     Command command,
                                     final int longestCommandName) {
        if (command.hidden()) {
            return;
        }

        final int indent = DEFAULT_INDENT;
        final int startOfDescription =
            (((indent + longestCommandName) / DEFAULT_INDENT) + 2) * DEFAULT_INDENT;

        int current = 0;
        printIndent(printer, indent);
        printer.print(name);
        current += (indent + name.length());
        printIndent(printer, startOfDescription - current);
        printDescription(
            printer,
            0,
            startOfDescription,
            command.description());
    }

    private static void printCategoryHeader(PrintStream printer,
                                            String category) {
        if (StringUtils.isEmpty(category)) {
            return;
        }

        printIndent(printer, 0);
        printDescription(
            printer,
            0,
            0,
            category + " :");
        printer.println();
    }

}
