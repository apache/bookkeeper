/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.tools.cli.commands.bookie;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.CommaParameterSplitter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.InterleavedStorageRegenerateIndexOp;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to regenerate an index file for interleaved storage.
 */
public class RegenerateInterleavedStorageIndexFileCommand
    extends BookieCommand<RegenerateInterleavedStorageIndexFileCommand.RISIFFlags> {

    static final Logger LOG = LoggerFactory.getLogger(RegenerateInterleavedStorageIndexFileCommand.class);

    private static final String NAME = "regenerate-interleaved-storage-index-file";
    private static final String DESC =
        "Regenerate an interleaved storage index file, from available entrylogger " + "files.";
    private static final String DEFAULT = "";

    public RegenerateInterleavedStorageIndexFileCommand() {
        this(new RISIFFlags());
    }

    private RegenerateInterleavedStorageIndexFileCommand(RISIFFlags flags) {
        super(CliSpec.<RISIFFlags>newBuilder()
                  .withName(NAME)
                  .withDescription(DESC)
                  .withFlags(flags)
                  .build());
    }

    /**
     * Flags for regenerate interleaved storage index file command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class RISIFFlags extends CliFlags {

        @Parameter(names = { "-p", "--password" },
            description = "The bookie stores the password in the index file, so we need it to regenerate."
                          + "This must match the value in the ledger metadata.")
        private String password = DEFAULT;

        @Parameter(names = { "-b", "--b64password" },
            description = "The password in base64 encoding, for cases where the password is not UTF-8.")
        private String b64Password = DEFAULT;

        @Parameter(names = { "-d", "--dryrun" }, description = "Process the entryLogger, but don't write anthing.")
        private boolean dryRun;

        @Parameter(names = { "-l", "--ledgerids" },
            description = "Ledger(s) whose index needs to be regenerated. Multiple can be specified, comma separated.",
            splitter = CommaParameterSplitter.class)
        private List<Long> ledgerIds;

    }

    @Override
    public boolean apply(ServerConfiguration conf, RISIFFlags cmdFlags) {
        try {
            return generate(conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean generate(ServerConfiguration conf, RISIFFlags flags) throws NoSuchAlgorithmException, IOException {
        validateFlags(flags);
        byte[] password;
        if (!flags.password.equals(DEFAULT)) {
            password = flags.password.getBytes(StandardCharsets.UTF_8);
        } else if (!flags.b64Password.equals(DEFAULT)) {
            password = Base64.getDecoder().decode(flags.b64Password);
        } else {
            LOG.error("The password must be specified to regenerate the index file");
            return false;
        }

        Set<Long> ledgerIds = flags.ledgerIds.stream().collect(Collectors.toSet());

        LOG.info("=== Rebuilding index file for {} ===", ledgerIds);
        ServerConfiguration serverConfiguration = new ServerConfiguration(conf);
        InterleavedStorageRegenerateIndexOp i = new InterleavedStorageRegenerateIndexOp(serverConfiguration, ledgerIds,
                                                                                        password);
        i.initiate(flags.dryRun);

        LOG.info("-- Done rebuilding index file for {} --", ledgerIds);
        return true;
    }

    private void validateFlags(RISIFFlags flags) {
        if (flags.password == null) {
            flags.password = DEFAULT;
        }
        if (flags.b64Password == null) {
            flags.b64Password = DEFAULT;
        }
    }
}
