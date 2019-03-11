package org.apache.bookkeeper.tools.cli.commands.bookie;

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * A command to initialize new bookie.
 */
public class InitCommand extends BookieCommand<CliFlags> {

    private static final String NAME = "init";
    private static final String DESC = "Initialize new bookie.";

    public InitCommand() {
        super(CliSpec.newBuilder()
                .withName(NAME)
                .withDescription(DESC)
                .withFlags(new CliFlags())
                .build());
    }

    @Override
    public boolean apply(ServerConfiguration conf, CliFlags cmdFlags) {

        boolean result = false;
        try {
            result = BookKeeperAdmin.initBookie(conf);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
        return result;
    }
}
