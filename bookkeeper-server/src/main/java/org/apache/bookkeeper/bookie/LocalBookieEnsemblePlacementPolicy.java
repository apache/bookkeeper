package org.apache.bookkeeper.bookie;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Special ensemble placement policy that always return local bookie. Only works with ledgers with ensemble=1.
 */
public class LocalBookieEnsemblePlacementPolicy implements EnsemblePlacementPolicy {

    static final Logger LOG = LoggerFactory.getLogger(LocalBookieEnsemblePlacementPolicy.class);

    private BookieSocketAddress bookieAddress;

    @Override
    public EnsemblePlacementPolicy initialize(Configuration conf) {

        // Configuration will have already the bookie configuration inserted
        ServerConfiguration serverConf = new ServerConfiguration();
        serverConf.addConfiguration(conf);

        try {
            bookieAddress = Bookie.getBookieAddress(serverConf);
        } catch (UnknownHostException e) {
            LOG.warn("Unable to get bookie address", e);
            throw new RuntimeException(e);
        }
        return this;
    }

    @Override
    public void uninitalize() {
        // do nothing
    }

    @Override
    public Set<BookieSocketAddress> onClusterChanged(Set<BookieSocketAddress> writableBookies,
            Set<BookieSocketAddress> readOnlyBookies) {
        return Collections.emptySet();
    }

    @Override
    public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize,
            Set<BookieSocketAddress> excludeBookies) throws BKNotEnoughBookiesException {
        if (ensembleSize > 1) {
            throw new IllegalArgumentException("Local ensemble policy can only return 1 bookie");
        }

        return Lists.newArrayList(bookieAddress);
    }

    @Override
    public BookieSocketAddress replaceBookie(BookieSocketAddress bookieToReplace,
            Set<BookieSocketAddress> excludeBookies) throws BKNotEnoughBookiesException {
        throw new BKNotEnoughBookiesException();
    }

}
