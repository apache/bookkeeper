package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.stats.StatsLogger;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MockBookieWatcher extends BookieWatcherImpl {

    public MockBookieWatcher(ClientConfiguration conf, EnsemblePlacementPolicy placementPolicy, RegistrationClient registrationClient, BookieAddressResolver bookieAddressResolver, StatsLogger statsLogger) {
        super(conf, placementPolicy, registrationClient, bookieAddressResolver, statsLogger);
    }

    @Override
    public BookieId replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize, Map<String, byte[]> customMetadata, List<BookieId> existingBookies, int bookieIdx, Set<BookieId> excludeBookies) throws BKException.BKNotEnoughBookiesException {
        throw new BKException.BKNotEnoughBookiesException();
    }
}
