import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.WalSegmentArchivedEvent;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;

/**
 * Created by dpavlov on 06.10.2017
 */
public class WalFillerExample {
    public static void main(String[] args) {
        final String path = new File("./persistent_store").getAbsolutePath();
        System.out.println("Use following as work:" + path);
        final IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setConsistentId("127_0_0_1_47500");
        cfg.setWorkDirectory(path);

        setupDisco(cfg);

        final PersistentStoreConfiguration pstCfg = new PersistentStoreConfiguration();
        pstCfg.setWalSegmentSize(1024 * 1024);
        pstCfg.setWalSegments(2); // for faster archive
        pstCfg.setWalMode(WALMode.BACKGROUND);
        cfg.setPersistentStoreConfiguration(pstCfg);

        cfg.setIncludeEventTypes(EventType.EVT_WAL_SEGMENT_ARCHIVED);

        final Ignite ignite = Ignition.start(cfg);
        try {
            ignite.active(true);

            genereareLoad(ignite);

            ignite.active(false);
        }
        finally {
            Ignition.stop(ignite.name(), false);
        }
    }

    private static void genereareLoad(Ignite ignite) {
        final AtomicInteger segmentsInArchive = new AtomicInteger();
        ignite.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event e) {
                WalSegmentArchivedEvent archComplEvt = (WalSegmentArchivedEvent)e;
                long idx = archComplEvt.getAbsWalSegmentIdx();
                System.out.println("Finished archive for segment [" + idx + ", " +
                    archComplEvt.getArchiveFile() + "]: [" + e + "]");

                segmentsInArchive.incrementAndGet();
                return true;
            }
        }, EVT_WAL_SEGMENT_ARCHIVED);

        final CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>();

        cacheCfg.setName("my-cache1");
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        final IgniteCache<Object, Object> cache = ignite.getOrCreateCache(cacheCfg);

        A.ensure(ignite.events().isEnabled(EVT_WAL_SEGMENT_ARCHIVED), "Event not enabled");

        final ThreadLocalRandom tlr = ThreadLocalRandom.current();
        final int requiredSegments = 1;
        while (segmentsInArchive.get() < requiredSegments) {
            try (final Transaction transaction = ignite.transactions().txStart()) {
                for (int i = 0; i < 100; i++) {
                    final int nextInt = tlr.nextInt();
                    final String key = "Key" + nextInt;

                    cache.put(key, "Value");

                    if (nextInt % 3 == 0)
                        cache.put(key, "Value for divisible by 3");

                    if (nextInt % 5 == 0)
                        cache.remove(key);
                }

                transaction.commit();
            }
        }
    }

    /**
     * Setups discovery to avoid discovering other clusters
     *
     * @param cfg ignite configuration to setup
     */
    private static void setupDisco(IgniteConfiguration cfg) {
        final TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();

        tcpDiscoverySpi.setLocalPortRange(1);
        tcpDiscoverySpi.setLocalPort(11111);

        final TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();

        finder.setAddresses(Collections.singletonList("127.0.0.1:11111"));
        tcpDiscoverySpi.setIpFinder(finder);
        cfg.setDiscoverySpi(tcpDiscoverySpi);
    }
}
