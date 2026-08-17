package redigo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.redisson.Redisson;
import org.redisson.api.RAtomicDouble;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBinaryStream;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RBoundedBlockingQueue;
import org.redisson.api.RBucket;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RCountDownLatch;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RLexSortedSet;
import org.redisson.api.RList;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RMapCache;
import org.redisson.api.RMapCacheNative;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RRingBuffer;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RSemaphore;
import org.redisson.api.RSetMultimapCacheNative;
import org.redisson.api.RListMultimapCacheNative;
import org.redisson.api.RSetMultimapCache;
import org.redisson.api.RListMultimapCache;
import org.redisson.api.RSetCache;
import org.redisson.api.RateIntervalUnit;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * REPL probe: reads space-separated commands from stdin, one JSON reply per
 * line on stdout. Runs a single JVM so Go tests avoid per-command startup.
 *
 * Redisson 4.6.1 with JsonJacksonCodec — the exact configuration redi.go's
 * wire format targets. Values passed as command args are JSON literals.
 */
public final class RedigoProbe {

    private static RedissonClient rs;
    private static final ObjectMapper OM = new ObjectMapper();
    private static RLock heldLock;
    private static RLock heldFair;
    private static RLock heldSpin;
    private static RLock heldNonReentrant;
    private static RLock heldNonReentrantFair;
    private static RReadWriteLock heldRwLock;
    private static org.redisson.api.RFencedLock heldFenced;
    private static String heldPermit;
    private static org.redisson.api.RLongAdder heldAdder;
    private static final java.util.concurrent.BlockingQueue<
            java.util.concurrent.BlockingQueue<Object>> reliableMsgs =
            new java.util.concurrent.LinkedBlockingQueue<>();

    public static void main(String[] args) throws Exception {
        org.redisson.config.Config cfg = new org.redisson.config.Config();
        String host = System.getenv().getOrDefault("REDIS_HOST", "localhost");
        String port = System.getenv().getOrDefault("REDIS_PORT", "6379");
        cfg.useSingleServer().setAddress("redis://" + host + ":" + port);
        cfg.setCodec(new JsonJacksonCodec());
        rs = Redisson.create(cfg);

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            String[] tok = line.split(" ");
            if (tok[0].equals("exit")) {
                break;
            }
            try {
                handle(tok);
            } catch (Exception e) {
                Map<String, Object> err = new HashMap<>();
                err.put("error", String.valueOf(e));
                reply(err);
            }
        }
        if (heldLock != null && heldLock.isHeldByCurrentThread()) {
            heldLock.unlock();
        }
        if (heldFair != null && heldFair.isHeldByCurrentThread()) {
            heldFair.unlock();
        }
        if (heldRwLock != null && heldRwLock.writeLock().isHeldByCurrentThread()) {
            heldRwLock.writeLock().unlock();
        }
        if (heldAdder != null) {
            heldAdder.destroy();
        }
        rs.shutdown();
    }

    private static void handle(String[] a) throws Exception {
        switch (a[0]) {
            case "ping" -> reply(map("ok", true));

            case "lock_acquire" -> {
                RLock l = rs.getLock(a[1]);
                boolean acq = l.tryLock(0, 60000, TimeUnit.MILLISECONDS);
                if (acq) {
                    l.unlock();
                }
                reply(map("acquired", acq));
            }
            case "lock_hold" -> {
                RLock l = rs.getLock(a[1]);
                boolean acq = l.tryLock(0, 60000, TimeUnit.MILLISECONDS);
                heldLock = acq ? l : null;
                reply(map("acquired", acq));
            }
            case "lock_release" -> {
                if (heldLock != null && heldLock.isHeldByCurrentThread()) {
                    heldLock.unlock();
                }
                heldLock = null;
                reply(map("released", true));
            }

            case "fair_try" -> {
                RLock l = rs.getFairLock(a[1]);
                boolean acq = l.tryLock(0, 30000, TimeUnit.MILLISECONDS);
                if (acq) {
                    l.unlock();
                }
                reply(map("acquired", acq));
            }
            case "fair_hold" -> {
                RLock l = rs.getFairLock(a[1]);
                boolean acq = l.tryLock(0, 30000, TimeUnit.MILLISECONDS);
                heldFair = acq ? l : null;
                reply(map("acquired", acq));
            }
            case "fair_unlock" -> {
                if (heldFair != null && heldFair.isHeldByCurrentThread()) {
                    heldFair.unlock();
                }
                heldFair = null;
                reply(map("ok", true));
            }
            case "fair_held" -> {
                RLock l = rs.getFairLock(a[1]);
                reply(map("held", l.isHeldByCurrentThread()));
            }

            case "fenced_try" -> {
                org.redisson.api.RFencedLock fl = rs.getFencedLock(a[1]);
                Long token = fl.tryLockAndGetToken(0, 60000, TimeUnit.MILLISECONDS);
                if (token != null) {
                    heldFenced = fl;
                }
                reply(map("token", token));
            }
            case "fenced_token" -> {
                org.redisson.api.RFencedLock fl = rs.getFencedLock(a[1]);
                reply(map("token", fl.getToken()));
            }
            case "fenced_release" -> {
                if (heldFenced != null && heldFenced.isHeldByCurrentThread()) {
                    // Drain re-entrancy so the key is fully released.
                    while (heldFenced.isHeldByCurrentThread()) {
                        heldFenced.unlock();
                    }
                }
                heldFenced = null;
                reply(map("ok", true));
            }

            case "map_put" -> {
                RMap<Object, Object> m = rs.getMap(a[1]);
                m.put(OM.readValue(a[2], Object.class), OM.readValue(a[3], Object.class));
                reply(map("ok", true));
            }
            case "map_get" -> {
                RMap<Object, Object> m = rs.getMap(a[1]);
                reply(map("value", m.get(OM.readValue(a[2], Object.class))));
            }

            case "rw_write_hold" -> {
                RReadWriteLock rw = rs.getReadWriteLock(a[1]);
                boolean acq = rw.writeLock().tryLock(0, 60000, TimeUnit.MILLISECONDS);
                heldRwLock = acq ? rw : null;
                reply(map("acquired", acq));
            }
            case "rw_read_try" -> {
                RReadWriteLock rw = rs.getReadWriteLock(a[1]);
                reply(map("acquired", rw.readLock().tryLock(0, 60000, TimeUnit.MILLISECONDS)));
            }
            case "rw_write_try" -> {
                RReadWriteLock rw = rs.getReadWriteLock(a[1]);
                reply(map("acquired", rw.writeLock().tryLock(0, 60000, TimeUnit.MILLISECONDS)));
            }
            case "rw_release" -> {
                if (heldRwLock != null && heldRwLock.writeLock().isHeldByCurrentThread()) {
                    heldRwLock.writeLock().unlock();
                }
                heldRwLock = null;
                reply(map("released", true));
            }

            case "list_add" -> {
                RList<Object> l = rs.getList(a[1]);
                reply(map("ok", l.add(OM.readValue(a[2], Object.class))));
            }
            case "list_get" -> {
                RList<Object> l = rs.getList(a[1]);
                reply(map("value", l.get(Integer.parseInt(a[2]))));
            }
            case "list_size" -> {
                RList<Object> l = rs.getList(a[1]);
                reply(map("size", l.size()));
            }

            case "zset_add" -> {
                RScoredSortedSet<Object> z = rs.getScoredSortedSet(a[1]);
                reply(map("ok", z.add(Double.parseDouble(a[2]), OM.readValue(a[3], Object.class))));
            }
            case "zset_score" -> {
                RScoredSortedSet<Object> z = rs.getScoredSortedSet(a[1]);
                reply(map("value", z.getScore(OM.readValue(a[2], Object.class))));
            }
            case "zset_rank" -> {
                RScoredSortedSet<Object> z = rs.getScoredSortedSet(a[1]);
                reply(map("value", z.rank(OM.readValue(a[2], Object.class))));
            }

            case "lex_add" -> {
                RLexSortedSet s = rs.getLexSortedSet(a[1]);
                reply(map("ok", s.add(a[2])));
            }
            case "lex_range" -> {
                RLexSortedSet s = rs.getLexSortedSet(a[1]);
                reply(map("values", new ArrayList<>(s.range(a[2], Boolean.parseBoolean(a[3]),
                        a[4], Boolean.parseBoolean(a[5])))));
            }
            case "lex_first" -> {
                RLexSortedSet s = rs.getLexSortedSet(a[1]);
                reply(map("value", s.first()));
            }

            case "bucket_set" -> {
                RBucket<Object> b = rs.getBucket(a[1]);
                b.set(OM.readValue(a[2], Object.class));
                reply(map("ok", true));
            }
            case "bucket_get" -> {
                RBucket<Object> b = rs.getBucket(a[1]);
                reply(map("value", b.get()));
            }
            case "binary_set" -> {
                RBinaryStream s = rs.getBinaryStream(a[1]);
                byte[] value = Base64.getDecoder().decode(a[2]);
                s.set(value);
                reply(map("size", s.size()));
            }
            case "binary_get" -> {
                RBinaryStream s = rs.getBinaryStream(a[1]);
                byte[] value = s.get();
                reply(map("value", value == null ? null : Base64.getEncoder().encodeToString(value)));
            }
            case "binary_channel_write" -> {
                RBinaryStream s = rs.getBinaryStream(a[1]);
                var channel = s.getChannel();
                channel.position(Long.parseLong(a[2]));
                int written = channel.write(ByteBuffer.wrap(Base64.getDecoder().decode(a[3])));
                reply(map("written", written));
            }

            case "mapcache_put" -> {
                RMapCache<Object, Object> mc = rs.getMapCache(a[1]);
                mc.put(OM.readValue(a[2], Object.class), OM.readValue(a[3], Object.class),
                        Long.parseLong(a[4]), TimeUnit.MILLISECONDS);
                reply(map("ok", true));
            }
            case "mapcache_get" -> {
                RMapCache<Object, Object> mc = rs.getMapCache(a[1]);
                reply(map("value", mc.get(OM.readValue(a[2], Object.class))));
            }

            case "dq_offer" -> {
                RBlockingQueue<Object> target = rs.getBlockingQueue(a[1]);
                RDelayedQueue<Object> dq = rs.getDelayedQueue(target);
                dq.offer(OM.readValue(a[2], Object.class),
                        Long.parseLong(a[3]), TimeUnit.MILLISECONDS);
                reply(map("ok", true));
            }
            case "dq_peek" -> {
                RBlockingQueue<Object> target = rs.getBlockingQueue(a[1]);
                reply(map("value", target.peek()));
            }

            case "hll_add" -> {
                org.redisson.api.RHyperLogLog<Object> hll = rs.getHyperLogLog(a[1]);
                reply(map("ok", hll.add(OM.readValue(a[2], Object.class))));
            }
            case "hll_count" -> {
                org.redisson.api.RHyperLogLog<Object> hll = rs.getHyperLogLog(a[1]);
                reply(map("count", hll.count()));
            }

            case "geo_add" -> {
                org.redisson.api.RGeo<Object> geo = rs.getGeo(a[1]);
                reply(map("ok", geo.add(Double.parseDouble(a[2]), Double.parseDouble(a[3]),
                        OM.readValue(a[4], Object.class)) > 0));
            }
            case "geo_dist" -> {
                org.redisson.api.RGeo<Object> geo = rs.getGeo(a[1]);
                reply(map("value", geo.dist(OM.readValue(a[2], Object.class),
                        OM.readValue(a[3], Object.class),
                        org.redisson.api.geo.GeoUnit.valueOf(a[4]))));
            }
            case "geo_pos" -> {
                org.redisson.api.RGeo<Object> geo = rs.getGeo(a[1]);
                org.redisson.api.geo.GeoPosition p =
                        geo.pos(OM.readValue(a[2], Object.class)).values().iterator().next();
                Map<String, Object> out = new HashMap<>();
                out.put("lon", p.getLongitude());
                out.put("lat", p.getLatitude());
                reply(out);
            }

            case "bitset_set" -> {
                org.redisson.api.RBitSet bs = rs.getBitSet(a[1]);
                bs.set(Long.parseLong(a[2]));
                reply(map("ok", true));
            }
            case "bitset_get" -> {
                org.redisson.api.RBitSet bs = rs.getBitSet(a[1]);
                reply(map("value", bs.get(Long.parseLong(a[2]))));
            }
            case "bitset_cardinality" -> {
                org.redisson.api.RBitSet bs = rs.getBitSet(a[1]);
                reply(map("value", bs.cardinality()));
            }
            case "bitset_length" -> {
                org.redisson.api.RBitSet bs = rs.getBitSet(a[1]);
                reply(map("value", bs.length()));
            }

            case "stream_add" -> {
                org.redisson.api.RStream<Object, Object> st = rs.getStream(a[1]);
                org.redisson.api.stream.StreamMessageId id =
                        st.add(org.redisson.api.stream.StreamAddArgs.entry(
                                OM.readValue(a[2], Object.class),
                                OM.readValue(a[3], Object.class)));
                reply(map("id", id.toString()));
            }
            case "stream_create_group" -> {
                org.redisson.api.RStream<Object, Object> st = rs.getStream(a[1]);
                st.createGroup(org.redisson.api.stream.StreamCreateGroupArgs
                        .name(a[2]).id(org.redisson.api.stream.StreamMessageId.ALL));
                reply(map("ok", true));
            }
            case "stream_read_group" -> {
                org.redisson.api.RStream<Object, Object> st = rs.getStream(a[1]);
                Map<org.redisson.api.stream.StreamMessageId, Map<Object, Object>> res =
                        st.readGroup(a[2], a[3],
                                org.redisson.api.stream.StreamReadGroupArgs
                                        .neverDelivered()
                                        .count(Integer.parseInt(a[4])));
                Map<String, Object> out = new HashMap<>();
                for (Map.Entry<org.redisson.api.stream.StreamMessageId, Map<Object, Object>> e : res.entrySet()) {
                    StringBuilder sb = new StringBuilder();
                    for (Map.Entry<Object, Object> f : e.getValue().entrySet()) {
                        sb.append(String.valueOf(f.getKey())).append("=")
                          .append(String.valueOf(f.getValue()));
                    }
                    out.put(e.getKey().toString(), sb.toString());
                }
                reply(map("entries", out));
            }
            case "stream_ack" -> {
                org.redisson.api.RStream<Object, Object> st = rs.getStream(a[1]);
                String[] parts = a[3].split("[-]");
                reply(map("value", st.ack(a[2],
                        new org.redisson.api.stream.StreamMessageId(
                                Long.parseLong(parts[0]), Long.parseLong(parts[1])))));
            }

            case "pes_set" -> {
                org.redisson.api.RPermitExpirableSemaphore pes =
                        rs.getPermitExpirableSemaphore(a[1]);
                reply(map("ok", pes.trySetPermits(Integer.parseInt(a[2]))));
            }
            case "pes_acquire" -> {
                org.redisson.api.RPermitExpirableSemaphore pes =
                        rs.getPermitExpirableSemaphore(a[1]);
                String pid = null;
                try {
                    pid = pes.tryAcquire(100, 60000, java.util.concurrent.TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                heldPermit = pid;
                reply(map("permit", pid));
            }
            case "pes_release" -> {
                org.redisson.api.RPermitExpirableSemaphore pes =
                        rs.getPermitExpirableSemaphore(a[1]);
                if (heldPermit != null) {
                    pes.tryRelease(heldPermit);
                }
                heldPermit = null;
                reply(map("released", true));
            }
            case "pes_available" -> {
                org.redisson.api.RPermitExpirableSemaphore pes =
                        rs.getPermitExpirableSemaphore(a[1]);
                reply(map("value", pes.availablePermits()));
            }

            case "rtopic_publish" -> {
                org.redisson.api.RReliableTopic topic = rs.getReliableTopic(a[1]);
                reply(map("subscribers", topic.publish(OM.readValue(a[2], Object.class))));
            }
            case "rtopic_listen" -> {
                org.redisson.api.RReliableTopic topic = rs.getReliableTopic(a[1]);
                java.util.concurrent.BlockingQueue<Object> q = new java.util.concurrent.LinkedBlockingQueue<>();
                // ponytail: raw Object type - the probe only echoes messages
                topic.addListener(Object.class, (org.redisson.api.listener.MessageListener<Object>) (channel, msg) -> q.add(msg));
                reliableMsgs.offer(q);
                reply(map("ok", true));
            }
            case "rtopic_collect" -> {
                java.util.concurrent.BlockingQueue<Object> q = reliableMsgs.poll();
                if (q == null) {
                    reply(map("value", null));
                } else {
                    Object m = q.poll(4, java.util.concurrent.TimeUnit.SECONDS);
                    reply(map("value", m));
                }
            }

            case "adder_create" -> {
                heldAdder = rs.getLongAdder(a[1]);
                reply(map("ok", true));
            }
            case "adder_add" -> {
                if (heldAdder != null) {
                    heldAdder.add(Long.parseLong(a[2]));
                }
                reply(map("ok", true));
            }
            case "adder_sum" -> {
                if (heldAdder == null) {
                    reply(map("error", "no adder"));
                } else {
                    reply(map("value", heldAdder.sum()));
                }
            }

            case "ts_add" -> {
                org.redisson.api.RTimeSeries<Object, Object> ts = rs.getTimeSeries(a[1]);
                ts.add(Long.parseLong(a[2]), OM.readValue(a[3], Object.class));
                reply(map("ok", true));
            }
            case "ts_get" -> {
                org.redisson.api.RTimeSeries<Object, Object> ts = rs.getTimeSeries(a[1]);
                reply(map("value", ts.get(Long.parseLong(a[2]))));
            }
            case "ts_range_size" -> {
                org.redisson.api.RTimeSeries<Object, Object> ts = rs.getTimeSeries(a[1]);
                reply(map("value", ts.size()));
            }

            case "along_add" -> {
                RAtomicLong al = rs.getAtomicLong(a[1]);
                reply(map("value", al.addAndGet(Long.parseLong(a[2]))));
            }
            case "along_get" -> {
                RAtomicLong al = rs.getAtomicLong(a[1]);
                reply(map("value", al.get()));
            }

            case "adouble_add" -> {
                RAtomicDouble ad = rs.getAtomicDouble(a[1]);
                reply(map("value", ad.addAndGet(Double.parseDouble(a[2]))));
            }
            case "adouble_get" -> {
                RAtomicDouble ad = rs.getAtomicDouble(a[1]);
                reply(map("value", ad.get()));
            }

            case "spin_hold" -> {
                RLock l = rs.getSpinLock(a[1]);
                boolean acq = l.tryLock(0, 30000, TimeUnit.MILLISECONDS);
                heldSpin = acq ? l : null;
                reply(map("acquired", acq));
            }
            case "spin_try" -> {
                RLock l = rs.getSpinLock(a[1]);
                boolean acq = l.tryLock(0, 30000, TimeUnit.MILLISECONDS);
                if (acq) {
                    l.unlock();
                }
                reply(map("acquired", acq));
            }
            case "spin_release" -> {
                if (heldSpin != null && heldSpin.isHeldByCurrentThread()) {
                    heldSpin.unlock();
                }
                heldSpin = null;
                reply(map("ok", true));
            }

            case "nrl_hold" -> {
                RLock l = rs.getNonReentrantLock(a[1]);
                boolean acq = l.tryLock(0, 30000, TimeUnit.MILLISECONDS);
                heldNonReentrant = acq ? l : null;
                reply(map("acquired", acq));
            }
            case "nrl_try" -> {
                RLock l = rs.getNonReentrantLock(a[1]);
                boolean acq = l.tryLock(0, 30000, TimeUnit.MILLISECONDS);
                if (acq) {
                    l.unlock();
                }
                reply(map("acquired", acq));
            }
            case "nrl_release" -> {
                if (heldNonReentrant != null && heldNonReentrant.isHeldByCurrentThread()) {
                    heldNonReentrant.unlock();
                }
                heldNonReentrant = null;
                reply(map("ok", true));
            }

            case "nrf_hold" -> {
                RLock l = rs.getNonReentrantFairLock(a[1]);
                boolean acq = l.tryLock(0, 30000, TimeUnit.MILLISECONDS);
                heldNonReentrantFair = acq ? l : null;
                reply(map("acquired", acq));
            }
            case "nrf_try" -> {
                RLock l = rs.getNonReentrantFairLock(a[1]);
                boolean acq = l.tryLock(0, 30000, TimeUnit.MILLISECONDS);
                if (acq) {
                    l.unlock();
                }
                reply(map("acquired", acq));
            }
            case "nrf_release" -> {
                if (heldNonReentrantFair != null && heldNonReentrantFair.isHeldByCurrentThread()) {
                    heldNonReentrantFair.unlock();
                }
                heldNonReentrantFair = null;
                reply(map("ok", true));
            }

            case "mcn_put" -> {
                RMapCacheNative<Object, Object> m = rs.getMapCacheNative(a[1]);
                m.put(OM.readValue(a[2], Object.class), OM.readValue(a[3], Object.class),
                        java.time.Duration.ofMillis(Long.parseLong(a[4])));
                reply(map("ok", true));
            }
            case "mcn_get" -> {
                RMapCacheNative<Object, Object> m = rs.getMapCacheNative(a[1]);
                reply(map("value", m.get(OM.readValue(a[2], Object.class))));
            }

            case "smmcn_put" -> {
                RSetMultimapCacheNative<Object, Object> m = rs.getSetMultimapCacheNative(a[1]);
                reply(map("added", m.put(OM.readValue(a[2], Object.class),
                        OM.readValue(a[3], Object.class))));
            }
            case "smmcn_expire" -> {
                RSetMultimapCacheNative<Object, Object> m = rs.getSetMultimapCacheNative(a[1]);
                reply(map("ok", m.expireKey(OM.readValue(a[2], Object.class),
                        Long.parseLong(a[3]), TimeUnit.MILLISECONDS)));
            }
            case "smmcn_getall" -> {
                RSetMultimapCacheNative<Object, Object> m = rs.getSetMultimapCacheNative(a[1]);
                reply(map("values", new ArrayList<>(m.getAll(OM.readValue(a[2], Object.class)))));
            }

            case "lmmcn_put" -> {
                RListMultimapCacheNative<Object, Object> m = rs.getListMultimapCacheNative(a[1]);
                reply(map("added", m.put(OM.readValue(a[2], Object.class),
                        OM.readValue(a[3], Object.class))));
            }
            case "lmmcn_expire" -> {
                RListMultimapCacheNative<Object, Object> m = rs.getListMultimapCacheNative(a[1]);
                reply(map("ok", m.expireKey(OM.readValue(a[2], Object.class),
                        Long.parseLong(a[3]), TimeUnit.MILLISECONDS)));
            }
            case "lmmcn_getall" -> {
                RListMultimapCacheNative<Object, Object> m = rs.getListMultimapCacheNative(a[1]);
                reply(map("values", new ArrayList<>(m.getAll(OM.readValue(a[2], Object.class)))));
            }

            case "rb_capacity" -> {
                RRingBuffer<Object> rb = rs.getRingBuffer(a[1]);
                reply(map("ok", rb.trySetCapacity(Integer.parseInt(a[2]))));
            }
            case "rb_add" -> {
                RRingBuffer<Object> rb = rs.getRingBuffer(a[1]);
                reply(map("ok", rb.add(OM.readValue(a[2], Object.class))));
            }
            case "rb_poll" -> {
                RRingBuffer<Object> rb = rs.getRingBuffer(a[1]);
                reply(map("value", rb.poll()));
            }
            case "rb_size" -> {
                RRingBuffer<Object> rb = rs.getRingBuffer(a[1]);
                reply(map("size", rb.size()));
            }

            case "sc_add" -> {
                RSetCache<Object> sc = rs.getSetCache(a[1]);
                reply(map("added", sc.add(OM.readValue(a[2], Object.class),
                        Long.parseLong(a[3]), TimeUnit.MILLISECONDS)));
            }
            case "sc_contains" -> {
                RSetCache<Object> sc = rs.getSetCache(a[1]);
                reply(map("contains", sc.contains(OM.readValue(a[2], Object.class))));
            }
            case "sc_size" -> {
                RSetCache<Object> sc = rs.getSetCache(a[1]);
                reply(map("size", sc.size()));
            }

            case "smmc_put" -> {
                RSetMultimapCache<Object, Object> m = rs.getSetMultimapCache(a[1]);
                reply(map("added", m.put(OM.readValue(a[2], Object.class),
                        OM.readValue(a[3], Object.class))));
            }
            case "smmc_expire" -> {
                RSetMultimapCache<Object, Object> m = rs.getSetMultimapCache(a[1]);
                reply(map("ok", m.expireKey(OM.readValue(a[2], Object.class),
                        Long.parseLong(a[3]), TimeUnit.MILLISECONDS)));
            }
            case "smmc_getall" -> {
                RSetMultimapCache<Object, Object> m = rs.getSetMultimapCache(a[1]);
                reply(map("values", new ArrayList<>(m.getAll(OM.readValue(a[2], Object.class)))));
            }

            case "lmmc_put" -> {
                RListMultimapCache<Object, Object> m = rs.getListMultimapCache(a[1]);
                reply(map("added", m.put(OM.readValue(a[2], Object.class),
                        OM.readValue(a[3], Object.class))));
            }
            case "lmmc_expire" -> {
                RListMultimapCache<Object, Object> m = rs.getListMultimapCache(a[1]);
                reply(map("ok", m.expireKey(OM.readValue(a[2], Object.class),
                        Long.parseLong(a[3]), TimeUnit.MILLISECONDS)));
            }
            case "lmmc_getall" -> {
                RListMultimapCache<Object, Object> m = rs.getListMultimapCache(a[1]);
                reply(map("values", new ArrayList<>(m.getAll(OM.readValue(a[2], Object.class)))));
            }

            case "bbq_capacity" -> {
                RBoundedBlockingQueue<Object> q = rs.getBoundedBlockingQueue(a[1]);
                reply(map("ok", q.trySetCapacity(Integer.parseInt(a[2]))));
            }
            case "bbq_offer" -> {
                RBoundedBlockingQueue<Object> q = rs.getBoundedBlockingQueue(a[1]);
                reply(map("ok", q.offer(OM.readValue(a[2], Object.class))));
            }
            case "bbq_poll" -> {
                RBoundedBlockingQueue<Object> q = rs.getBoundedBlockingQueue(a[1]);
                reply(map("value", q.poll()));
            }
            case "bbq_size" -> {
                RBoundedBlockingQueue<Object> q = rs.getBoundedBlockingQueue(a[1]);
                reply(map("size", q.size()));
            }

            case "bloom_init" -> {
                RBloomFilter<Object> f = rs.getBloomFilter(a[1]);
                reply(map("ok", f.tryInit(Long.parseLong(a[2]), Double.parseDouble(a[3]))));
            }
            case "bloom_add" -> {
                RBloomFilter<Object> f = rs.getBloomFilter(a[1]);
                reply(map("added", f.add(OM.readValue(a[2], Object.class))));
            }
            case "bloom_contains" -> {
                RBloomFilter<Object> f = rs.getBloomFilter(a[1]);
                reply(map("contains", f.contains(OM.readValue(a[2], Object.class))));
            }

            case "rate_set" -> {
                RRateLimiter rl = rs.getRateLimiter(a[1]);
                reply(map("ok", rl.trySetRate(RateType.OVERALL,
                        Long.parseLong(a[2]), Long.parseLong(a[3]),
                        RateIntervalUnit.MILLISECONDS)));
            }
            case "rate_try" -> {
                RRateLimiter rl = rs.getRateLimiter(a[1]);
                reply(map("acquired", rl.tryAcquire(Long.parseLong(a[2]))));
            }

            case "sem_set" -> {
                RSemaphore s = rs.getSemaphore(a[1]);
                reply(map("ok", s.trySetPermits(Integer.parseInt(a[2]))));
            }
            case "sem_try" -> {
                RSemaphore s = rs.getSemaphore(a[1]);
                reply(map("acquired", s.tryAcquire(Integer.parseInt(a[2]))));
            }
            case "sem_release" -> {
                RSemaphore s = rs.getSemaphore(a[1]);
                s.release(Integer.parseInt(a[2]));
                reply(map("ok", true));
            }

            case "latch_set" -> {
                RCountDownLatch l = rs.getCountDownLatch(a[1]);
                reply(map("ok", l.trySetCount(Long.parseLong(a[2]))));
            }
            case "latch_count_down" -> {
                RCountDownLatch l = rs.getCountDownLatch(a[1]);
                l.countDown();
                reply(map("ok", true));
            }
            case "latch_count" -> {
                RCountDownLatch l = rs.getCountDownLatch(a[1]);
                reply(map("count", l.getCount()));
            }

            default -> reply(map("error", "unknown command: " + a[0]));
        }
    }

    private static Map<String, Object> map(String k, Object v) {
        Map<String, Object> m = new HashMap<>();
        m.put(k, v);
        return m;
    }

    private static void reply(Map<String, Object> m) throws Exception {
        System.out.println(OM.writeValueAsString(m));
        System.out.flush();
    }
}
