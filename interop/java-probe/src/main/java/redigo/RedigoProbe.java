package redigo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RBucket;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RCountDownLatch;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RLexSortedSet;
import org.redisson.api.RList;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RMapCache;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RSemaphore;
import org.redisson.api.RateIntervalUnit;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
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
    private static RReadWriteLock heldRwLock;
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

            case "along_add" -> {
                RAtomicLong al = rs.getAtomicLong(a[1]);
                reply(map("value", al.addAndGet(Long.parseLong(a[2]))));
            }
            case "along_get" -> {
                RAtomicLong al = rs.getAtomicLong(a[1]);
                reply(map("value", al.get()));
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
