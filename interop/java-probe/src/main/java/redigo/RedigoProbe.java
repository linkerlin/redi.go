package redigo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.redisson.Redisson;
import org.redisson.api.RAtomicDouble;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBatch;
import org.redisson.api.RBinaryStream;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RBlockingDeque;
import org.redisson.api.RBoundedBlockingQueue;
import org.redisson.api.RBucket;
import org.redisson.api.RBuckets;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RCountDownLatch;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RDeque;
import org.redisson.api.RDoubleAdder;
import org.redisson.api.RKeys;
import org.redisson.api.RLexSortedSet;
import org.redisson.api.RList;
import org.redisson.api.RListMultimap;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RMapCache;
import org.redisson.api.RMapCacheNative;
import org.redisson.api.RPatternTopic;
import org.redisson.api.RQueue;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RRingBuffer;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RSemaphore;
import org.redisson.api.RSet;
import org.redisson.api.RSetMultimap;
import org.redisson.api.RSetMultimapCacheNative;
import org.redisson.api.RListMultimapCacheNative;
import org.redisson.api.RSetMultimapCache;
import org.redisson.api.RListMultimapCache;
import org.redisson.api.RSetCache;
import org.redisson.api.FunctionMode;
import org.redisson.api.FunctionResult;
import org.redisson.api.RFunction;
import org.redisson.api.RIdGenerator;
import org.redisson.api.RShardedTopic;
import org.redisson.api.RTopic;
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
    private static RLock heldMulti;
    private static RLock heldRedLock;
    private static RReadWriteLock heldRwLock;
    private static org.redisson.api.RFencedLock heldFenced;
    private static String heldPermit;
    private static org.redisson.api.RLongAdder heldAdder;
    private static RDoubleAdder heldDoubleAdder;
    private static final java.util.concurrent.BlockingQueue<
            java.util.concurrent.BlockingQueue<Object>> reliableMsgs =
            new java.util.concurrent.LinkedBlockingQueue<>();
    private static final java.util.concurrent.BlockingQueue<
            java.util.concurrent.BlockingQueue<Object>> shardedMsgs =
            new java.util.concurrent.LinkedBlockingQueue<>();
    private static final java.util.concurrent.BlockingQueue<
            java.util.concurrent.BlockingQueue<Object>> topicMsgs =
            new java.util.concurrent.LinkedBlockingQueue<>();
    private static final java.util.concurrent.BlockingQueue<
            java.util.concurrent.BlockingQueue<Object>> patternMsgs =
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
        if (heldMulti != null && heldMulti.isHeldByCurrentThread()) {
            heldMulti.unlock();
        }
        if (heldRedLock != null && heldRedLock.isHeldByCurrentThread()) {
            heldRedLock.unlock();
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

            case "mlock_try" -> {
                RLock left = rs.getLock(a[1]);
                RLock right = rs.getLock(a[2]);
                RLock ml = rs.getMultiLock(left, right);
                boolean acq = ml.tryLock(0, 60000, TimeUnit.MILLISECONDS);
                heldMulti = acq ? ml : null;
                reply(map("acquired", acq));
            }
            case "mlock_unlock" -> {
                if (heldMulti != null && heldMulti.isHeldByCurrentThread()) {
                    heldMulti.unlock();
                }
                heldMulti = null;
                reply(map("ok", true));
            }

            case "redlock_try" -> {
                RLock x = rs.getLock(a[1]);
                RLock y = rs.getLock(a[2]);
                RLock z = rs.getLock(a[3]);
                RLock rl = rs.getRedLock(x, y, z);
                boolean acq = rl.tryLock(0, 60000, TimeUnit.MILLISECONDS);
                heldRedLock = acq ? rl : null;
                reply(map("acquired", acq));
            }
            case "redlock_unlock" -> {
                if (heldRedLock != null && heldRedLock.isHeldByCurrentThread()) {
                    heldRedLock.unlock();
                }
                heldRedLock = null;
                reply(map("ok", true));
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
            case "map_remove" -> {
                RMap<Object, Object> m = rs.getMap(a[1]);
                reply(map("value", m.remove(OM.readValue(a[2], Object.class))));
            }
            case "map_remove_if" -> {
                RMap<Object, Object> m = rs.getMap(a[1]);
                reply(map("ok", m.remove(OM.readValue(a[2], Object.class),
                        OM.readValue(a[3], Object.class))));
            }

            case "batch_map_put" -> {
                RBatch batch = rs.createBatch();
                batch.getMap(a[1]).putAsync(OM.readValue(a[2], Object.class),
                        OM.readValue(a[3], Object.class));
                batch.execute();
                reply(map("ok", true));
            }

            case "bq_offer" -> {
                RBlockingQueue<Object> q = rs.getBlockingQueue(a[1]);
                reply(map("ok", q.offer(OM.readValue(a[2], Object.class))));
            }
            case "bq_poll" -> {
                RBlockingQueue<Object> q = rs.getBlockingQueue(a[1]);
                reply(map("value", q.poll()));
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
            case "list_get_many" -> {
                RList<Object> l = rs.getList(a[1]);
                int[] idx = new int[a.length - 2];
                for (int i = 2; i < a.length; i++) {
                    idx[i - 2] = Integer.parseInt(a[i]);
                }
                reply(map("value", l.get(idx)));
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
            case "bucket_getex" -> {
                RBucket<Object> b = rs.getBucket(a[1]);
                reply(map("value", b.getAndExpire(
                        java.time.Duration.ofMillis(Long.parseLong(a[2])))));
            }
            case "bucket_expire_if_set" -> {
                RBucket<Object> b = rs.getBucket(a[1]);
                reply(map("ok", b.expireIfSet(
                        java.time.Duration.ofMillis(Long.parseLong(a[2])))));
            }
            case "bucket_expire_time" -> {
                RBucket<Object> b = rs.getBucket(a[1]);
                reply(map("value", b.getExpireTime()));
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
            case "mapcache_ttl" -> {
                RMapCache<Object, Object> mc = rs.getMapCache(a[1]);
                reply(map("ttl", mc.remainTimeToLive(OM.readValue(a[2], Object.class))));
            }
            case "mapcache_get_ttl_only" -> {
                RMapCache<Object, Object> mc = rs.getMapCache(a[1]);
                reply(map("value", mc.getWithTTLOnly(OM.readValue(a[2], Object.class))));
            }
            case "mapcache_putall_ttl" -> {
                RMapCache<Object, Object> mc = rs.getMapCache(a[1]);
                java.util.Map<Object, Object> entries = new HashMap<>();
                entries.put(OM.readValue(a[2], Object.class), OM.readValue(a[3], Object.class));
                mc.putAll(entries, Long.parseLong(a[4]), TimeUnit.MILLISECONDS);
                reply(map("ok", true));
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

            case "idgen_init" -> {
                RIdGenerator g = rs.getIdGenerator(a[1]);
                reply(map("ok", g.tryInit(Long.parseLong(a[2]), Long.parseLong(a[3]))));
            }
            case "idgen_next" -> {
                RIdGenerator g = rs.getIdGenerator(a[1]);
                reply(map("id", g.nextId()));
            }

            case "func_load" -> {
                RFunction fn = rs.getFunction();
                String lib = a[1];
                String code = new String(Base64.getDecoder().decode(a[2]));
                fn.loadAndReplace(lib, code);
                reply(map("ok", true));
            }
            case "func_call" -> {
                RFunction fn = rs.getFunction();
                Object r = fn.call(FunctionMode.READ, a[1], FunctionResult.LONG,
                        java.util.Collections.emptyList(), Long.parseLong(a[2]));
                reply(map("value", r));
            }
            case "func_delete" -> {
                rs.getFunction().delete(a[1]);
                reply(map("ok", true));
            }

            case "stopic_listen" -> {
                RShardedTopic topic = rs.getShardedTopic(a[1]);
                java.util.concurrent.BlockingQueue<Object> q =
                        new java.util.concurrent.LinkedBlockingQueue<>();
                topic.addListener(Object.class,
                        (org.redisson.api.listener.MessageListener<Object>) (channel, msg) -> q.add(msg));
                shardedMsgs.offer(q);
                reply(map("ok", true));
            }
            case "stopic_publish" -> {
                RShardedTopic topic = rs.getShardedTopic(a[1]);
                reply(map("subscribers", topic.publish(OM.readValue(a[2], Object.class))));
            }
            case "stopic_collect" -> {
                java.util.concurrent.BlockingQueue<Object> q = shardedMsgs.poll();
                if (q == null) {
                    reply(map("value", null));
                } else {
                    Object m = q.poll(4, TimeUnit.SECONDS);
                    reply(map("value", m));
                }
            }

            case "topic_listen" -> {
                RTopic topic = rs.getTopic(a[1]);
                java.util.concurrent.BlockingQueue<Object> q =
                        new java.util.concurrent.LinkedBlockingQueue<>();
                topic.addListener(Object.class,
                        (org.redisson.api.listener.MessageListener<Object>) (channel, msg) -> q.add(msg));
                topicMsgs.offer(q);
                reply(map("ok", true));
            }
            case "topic_publish" -> {
                RTopic topic = rs.getTopic(a[1]);
                reply(map("subscribers", topic.publish(OM.readValue(a[2], Object.class))));
            }
            case "topic_collect" -> {
                java.util.concurrent.BlockingQueue<Object> q = topicMsgs.poll();
                if (q == null) {
                    reply(map("value", null));
                } else {
                    Object m = q.poll(4, TimeUnit.SECONDS);
                    reply(map("value", m));
                }
            }

            case "ptopic_listen" -> {
                RPatternTopic topic = rs.getPatternTopic(a[1]);
                java.util.concurrent.BlockingQueue<Object> q =
                        new java.util.concurrent.LinkedBlockingQueue<>();
                topic.addListener(Object.class, (pattern, channel, msg) -> q.add(msg));
                patternMsgs.offer(q);
                reply(map("ok", true));
            }
            case "ptopic_collect" -> {
                java.util.concurrent.BlockingQueue<Object> q = patternMsgs.poll();
                if (q == null) {
                    reply(map("value", null));
                } else {
                    Object m = q.poll(4, TimeUnit.SECONDS);
                    reply(map("value", m));
                }
            }

            case "deque_add_first" -> {
                RDeque<Object> d = rs.getDeque(a[1]);
                d.addFirst(OM.readValue(a[2], Object.class));
                reply(map("ok", true));
            }
            case "deque_add_last" -> {
                RDeque<Object> d = rs.getDeque(a[1]);
                d.addLast(OM.readValue(a[2], Object.class));
                reply(map("ok", true));
            }
            case "deque_remove_first" -> {
                RDeque<Object> d = rs.getDeque(a[1]);
                reply(map("value", d.pollFirst()));
            }
            case "deque_remove_last" -> {
                RDeque<Object> d = rs.getDeque(a[1]);
                reply(map("value", d.pollLast()));
            }
            case "deque_size" -> {
                RDeque<Object> d = rs.getDeque(a[1]);
                reply(map("size", d.size()));
            }

            case "bdq_put_last" -> {
                RBlockingDeque<Object> d = rs.getBlockingDeque(a[1]);
                d.putLast(OM.readValue(a[2], Object.class));
                reply(map("ok", true));
            }
            case "bdq_take_first" -> {
                RBlockingDeque<Object> d = rs.getBlockingDeque(a[1]);
                reply(map("value", d.pollFirst()));
            }

            case "script_eval" -> {
                RScript script = rs.getScript();
                String lua = new String(Base64.getDecoder().decode(a[1]));
                Object r = script.eval(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.LONG);
                reply(map("value", r));
            }
            case "script_load" -> {
                RScript script = rs.getScript();
                String lua = new String(Base64.getDecoder().decode(a[1]));
                reply(map("sha", script.scriptLoad(lua)));
            }

            case "buckets_set" -> {
                RBuckets buckets = rs.getBuckets();
                Map<String, Object> mapping = new HashMap<>();
                for (int i = 1; i + 1 < a.length; i += 2) {
                    mapping.put(a[i], OM.readValue(a[i + 1], Object.class));
                }
                buckets.set(mapping);
                reply(map("ok", true));
            }
            case "buckets_get" -> {
                RBuckets buckets = rs.getBuckets();
                String[] keys = java.util.Arrays.copyOfRange(a, 1, a.length);
                reply(map("values", buckets.get(keys)));
            }

            case "keys_type" -> {
                RKeys keys = rs.getKeys();
                reply(map("type", keys.getType(a[1]).toString()));
            }
            case "keys_count_exists" -> {
                RKeys keys = rs.getKeys();
                String[] names = java.util.Arrays.copyOfRange(a, 1, a.length);
                reply(map("count", keys.countExists(names)));
            }
            case "keys_delete" -> {
                RKeys keys = rs.getKeys();
                String[] names = java.util.Arrays.copyOfRange(a, 1, a.length);
                reply(map("deleted", keys.delete(names)));
            }

            case "set_add" -> {
                RSet<Object> set = rs.getSet(a[1]);
                reply(map("added", set.add(OM.readValue(a[2], Object.class))));
            }
            case "set_contains" -> {
                RSet<Object> set = rs.getSet(a[1]);
                reply(map("contains", set.contains(OM.readValue(a[2], Object.class))));
            }
            case "set_size" -> {
                RSet<Object> set = rs.getSet(a[1]);
                reply(map("size", set.size()));
            }
            case "set_contains_each" -> {
                RSet<Object> set = rs.getSet(a[1]);
                java.util.List<Object> query = new ArrayList<>();
                for (int i = 2; i < a.length; i++) {
                    query.add(OM.readValue(a[i], Object.class));
                }
                reply(map("values", new ArrayList<>(set.containsEach(query))));
            }

            case "queue_offer" -> {
                RQueue<Object> q = rs.getQueue(a[1]);
                reply(map("ok", q.offer(OM.readValue(a[2], Object.class))));
            }
            case "queue_poll" -> {
                RQueue<Object> q = rs.getQueue(a[1]);
                reply(map("value", q.poll()));
            }
            case "queue_size" -> {
                RQueue<Object> q = rs.getQueue(a[1]);
                reply(map("size", q.size()));
            }
            case "queue_indexof" -> {
                RQueue<Object> q = rs.getQueue(a[1]);
                reply(map("index", q.indexOf(OM.readValue(a[2], Object.class))));
            }
            case "queue_move" -> {
                RQueue<Object> q = rs.getQueue(a[1]);
                reply(map("value", q.pollLastAndOfferFirstTo(a[2])));
            }
            case "queue_poll_n" -> {
                RQueue<Object> q = rs.getQueue(a[1]);
                reply(map("values", q.poll(Integer.parseInt(a[2]))));
            }

            case "smm_put" -> {
                RSetMultimap<Object, Object> m = rs.getSetMultimap(a[1]);
                reply(map("added", m.put(OM.readValue(a[2], Object.class),
                        OM.readValue(a[3], Object.class))));
            }
            case "smm_getall" -> {
                RSetMultimap<Object, Object> m = rs.getSetMultimap(a[1]);
                reply(map("values", new ArrayList<>(m.getAll(OM.readValue(a[2], Object.class)))));
            }

            case "lmm_put" -> {
                RListMultimap<Object, Object> m = rs.getListMultimap(a[1]);
                reply(map("added", m.put(OM.readValue(a[2], Object.class),
                        OM.readValue(a[3], Object.class))));
            }
            case "lmm_getall" -> {
                RListMultimap<Object, Object> m = rs.getListMultimap(a[1]);
                reply(map("values", new ArrayList<>(m.getAll(OM.readValue(a[2], Object.class)))));
            }

            case "dadder_create" -> {
                heldDoubleAdder = rs.getDoubleAdder(a[1]);
                reply(map("ok", true));
            }
            case "dadder_add" -> {
                if (heldDoubleAdder != null) {
                    heldDoubleAdder.add(Double.parseDouble(a[2]));
                }
                reply(map("ok", true));
            }
            case "dadder_sum" -> {
                if (heldDoubleAdder == null) {
                    reply(map("error", "no double adder"));
                } else {
                    reply(map("value", heldDoubleAdder.sum()));
                }
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
            case "bloom_exists" -> {
                RBloomFilter<Object> f = rs.getBloomFilter(a[1]);
                java.util.List<Object> els = new ArrayList<>();
                for (int i = 2; i < a.length; i++) {
                    els.add(OM.readValue(a[i], Object.class));
                }
                reply(map("value", f.exists(els)));
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
