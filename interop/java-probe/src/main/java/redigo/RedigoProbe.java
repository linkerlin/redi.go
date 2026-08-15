package redigo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RCountDownLatch;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RSemaphore;
import org.redisson.api.RateIntervalUnit;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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
