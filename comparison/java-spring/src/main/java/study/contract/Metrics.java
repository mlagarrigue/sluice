package study.contract;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import jdk.jfr.consumer.RecordingStream;

/**
 * The metrics point (comparison/README.md, "The contract every contender
 * honours"), cumulative since the process started.
 *
 * <p>cpu_seconds is the process's CPU (OperatingSystemMXBean). The JVM
 * publishes no figure for its collector's CPU: gc.cpu_seconds is the CPU of
 * the collector's own threads, read from /proc/self/task (G1's "GC Thread#",
 * "G1 Conc#", "G1 Refine#", "G1 Main Marker", "G1 Service"; ZGC's "ZWorker",
 * "ZDriver", "ZDirector"...). The work a collector makes the application's
 * threads do -- barriers, allocation slow paths -- is in their CPU, not
 * here. The pauses are JFR's jdk.GCPhasePause events, streamed in the
 * process, one per stop-the-world pause of either collector, at nanosecond
 * resolution; JFR hands them over about once a second, so a document lags
 * the last second's pauses. Buckets double from 10 us. batches is null
 * unless the version groups (the DataLoader variant, which counts each
 * batch with {@link #recordBatch}).
 */
public final class Metrics {
    private Metrics() {}

    private static final Object lock = new Object();
    private static long pauses;
    private static long pauseNanos;
    private static final TreeMap<Double, Long> buckets = new TreeMap<>();

    /**
     * Starts the pause stream; called once, before the server listens. A
     * native image built without JFR has none: the pauses then stay at zero,
     * and the image is never measured under load (study 2.6). Spring's
     * ahead-of-time processing runs the main class to build the native
     * image: no stream then, whose thread would keep that JVM from exiting.
     */
    public static void start() {
        if (Boolean.getBoolean("spring.aot.processing")) {
            return;
        }
        try {
            RecordingStream rs = new RecordingStream();
            rs.enable("jdk.GCPhasePause").withThreshold(Duration.ZERO);
            rs.onEvent("jdk.GCPhasePause", e -> record(e.getDuration().toNanos()));
            rs.startAsync();
        } catch (RuntimeException | LinkageError e) {
            System.err.println("metrics: no JFR, pauses not counted: " + e);
        }
    }

    private static void record(long nanos) {
        double s = nanos / 1e9;
        double le = 1e-5;
        while (le < s) {
            le *= 2;
        }
        synchronized (lock) {
            pauses++;
            pauseNanos += nanos;
            buckets.merge(le, 1L, Long::sum);
        }
    }

    private static long batches;
    private static long batchKeys;
    private static final TreeMap<Long, Long> batchSizes = new TreeMap<>();

    /** Counts one batch of n keys, by size in powers of two (1, 2, 4...). */
    public static void recordBatch(int n) {
        long le = 1;
        while (le < n) {
            le *= 2;
        }
        synchronized (lock) {
            batches++;
            batchKeys += n;
            batchSizes.merge(le, 1L, Long::sum);
        }
    }

    public static Map<String, Object> document() {
        return document(false);
    }

    public static Map<String, Object> document(boolean grouped) {
        var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        List<Map<String, Object>> b = new ArrayList<>();
        long count;
        double seconds;
        synchronized (lock) {
            count = pauses;
            seconds = pauseNanos / 1e9;
            buckets.forEach((le, n) -> b.add(Map.of("le", le, "count", n)));
        }
        Map<String, Object> pauseDoc = new LinkedHashMap<>();
        pauseDoc.put("count", count);
        pauseDoc.put("seconds", seconds);
        pauseDoc.put("buckets", b);
        Map<String, Object> gc = new LinkedHashMap<>();
        gc.put("cpu_seconds", collectorCpuSeconds());
        gc.put("pauses", pauseDoc);
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("runtime", "java " + Runtime.version());
        doc.put("cpu_seconds", os.getProcessCpuTime() / 1e9);
        doc.put("gc", gc);
        doc.put("batches", grouped ? batchDocument() : null);
        return doc;
    }

    private static Map<String, Object> batchDocument() {
        Map<String, Object> b = new LinkedHashMap<>();
        List<Map<String, Object>> sizes = new ArrayList<>();
        synchronized (lock) {
            b.put("count", batches);
            b.put("keys", batchKeys);
            batchSizes.forEach((le, n) -> sizes.add(Map.of("le", le, "count", n)));
        }
        b.put("sizes", sizes);
        return b;
    }

    private static final String[] COLLECTOR_THREADS = {"GC Thread", "G1 ", "Z"};

    /** The CPU of the collector's threads, user and system, in seconds. */
    private static double collectorCpuSeconds() {
        long ticks = 0;
        try (DirectoryStream<Path> tasks = Files.newDirectoryStream(Path.of("/proc/self/task"))) {
            for (Path task : tasks) {
                try {
                    String comm = Files.readString(task.resolve("comm")).strip();
                    if (!isCollector(comm)) {
                        continue;
                    }
                    String stat = Files.readString(task.resolve("stat"));
                    // Fields after the command, which may hold spaces: state is
                    // field 3, utime 14 and stime 15.
                    String[] f = stat.substring(stat.lastIndexOf(')') + 2).split(" ");
                    ticks += Long.parseLong(f[11]) + Long.parseLong(f[12]);
                } catch (IOException | RuntimeException e) {
                    // A thread that ended between the listing and the read.
                }
            }
        } catch (IOException e) {
            return 0;
        }
        return ticks / 100.0; // USER_HZ, 100 on Linux
    }

    private static boolean isCollector(String comm) {
        for (String p : COLLECTOR_THREADS) {
            if (comm.startsWith(p)) {
                return true;
            }
        }
        return false;
    }
}
