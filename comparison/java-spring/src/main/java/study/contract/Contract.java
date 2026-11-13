package study.contract;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What the Java contender's versions share and none is measured on.
 *
 * <p>The contract's environment (comparison/README.md, "The contract every
 * contender honours") mapped onto Spring Boot's properties, the static
 * document and the write table. The metrics point is {@link Metrics}. The
 * contender's own code -- the controller, its queries -- is study.Orders,
 * where the study counts its lines (section 1.1).
 */
public final class Contract {
    private Contract() {}

    public static final long LIST_MAX = 100_000;

    public static final List<String> WRITE_TABLE = List.of(
            """
            CREATE TABLE IF NOT EXISTS comparison_writes (
                id bigserial PRIMARY KEY, key bigint NOT NULL, total bigint NOT NULL,
                created timestamptz NOT NULL DEFAULT now())""",
            "ALTER TABLE comparison_writes SET (autovacuum_enabled = off)");

    public static final Map<String, Object> STATIC = staticDoc();

    private static Map<String, Object> staticDoc() {
        List<String> padding = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            padding.add(String.format("line-%02d-abcdefghijklmnop", i));
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("status", "ok");
        doc.put("service", "sluice-study");
        doc.put("version", "r");
        doc.put("padding", List.copyOf(padding));
        return Map.copyOf(doc);
    }

    /** A configuration this contender cannot serve: refused before it listens. */
    public static final class Refused extends RuntimeException {
        Refused(String message) {
            super(message);
        }
    }

    /**
     * Reads STUDY_* and the PG* variables with the study server's defaults
     * and returns Spring Boot's properties for them. A configuration a
     * version does not implement is refused here, before it listens, and is
     * published "not supported" (2.4): HTTP/3 (Tomcat has none), and every
     * coding but gzip.
     *
     * <p>The TLS is every contender's: TLS 1.3 only, the campaign's
     * certificate, ALPN naming h2 alone on an h2 run, and nothing on an h1
     * run. A client that negotiates nothing is still served HTTP/1.1 on an
     * h2 run: Tomcat's connector keeps it, declared in the README. No session
     * tickets unless asked: JSSE resumes TLS 1.3 through nothing else, and
     * jdk.tls.server.newSessionTicketCount = 0 has it issue none. That is a
     * system property, read when JSSE starts, so it is set here, before
     * Spring builds anything.
     */
    public static Map<String, Object> properties(Map<String, String> env) {
        String listen = or(env.get("STUDY_LISTEN"), "127.0.0.1:8443");
        int colon = listen.lastIndexOf(':');
        if (colon < 0) {
            throw new Refused("STUDY_LISTEN=" + listen + ": host:port");
        }
        String host = listen.substring(0, colon).replace("[", "").replace("]", "");
        String port = listen.substring(colon + 1);
        String protocol = or(env.get("STUDY_PROTOCOL"), "h1");
        boolean tls = onOff(env, "STUDY_TLS", true);
        boolean resume = onOff(env, "STUDY_TLS_RESUME", false);
        String compression = or(env.get("STUDY_COMPRESSION"), "off");
        int pool;
        try {
            pool = Integer.parseInt(or(env.get("STUDY_DB_POOL"), "8"));
        } catch (NumberFormatException e) {
            throw new Refused("STUDY_DB_POOL=" + env.get("STUDY_DB_POOL") + ": a positive count");
        }
        if (pool <= 0) {
            throw new Refused("STUDY_DB_POOL=" + pool + ": a positive count");
        }
        switch (protocol) {
            case "h1", "h2" -> {}
            case "h3" -> throw new Refused("STUDY_PROTOCOL=h3: not supported by this contender");
            default -> throw new Refused("STUDY_PROTOCOL=" + protocol + ": h1, h2 or h3");
        }
        switch (compression) {
            case "off", "gzip" -> {}
            case "deflate", "br", "zstd" ->
                throw new Refused("STUDY_COMPRESSION=" + compression + ": not offered by this contender");
            default -> throw new Refused("STUDY_COMPRESSION=" + compression + ": off, gzip, deflate, br or zstd");
        }
        if (!tls && !protocol.equals("h1")) {
            throw new Refused("STUDY_TLS=off with STUDY_PROTOCOL=" + protocol
                    + ": in clear the study runs HTTP/1.1 only");
        }
        String cert = or(env.get("STUDY_TLS_CERT"), "");
        String key = or(env.get("STUDY_TLS_KEY"), "");
        if (tls && (cert.isEmpty() || key.isEmpty())) {
            // The images carry a certificate for a local run, named by default.
            throw new Refused("STUDY_TLS_CERT and STUDY_TLS_KEY name the certificate and its key");
        }

        Map<String, Object> p = new HashMap<>();
        p.put("server.address", host);
        p.put("server.port", port);
        if (tls) {
            p.put("server.ssl.certificate", "file:" + cert);
            p.put("server.ssl.certificate-private-key", "file:" + key);
            p.put("server.ssl.protocol", "TLSv1.3");
            p.put("server.ssl.enabled-protocols", "TLSv1.3");
            if (!resume) {
                System.setProperty("jdk.tls.server.newSessionTicketCount", "0");
            }
        }
        p.put("server.http2.enabled", protocol.equals("h2"));
        if (compression.equals("gzip")) {
            p.put("server.compression.enabled", true);
        }
        // PostgreSQL's JDBC driver does not read libpq's variables: they are
        // mapped onto the URL and the pool here.
        p.put("spring.datasource.url", "jdbc:postgresql://" + or(env.get("PGHOST"), "localhost") + ":"
                + or(env.get("PGPORT"), "5432") + "/" + or(env.get("PGDATABASE"), ""));
        p.put("spring.datasource.username", or(env.get("PGUSER"), ""));
        p.put("spring.datasource.password", or(env.get("PGPASSWORD"), ""));
        p.put("spring.datasource.hikari.maximum-pool-size", pool);
        return p;
    }

    /** One line for the startup log: what the run serves. */
    public static String summary(Map<String, String> env) {
        if (!onOff(env, "STUDY_TLS", true)) {
            return "clear text, HTTP/1.1";
        }
        String protocol = or(env.get("STUDY_PROTOCOL"), "h1");
        return "TLS 1.3+, certificate " + env.get("STUDY_TLS_CERT") + ", ALPN "
                + (protocol.equals("h2") ? "h2" : "http/1.1") + ", resumption "
                + (onOff(env, "STUDY_TLS_RESUME", false) ? "on" : "off");
    }

    private static String or(String v, String dflt) {
        return v == null || v.isEmpty() ? dflt : v;
    }

    private static boolean onOff(Map<String, String> env, String name, boolean dflt) {
        String v = or(env.get(name), "");
        return switch (v) {
            case "" -> dflt;
            case "on" -> true;
            case "off" -> false;
            default -> throw new Refused(name + "=" + v + ": on or off");
        };
    }
}
