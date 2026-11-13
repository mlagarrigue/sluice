package study;

import java.util.List;
import java.util.Map;
import org.springframework.boot.CommandLineRunner;
import org.springframework.dao.DataAccessException;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import study.contract.Contract;
import study.contract.Metrics;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The four routes of the study, on JdbcTemplate as Spring's guide
 * "Accessing Relational Data using JDBC" uses it: SQL written by hand, a
 * RowMapper per row, the tables made by a CommandLineRunner at startup.
 */
@RestController
public class Orders implements CommandLineRunner {
    record Order(long id, long total, String label) {}

    record Page(long from, long limit, List<Order> items) {}

    record Written(long key) {}

    private static final RowMapper<Order> ORDER =
            (rs, n) -> new Order(rs.getLong("id"), rs.getLong("total"), rs.getString("label"));

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final JdbcTemplate jdbc;

    public Orders(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void run(String... args) {
        Contract.WRITE_TABLE.forEach(jdbc::execute);
    }

    private static ResponseEntity<Object> fail(int status, String error) {
        return ResponseEntity.status(status).body(Map.of("error", error));
    }

    private static ResponseEntity<Object> fail(int status, String error, long key) {
        return ResponseEntity.status(status).body(Map.of("error", error, "key", key));
    }

    @GetMapping("/static")
    public Map<String, Object> staticDoc() {
        return Contract.STATIC;
    }

    @GetMapping("/orders/{id}")
    public ResponseEntity<Object> unit(@PathVariable String id) {
        long key;
        try {
            key = Long.parseLong(id);
        } catch (NumberFormatException e) {
            return fail(400, "bad key");
        }
        List<Order> rows;
        try {
            rows = jdbc.query("SELECT id, total, label FROM bench_orders WHERE id = ?", ORDER, key);
        } catch (DataAccessException e) {
            System.err.println("unit " + key + ": " + e.getMessage());
            return fail(503, "unavailable", key);
        }
        if (rows.isEmpty()) {
            return fail(404, "no such key", key);
        }
        return ResponseEntity.ok(rows.getFirst());
    }

    @GetMapping("/orders")
    public ResponseEntity<Object> list(
            @RequestParam(name = "from", defaultValue = "") String from,
            @RequestParam(name = "limit", defaultValue = "") String limit) {
        long start;
        try {
            start = Long.parseLong(from);
        } catch (NumberFormatException e) {
            return fail(400, "bad from");
        }
        long n;
        try {
            n = Long.parseLong(limit);
        } catch (NumberFormatException e) {
            n = 0;
        }
        if (n <= 0 || n > Contract.LIST_MAX) {
            return fail(400, "bad limit", start);
        }
        List<Order> rows;
        try {
            rows = jdbc.query(
                    "SELECT id, total, label FROM bench_orders WHERE id > ? ORDER BY id LIMIT ?", ORDER, start, n);
        } catch (DataAccessException e) {
            System.err.println("list " + start + ": " + e.getMessage());
            return fail(503, "unavailable", start);
        }
        return ResponseEntity.ok(new Page(start, n, rows));
    }

    @PostMapping("/orders")
    public ResponseEntity<Object> write(@RequestBody(required = false) String body) {
        long key;
        long total;
        try {
            JsonNode doc = JSON.readTree(body);
            JsonNode k = doc.get("key");
            JsonNode t = doc.get("total");
            if (k == null || t == null || !k.isIntegralNumber() || !t.isIntegralNumber()
                    || !k.canConvertToLong() || !t.canConvertToLong()) {
                return fail(400, "bad body");
            }
            key = k.longValue();
            total = t.longValue();
        } catch (RuntimeException e) {
            return fail(400, "bad body");
        }
        if (key == 0) {
            return fail(400, "bad body");
        }
        try {
            jdbc.update("INSERT INTO comparison_writes (key, total) VALUES (?, ?)", key, total);
        } catch (DataAccessException e) {
            System.err.println("write " + key + ": " + e.getMessage());
            return fail(503, "unavailable", key);
        }
        return ResponseEntity.ok(new Written(key));
    }

    @GetMapping("/study/metrics")
    public Map<String, Object> metrics() {
        return Metrics.document();
    }
}
