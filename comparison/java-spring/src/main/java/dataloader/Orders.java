package dataloader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderFactory;
import org.dataloader.DataLoaderOptions;
import org.dataloader.registries.ScheduledDataLoaderRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import study.contract.Metrics;

/**
 * study.Orders whose unit read goes through a DataLoader of java-dataloader
 * (graphql-java's): the keys loaded within one tick of its registry leave as
 * one "= ANY(?)" query, each batch counted in the metrics point.
 *
 * <p>One deviation from the documentation, declared (2.1): graphql-java
 * builds its loaders per GraphQL request, which with one key per HTTP
 * request groups nothing; this one is shared by the process, its cache off.
 * Its settings are the defaults -- a tick every 10 ms, no bound on a batch --
 * but one: the registry's ticker mode. Without it a batch leaves only on an
 * explicit dispatch(), which graphql-java's engine makes and a controller
 * does not; the ticker, the README's "ScheduledDataLoaderRegistry", is the
 * documented way to do without. The batch function is the README's:
 * CompletableFuture.supplyAsync on the common pool.
 */
@RestController
public class Orders extends study.Orders implements AutoCloseable {
    private final DataLoader<Long, Order> orders;
    private final ScheduledDataLoaderRegistry registry;

    public Orders(JdbcTemplate jdbc) {
        super(jdbc);
        orders = DataLoaderFactory.newDataLoader(
                (List<Long> ids) -> CompletableFuture.supplyAsync(() -> load(ids)),
                DataLoaderOptions.newOptions().setCachingEnabled(false).build());
        registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("orders", orders)
                .tickerMode(true)
                .build();
        // The ticker starts at the first dispatch, which graphql-java's
        // engine makes and nothing here would: once, then every tick.
        registry.dispatchAll();
    }

    private List<Order> load(List<Long> ids) {
        Metrics.recordBatch(ids.size());
        Map<Long, Order> byId = new HashMap<>();
        for (Order o : jdbc.query("SELECT id, total, label FROM bench_orders WHERE id = ANY(?)", ORDER,
                (Object) ids.toArray(new Long[0]))) {
            byId.put(o.id(), o);
        }
        return ids.stream().map(byId::get).toList();
    }

    @Override
    public void close() {
        registry.close();
    }

    @Override
    @GetMapping("/orders/{id}")
    public ResponseEntity<Object> unit(@PathVariable String id) {
        long key;
        try {
            key = Long.parseLong(id);
        } catch (NumberFormatException e) {
            return fail(400, "bad key");
        }
        Order o;
        try {
            o = orders.load(key).join();
        } catch (RuntimeException e) {
            System.err.println("unit " + key + ": " + e.getMessage());
            return fail(503, "unavailable", key);
        }
        if (o == null) {
            return fail(404, "no such key", key);
        }
        return ResponseEntity.ok(o);
    }

    @Override
    @GetMapping("/study/metrics")
    public Map<String, Object> metrics() {
        return Metrics.document(true);
    }
}
