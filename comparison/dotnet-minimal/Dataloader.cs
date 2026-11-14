// The .NET contender "with" grouping (study §2.1, §2.6): Program.cs whose
// unit read goes through a DataLoader of GreenDonut, .NET's port, the one
// Hot Chocolate uses. The keys a batch gathers leave as one "= ANY(@ids)"
// query, each batch counted in the metrics point. One deviation from the
// documentation, declared (§2.1): Hot Chocolate builds its loaders per
// GraphQL request, which with one key per HTTP request groups nothing; this
// one is shared by the process. Its settings are the defaults: no cache
// (GreenDonut caches only when given a cache), at most 1 024 keys a batch,
// batches dispatched by AutoBatchScheduler. The image starts the program
// with the argument "dataloader".

using System.Globalization;
using Dapper;
using GreenDonut;
using Npgsql;
using Study;

sealed class OrdersLoader(NpgsqlDataSource db, Metrics metrics)
    : BatchDataLoader<long, Order>(AutoBatchScheduler.Default, new DataLoaderOptions())
{
    protected override async Task<IReadOnlyDictionary<long, Order>> LoadBatchAsync(
        IReadOnlyList<long> keys, CancellationToken cancellationToken)
    {
        metrics.RecordBatch(keys.Count);
        await using var conn = await db.OpenConnectionAsync(cancellationToken);
        var rows = await conn.QueryAsync<Order>(
            "SELECT id, total, label FROM bench_orders WHERE id = ANY(@ids)", new { ids = keys.ToArray() });
        return rows.ToDictionary(o => o.Id);
    }
}

static class Dataloader
{
    public static void MapUnit(WebApplication app, Metrics metrics)
    {
        var loader = new OrdersLoader(app.Services.GetRequiredService<NpgsqlDataSource>(), metrics);
        app.MapGet("/orders/{id}", async (string id) =>
        {
            if (!long.TryParse(id, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var key))
            {
                return Results.Json(new { error = "bad key" }, statusCode: 400);
            }
            Order? order;
            try
            {
                order = await loader.LoadAsync(key);
            }
            catch (NpgsqlException e)
            {
                Console.Error.WriteLine($"unit {key}: {e.Message}");
                return Results.Json(new { error = "unavailable", key }, statusCode: 503);
            }
            return order is null
                ? Results.Json(new { error = "no such key", key }, statusCode: 404)
                : Results.Json(order);
        });
    }
}
