// The study's .NET contender: ASP.NET Core Minimal APIs, Dapper on Npgsql
// (study §2.5, §2.6) -- the routes as the Minimal APIs tutorial writes them,
// SQL written by hand as Dapper's README does. The standard and optimised
// images run this same code; what the optimised one changes is listed, with
// its sources, in comparison/README.md.

using System.Globalization;
using System.Text.Json;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using Study;

Contract cfg;
try
{
    cfg = Contract.Read();
}
catch (Refused e)
{
    Console.Error.WriteLine(e.Message);
    return 1;
}
Console.Error.WriteLine($"dotnet-minimal: {cfg.Protocol} on {cfg.Host}:{cfg.Port} ({cfg.Summary()})");
var metrics = new Metrics();

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.ConfigureKestrel(cfg.Configure);
cfg.AddCompression(builder.Services);
builder.Services.AddSingleton(cfg.DataSource());

var app = builder.Build();
if (cfg.Compression != "off")
{
    app.UseResponseCompression();
}

var db = app.Services.GetRequiredService<NpgsqlDataSource>();
await using (var conn = await db.OpenConnectionAsync())
{
    foreach (var ddl in Contract.WriteTable)
    {
        await conn.ExecuteAsync(ddl);
    }
}

static IResult Fail(int status, string error) =>
    Results.Json(new { error }, statusCode: status);

static IResult FailKey(int status, string error, long key) =>
    Results.Json(new { error, key }, statusCode: status);

static bool ParseKey(string? s, out long v) =>
    long.TryParse(s, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out v);

app.MapGet("/static", () => Results.Json(Contract.Static));

app.MapGet("/orders/{id}", async (string id, NpgsqlDataSource db) =>
{
    if (!ParseKey(id, out var key))
    {
        return Fail(400, "bad key");
    }
    Order? order;
    try
    {
        await using var conn = await db.OpenConnectionAsync();
        order = await conn.QueryFirstOrDefaultAsync<Order>(
            "SELECT id, total, label FROM bench_orders WHERE id = @key", new { key });
    }
    catch (NpgsqlException e)
    {
        Console.Error.WriteLine($"unit {key}: {e.Message}");
        return FailKey(503, "unavailable", key);
    }
    return order is null ? FailKey(404, "no such key", key) : Results.Json(order);
});

app.MapGet("/orders", async ([FromQuery(Name = "from")] string? from, string? limit, NpgsqlDataSource db) =>
{
    if (!ParseKey(from, out var start))
    {
        return Fail(400, "bad from");
    }
    if (!ParseKey(limit, out var n))
    {
        n = 0;
    }
    if (n <= 0 || n > Contract.ListMax)
    {
        return FailKey(400, "bad limit", start);
    }
    List<Order> items;
    try
    {
        await using var conn = await db.OpenConnectionAsync();
        items = (await conn.QueryAsync<Order>(
            "SELECT id, total, label FROM bench_orders WHERE id > @start ORDER BY id LIMIT @n", new { start, n })).AsList();
    }
    catch (NpgsqlException e)
    {
        Console.Error.WriteLine($"list {start}: {e.Message}");
        return FailKey(503, "unavailable", start);
    }
    return Results.Json(new Page(start, n, items));
});

app.MapPost("/orders", async (HttpRequest request, NpgsqlDataSource db) =>
{
    long key, total;
    try
    {
        using var doc = await JsonDocument.ParseAsync(request.Body);
        if (doc.RootElement.ValueKind != JsonValueKind.Object
            || !doc.RootElement.TryGetProperty("key", out var k) || k.ValueKind != JsonValueKind.Number
            || !doc.RootElement.TryGetProperty("total", out var t) || t.ValueKind != JsonValueKind.Number
            || !k.TryGetInt64(out key) || !t.TryGetInt64(out total) || key == 0)
        {
            return Fail(400, "bad body");
        }
    }
    catch (JsonException)
    {
        return Fail(400, "bad body");
    }
    try
    {
        await using var conn = await db.OpenConnectionAsync();
        await conn.ExecuteAsync("INSERT INTO comparison_writes (key, total) VALUES (@key, @total)", new { key, total });
    }
    catch (NpgsqlException e)
    {
        Console.Error.WriteLine($"write {key}: {e.Message}");
        return FailKey(503, "unavailable", key);
    }
    return Results.Json(new { key });
});

app.MapGet("/study/metrics", () => Results.Json(metrics.Document()));

await app.RunAsync();
return 0;

record Order(long Id, long Total, string Label);

record Page(long From, long Limit, List<Order> Items);
