// What the .NET contender's versions share and none is measured on: the
// contract's environment (comparison/README.md, "The contract every
// contender honours") mapped onto Kestrel, the static document, the write
// table and the metrics point. The contender's own code -- the routes and
// their queries -- is Program.cs, where the study counts its lines (§1.1).

using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.IO.Compression;
using System.Net;
using System.Net.Quic;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.ResponseCompression;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Npgsql;

namespace Study;

public sealed class Refused(string message) : Exception(message);

public sealed record Contract(
    string Host, int Port, string Protocol, bool Tls, string Cert, string Key,
    bool Resume, string Compression, int Pool)
{
    public const long ListMax = 100_000;

    public static readonly string[] WriteTable =
    [
        """
        CREATE TABLE IF NOT EXISTS comparison_writes (
            id bigserial PRIMARY KEY, key bigint NOT NULL, total bigint NOT NULL,
            created timestamptz NOT NULL DEFAULT now())
        """,
        "ALTER TABLE comparison_writes SET (autovacuum_enabled = off)",
    ];

    public static readonly Dictionary<string, object> Static = new()
    {
        ["status"] = "ok",
        ["service"] = "sluice-study",
        ["version"] = "r",
        ["padding"] = Enumerable.Range(0, 40).Select(i => $"line-{i:00}-abcdefghijklmnop").ToArray(),
    };

    /// <summary>
    /// Reads STUDY_* with the study server's defaults. A configuration this
    /// contender cannot serve is refused here, before it listens, and is
    /// published "not supported" (§2.4): deflate and zstd, which
    /// ResponseCompression does not offer, and HTTP/3 where msquic is missing.
    /// </summary>
    public static Contract Read()
    {
        string Env(string name, string dflt)
        {
            var v = Environment.GetEnvironmentVariable(name);
            return string.IsNullOrEmpty(v) ? dflt : v;
        }

        bool OnOff(string name, bool dflt) => Env(name, "") switch
        {
            "" => dflt,
            "on" => true,
            "off" => false,
            var v => throw new Refused($"{name}={v}: on or off"),
        };

        var listen = Env("STUDY_LISTEN", "127.0.0.1:8443");
        var colon = listen.LastIndexOf(':');
        if (colon < 0 || !int.TryParse(listen[(colon + 1)..], out var port))
        {
            throw new Refused($"STUDY_LISTEN={listen}: host:port");
        }
        if (!int.TryParse(Env("STUDY_DB_POOL", "8"), out var pool) || pool <= 0)
        {
            throw new Refused($"STUDY_DB_POOL={Env("STUDY_DB_POOL", "")}: a positive count");
        }
        var c = new Contract(
            Host: listen[..colon].Trim('[', ']'),
            Port: port,
            Protocol: Env("STUDY_PROTOCOL", "h1"),
            Tls: OnOff("STUDY_TLS", true),
            Cert: Env("STUDY_TLS_CERT", ""),
            Key: Env("STUDY_TLS_KEY", ""),
            Resume: OnOff("STUDY_TLS_RESUME", false),
            Compression: Env("STUDY_COMPRESSION", "off"),
            Pool: pool);
        if (c.Protocol is not ("h1" or "h2" or "h3"))
        {
            throw new Refused($"STUDY_PROTOCOL={c.Protocol}: h1, h2 or h3");
        }
        switch (c.Compression)
        {
            case "off" or "gzip" or "br":
                break;
            case "deflate" or "zstd":
                throw new Refused($"STUDY_COMPRESSION={c.Compression}: not offered by this contender");
            default:
                throw new Refused($"STUDY_COMPRESSION={c.Compression}: off, gzip, deflate, br or zstd");
        }
        if (!c.Tls && c.Protocol != "h1")
        {
            throw new Refused($"STUDY_TLS=off with STUDY_PROTOCOL={c.Protocol}: in clear the study runs HTTP/1.1 only");
        }
        if (c.Tls && (c.Cert == "" || c.Key == ""))
        {
            // The images carry a certificate for a local run, named by default.
            throw new Refused("STUDY_TLS_CERT and STUDY_TLS_KEY name the certificate and its key");
        }
#pragma warning disable CA2252, CA1416 // QuicListener: supported on Linux with msquic
        if (c.Protocol == "h3" && !QuicListener.IsSupported)
        {
            throw new Refused("STUDY_PROTOCOL=h3: msquic is missing (libmsquic)");
        }
#pragma warning restore CA2252, CA1416
        return c;
    }

    public string Summary() => !Tls
        ? "clear text, HTTP/1.1"
        : $"TLS 1.3+, certificate {Cert}, ALPN {Alpn}, resumption {(Resume ? "on" : "off")}";

    private string Alpn => Protocol switch { "h2" => "h2", "h3" => "h3", _ => "http/1.1" };

    /// <summary>
    /// One endpoint, one protocol: HTTP/3 alone listens on UDP only. The TLS
    /// of every contender: TLS 1.3 only, the campaign's certificate, ALPN
    /// naming the one protocol (Kestrel names what Protocols holds), no
    /// session tickets unless asked over h1 and h2 (SslStream's
    /// AllowTlsResume, the only way it resumes TLS 1.3). Over h3 the setting
    /// cannot reach msquic: resumption there is .NET's default, declared.
    /// </summary>
    public void Configure(KestrelServerOptions o)
    {
        var address = IPAddress.TryParse(Host, out var a) ? a : Dns.GetHostAddresses(Host)[0];
        o.Listen(address, Port, lo =>
        {
            lo.Protocols = Protocol switch
            {
                "h2" => HttpProtocols.Http2,
                "h3" => HttpProtocols.Http3,
                _ => HttpProtocols.Http1,
            };
            if (Tls)
            {
                var https = new HttpsConnectionAdapterOptions
                {
                    ServerCertificate = X509Certificate2.CreateFromPemFile(Cert, Key),
                    SslProtocols = SslProtocols.Tls13,
                };
                // HTTP/3's TLS is msquic's, which Kestrel gives no callback
                // into: Kestrel refuses OnAuthenticate on an HTTP/3 endpoint.
                if (Protocol != "h3")
                {
                    https.OnAuthenticate = (_, ssl) => ssl.AllowTlsResume = Resume;
                }
                lo.UseHttps(https);
            }
        });
    }

    /// <summary>
    /// ResponseCompression with the one coding the run names, over TLS too
    /// (EnableForHttps: the contract compresses there); gzip at level 1
    /// (CompressionLevel.Fastest), br at .NET's Optimal.
    /// </summary>
    public void AddCompression(IServiceCollection services)
    {
        if (Compression == "off")
        {
            return;
        }
        services.AddResponseCompression(o =>
        {
            o.EnableForHttps = true;
            if (Compression == "gzip")
            {
                o.Providers.Add<GzipCompressionProvider>();
            }
            else
            {
                o.Providers.Add<BrotliCompressionProvider>();
            }
        });
        services.Configure<GzipCompressionProviderOptions>(g => g.Level = CompressionLevel.Fastest);
        services.Configure<BrotliCompressionProviderOptions>(b => b.Level = CompressionLevel.Optimal);
    }

    /// <summary>
    /// The pool, from libpq's variables, which Npgsql also reads; the size is
    /// the contract's. No GSS encryption: Npgsql tries it first on every
    /// connection by default, the images carry no Kerberos library, and no
    /// other contender's driver tries it.
    /// </summary>
    public NpgsqlDataSource DataSource()
    {
        var b = new NpgsqlConnectionStringBuilder
        {
            GssEncryptionMode = GssEncryptionMode.Disable,
            Host = Environment.GetEnvironmentVariable("PGHOST") ?? "localhost",
            Port = int.TryParse(Environment.GetEnvironmentVariable("PGPORT"), out var p) ? p : 5432,
            Username = Environment.GetEnvironmentVariable("PGUSER"),
            Password = Environment.GetEnvironmentVariable("PGPASSWORD"),
            Database = Environment.GetEnvironmentVariable("PGDATABASE"),
            MaxPoolSize = Pool,
        };
        return NpgsqlDataSource.Create(b.ConnectionString);
    }
}

/// <summary>
/// The metrics point, cumulative since the process started. cpu_seconds is
/// the process's CPU. The runtime publishes no figure for its collector's
/// CPU: gc.cpu_seconds is the CPU of the collector's own threads, read from
/// /proc/self/task (".NET Server GC", one per heap, and the background
/// collector's, which appears at its first collection); the work a collector
/// makes the application's threads do is in their CPU, not here. The
/// pauses are the runtime's own events, read in the process: from
/// GCSuspendEEBegin to GCRestartEEEnd for a suspension made for a GC, one
/// pause each. Buckets double from 10 us. batches is null: this version does
/// not group.
/// </summary>
public sealed class Metrics : EventListener
{
    private readonly object _lock = new();
    private long _pauses;
    private double _pauseSeconds;
    private readonly SortedDictionary<double, long> _buckets = [];
    private DateTime? _suspended;

    protected override void OnEventSourceCreated(EventSource source)
    {
        if (source.Name == "Microsoft-Windows-DotNETRuntime")
        {
            EnableEvents(source, EventLevel.Informational, (EventKeywords)0x1); // GC
        }
    }

    protected override void OnEventWritten(EventWrittenEventArgs e)
    {
        switch (e.EventName)
        {
            // Reason 1 is a suspension for a GC, 6 for its preparation.
            case "GCSuspendEEBegin_V1":
                var reason = Convert.ToUInt32(e.Payload?[0] ?? 0u);
                _suspended = reason is 1 or 6 ? e.TimeStamp : null;
                break;
            case "GCRestartEEEnd_V1" when _suspended is { } start:
                Record((e.TimeStamp - start).TotalSeconds);
                _suspended = null;
                break;
        }
    }

    private void Record(double s)
    {
        var le = 1e-5;
        while (le < s)
        {
            le *= 2;
        }
        lock (_lock)
        {
            _pauses++;
            _pauseSeconds += s;
            _buckets[le] = _buckets.GetValueOrDefault(le) + 1;
        }
    }

    public Dictionary<string, object?> Document()
    {
        long count;
        double seconds;
        List<Dictionary<string, object>> buckets;
        lock (_lock)
        {
            count = _pauses;
            seconds = _pauseSeconds;
            buckets = _buckets.Select(b => new Dictionary<string, object> { ["le"] = b.Key, ["count"] = b.Value }).ToList();
        }
        return new()
        {
            ["runtime"] = "dotnet " + Environment.Version,
            ["cpu_seconds"] = Process.GetCurrentProcess().TotalProcessorTime.TotalSeconds,
            ["gc"] = new Dictionary<string, object>
            {
                ["cpu_seconds"] = CollectorCpuSeconds(),
                ["pauses"] = new Dictionary<string, object>
                {
                    ["count"] = count,
                    ["seconds"] = seconds,
                    ["buckets"] = buckets,
                },
            },
            ["batches"] = null,
        };
    }

    private static readonly string[] CollectorThreads = [".NET SVR GC", ".NET BGC", ".NET Server GC", ".NET Background GC"];

    private static double CollectorCpuSeconds()
    {
        long ticks = 0;
        try
        {
            foreach (var task in Directory.EnumerateDirectories("/proc/self/task"))
            {
                try
                {
                    var comm = File.ReadAllText(Path.Combine(task, "comm")).Trim();
                    if (!CollectorThreads.Any(comm.StartsWith))
                    {
                        continue;
                    }
                    var stat = File.ReadAllText(Path.Combine(task, "stat"));
                    // Fields after the command, which may hold spaces: state is
                    // field 3, utime 14 and stime 15.
                    var f = stat[(stat.LastIndexOf(')') + 2)..].Split(' ');
                    ticks += long.Parse(f[11]) + long.Parse(f[12]);
                }
                catch (Exception e) when (e is IOException or FormatException or IndexOutOfRangeException)
                {
                    // A thread that ended between the listing and the read.
                }
            }
        }
        catch (IOException)
        {
            return 0;
        }
        return ticks / 100.0; // USER_HZ, 100 on Linux
    }
}
